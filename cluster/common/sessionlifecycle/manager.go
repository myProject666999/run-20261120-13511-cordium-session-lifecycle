/*
 * Copyright Octelium Labs, LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sessionlifecycle

import (
	"context"
	"errors"
	"time"
)

// errNoChange is an internal sentinel used inside compare-and-update
// callbacks to signal that an idempotent request needs no write. It never
// escapes the Manager.
var errNoChange = errors.New("sessionlifecycle: state already matches request")

// Options configures a Manager.
type Options struct {
	// Clock is the time source. Defaults to SystemClock.
	Clock Clock
	// PurgeOnFinalize makes FinalizeDestroy delete the record instead of
	// retaining a StateDestroyed tombstone. With purging enabled, a repeated
	// destroy after finalization surfaces ErrSessionNotFound (matching an
	// upstream cleaner that removes records itself); with tombstones it
	// succeeds idempotently.
	PurgeOnFinalize bool
}

// Manager is the single entry point for every lifecycle mutation. It owns
// the transition table validation and the optimistic-concurrency retry loop;
// callers never write session state to the Store themselves.
type Manager struct {
	store Store
	clock Clock
	purge bool
}

// NewManager builds a Manager over store.
func NewManager(store Store, opts *Options) *Manager {
	m := &Manager{store: store}
	if opts != nil {
		m.clock = opts.Clock
		m.purge = opts.PurgeOnFinalize
	}
	if m.clock == nil {
		m.clock = NewSystemClock()
	}
	return m
}

// Store exposes the backing store (used by the reaper and migration code).
func (m *Manager) Store() Store { return m.store }

// Clock exposes the configured clock.
func (m *Manager) Clock() Clock { return m.clock }

// CreateSession inserts a new session in StateCreating with its creation
// deadline already armed. A zero CreateTimeout means the creating state is
// not time-bounded.
func (m *Manager) CreateSession(ctx context.Context, id string, policy Policy) (*Session, error) {
	session := &Session{
		ID:             id,
		Version:        1,
		State:          StateCreating,
		Policy:         policy,
		LastActivityAt: m.clock.Now(),
	}
	if policy.CreateTimeout > 0 {
		dl := NewDeadline(m.clock, policy.CreateTimeout)
		session.CreateDeadline = &dl
	}
	if err := m.store.Create(ctx, session); err != nil {
		return nil, err
	}
	return session, nil
}

// Get loads a session.
func (m *Manager) Get(ctx context.Context, id string) (*Session, error) {
	return m.store.Get(ctx, id)
}

// MarkRunning completes creation: StateCreating -> StateRunning.
func (m *Manager) MarkRunning(ctx context.Context, id string) (*Session, error) {
	return m.transition(ctx, id, OpActivate)
}

// Suspend moves StateRunning -> StateSuspended. Suspending an already
// suspended session succeeds idempotently without extending retention.
func (m *Manager) Suspend(ctx context.Context, id string) (*Session, error) {
	return m.transition(ctx, id, OpSuspend)
}

// Resume moves StateSuspended -> StateRunning and re-arms the idle deadline.
func (m *Manager) Resume(ctx context.Context, id string) (*Session, error) {
	return m.transition(ctx, id, OpResume)
}

// Destroy moves any live session to StateDestroying. It is idempotent for
// sessions already destroying or destroyed.
func (m *Manager) Destroy(ctx context.Context, id string) (*Session, error) {
	return m.transition(ctx, id, OpDestroy)
}

// FinalizeDestroy moves StateDestroying -> StateDestroyed after every release
// step has succeeded. Depending on Manager options it retains a tombstone or
// purges the record.
func (m *Manager) FinalizeDestroy(ctx context.Context, id string) (*Session, error) {
	session, err := m.transition(ctx, id, OpFinalizeDestroy)
	if err != nil {
		return nil, err
	}
	if m.purge {
		if err := m.store.Delete(ctx, id); err != nil {
			return nil, err
		}
	}
	return session, nil
}

// transition is the unified mutation entry: read state and version, resolve
// the requested transition against the table, then write back carrying the
// version. A version conflict means another instance won the race, so the
// whole read/decide/write cycle restarts instead of overwriting.
func (m *Manager) transition(ctx context.Context, id string, op Operation) (*Session, error) {
	for {
		current, err := m.store.Get(ctx, id)
		if err != nil {
			return nil, err
		}

		target, err := resolveTransition(id, op, current.State)
		if err != nil {
			return nil, err
		}

		updated, err := m.store.CompareAndUpdate(ctx, id, current.Version,
			func(stored *Session) (*Session, error) {
				if stored.State == target {
					// Idempotent request: leave timers and version untouched.
					return nil, errNoChange
				}
				stored.State = target
				m.applyDeadline(stored, op)
				return stored, nil
			})

		switch {
		case err == nil:
			return updated, nil
		case errors.Is(err, errNoChange):
			// No write happened; the snapshot read at the top of the loop is
			// still the current truth.
			return current, nil
		case errors.Is(err, ErrVersionConflict):
			continue
		default:
			return nil, err
		}
	}
}

// applyDeadline (re)arms the deadlines implied by a committed transition.
// Deadlines of states the session leaves are always cleared so no stale
// timer can fire against the wrong state.
func (m *Manager) applyDeadline(s *Session, op Operation) {
	now := m.clock.Now()
	switch op {
	case OpActivate:
		s.CreateDeadline = nil
		s.LastActivityAt = now
		if s.Policy.IdleTimeout > 0 {
			dl := NewDeadline(m.clock, s.Policy.IdleTimeout)
			s.IdleDeadline = &dl
		} else {
			s.IdleDeadline = nil
		}
	case OpSuspend:
		s.IdleDeadline = nil
		if s.Policy.SuspendedMaxLifetime > 0 {
			dl := NewDeadline(m.clock, s.Policy.SuspendedMaxLifetime)
			s.SuspendDeadline = &dl
		} else {
			s.SuspendDeadline = nil
		}
	case OpResume:
		s.SuspendDeadline = nil
		s.LastActivityAt = now
		if s.Policy.IdleTimeout > 0 {
			dl := NewDeadline(m.clock, s.Policy.IdleTimeout)
			s.IdleDeadline = &dl
		} else {
			s.IdleDeadline = nil
		}
	case OpDestroy:
		s.CreateDeadline = nil
		s.IdleDeadline = nil
		s.SuspendDeadline = nil
		if s.DestroyingSince.IsZero() {
			s.DestroyingSince = now
		}
	case OpFinalizeDestroy:
		// Terminal state: nothing to arm.
	}
}

// RecordActivity renews a running session. It shares the exact
// compare-and-update primitive used by idle reclamation: the read snapshot's
// version must still be current when the new idle deadline is committed. If
// a reaper commits first, the version has moved and this call retries against
// the new state rather than resurrecting anything; conversely, when this
// commits first the reaper's stale-version update is rejected.
func (m *Manager) RecordActivity(ctx context.Context, id string) (*Session, error) {
	for {
		current, err := m.store.Get(ctx, id)
		if err != nil {
			return nil, err
		}

		switch current.State {
		case StateRunning:
			// Activity is only meaningful for a running session; fall
			// through to the CAS below.
		case StateDestroying:
			return nil, &TransitionError{
				ID: id, Op: OpResume, From: current.State, To: StateRunning,
				Reason:    ErrDestroyInProgress,
				ReasonMsg: "cannot record activity while destruction is in progress",
			}
		default:
			return nil, &TransitionError{
				ID: id, Op: OpResume, From: current.State, To: StateRunning,
				Reason:    ErrInvalidTransition,
				ReasonMsg: "activity can only be recorded on a running session",
			}
		}

		updated, err := m.store.CompareAndUpdate(ctx, id, current.Version,
			func(stored *Session) (*Session, error) {
				stored.LastActivityAt = m.clock.Now()
				if stored.Policy.IdleTimeout > 0 {
					dl := NewDeadline(m.clock, stored.Policy.IdleTimeout)
					stored.IdleDeadline = &dl
				} else {
					stored.IdleDeadline = nil
				}
				return stored, nil
			})
		if errors.Is(err, ErrVersionConflict) {
			// Someone else (reaper or another instance) changed the record;
			// re-read and re-evaluate rather than overwriting.
			continue
		}
		if err != nil {
			return nil, err
		}
		return updated, nil
	}
}

// ReapDue applies at most one timeout decision to the session represented by
// snapshot, using the snapshot's version as the compare basis. It returns
// true when a transition was committed. A version conflict means the record
// moved since the scan (typically an activity renewal) and is reported as
// ErrVersionConflict so the caller skips the stale decision; nothing is
// force-written.
//
// Due detection requires both clocks to agree, which is conservative: within
// a process the monotonic clock ignores wall jumps, while a recovered
// deadline's wall arm is widened by recoveryGrace to protect decisions
// rebuilt after a restart against skewed instance clocks.
func (m *Manager) ReapDue(ctx context.Context, snapshot *Session, recoveryGrace time.Duration) (bool, error) {
	deadline := snapshot.effectiveDeadline()
	if deadline == nil {
		return false, nil
	}
	if !deadline.Due(m.clock, recoveryGrace) {
		return false, nil
	}

	op, err := timeoutOperation(snapshot)
	if err != nil {
		return false, err
	}

	target, err := resolveTransition(snapshot.ID, op, snapshot.State)
	if err != nil {
		return false, err
	}

	_, err = m.store.CompareAndUpdate(ctx, snapshot.ID, snapshot.Version,
		func(stored *Session) (*Session, error) {
			if stored.State == target {
				return nil, errNoChange
			}
			stored.State = target
			m.applyDeadline(stored, op)
			return stored, nil
		})
	if errors.Is(err, errNoChange) {
		// Another worker committed the same transition; treat as handled.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// timeoutOperation maps a due deadline in the current state to the operation
// that must run.
func timeoutOperation(s *Session) (Operation, error) {
	switch s.State {
	case StateCreating:
		return OpDestroy, nil
	case StateRunning:
		if s.Policy.IdleTimeoutAction == IdleDestroy {
			return OpDestroy, nil
		}
		return OpSuspend, nil
	case StateSuspended:
		return OpDestroy, nil
	default:
		return 0, &TransitionError{
			ID: s.ID, From: s.State,
			Reason:    ErrInvalidTransition,
			ReasonMsg: "state has no timeout-driven transition",
		}
	}
}

// Rehydrate rebuilds monotonic deadline bases after a process restart from
// the persisted wall-clock instants. It must be called once when a Manager
// starts against a pre-existing store; until then wall-based checks still
// work but monotonic readings are zero.
func (m *Manager) Rehydrate(ctx context.Context) error {
	sessions, err := m.store.List(ctx)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		deadline := session.effectiveDeadline()
		if deadline == nil {
			continue
		}
		rebuilt := deadline.Rehydrate(m.clock)
		_, err := m.store.CompareAndUpdate(ctx, session.ID, session.Version,
			func(stored *Session) (*Session, error) {
				switch stored.State {
				case StateCreating:
					stored.CreateDeadline = &rebuilt
				case StateRunning:
					stored.IdleDeadline = &rebuilt
				case StateSuspended:
					stored.SuspendDeadline = &rebuilt
				default:
					return nil, errNoChange
				}
				return stored, nil
			})
		if errors.Is(err, errNoChange) {
			continue
		}
		if errors.Is(err, ErrVersionConflict) {
			// Another instance is already managing this record; its own
			// rehydration wins. Skip rather than fight it.
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}
