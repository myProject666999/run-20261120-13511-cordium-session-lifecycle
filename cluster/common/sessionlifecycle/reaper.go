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

// DestroyHooks performs the external side of destruction. Every step must be
// idempotent: the pipeline retries the whole sequence after any failure, and
// sessions that fail stay in StateDestroying (they can never move back to a
// live state), so a hook must tolerate being called more than once.
type DestroyHooks interface {
	// ReleaseResources frees compute resources (containers, processes).
	ReleaseResources(ctx context.Context, sessionID string) error
	// CloseConnections drops tunnels and open network connections.
	CloseConnections(ctx context.Context, sessionID string) error
	// CleanupStorage removes or archives persistent storage of the session.
	CleanupStorage(ctx context.Context, sessionID string) error
}

// NoopHooks is a DestroyHooks implementation whose steps always succeed.
type NoopHooks struct{}

// ReleaseResources implements DestroyHooks.
func (NoopHooks) ReleaseResources(context.Context, string) error { return nil }

// CloseConnections implements DestroyHooks.
func (NoopHooks) CloseConnections(context.Context, string) error { return nil }

// CleanupStorage implements DestroyHooks.
func (NoopHooks) CleanupStorage(context.Context, string) error { return nil }

// ReaperConfig configures the background reaper.
type ReaperConfig struct {
	// BatchSize bounds the number of sessions processed in one scan batch.
	// The reaper yields between batches so a mass-expiry cannot overload the
	// backend. Defaults to 100.
	BatchSize int
	// BatchPause is the yield between batches. Defaults to 10ms.
	BatchPause time.Duration
	// RecoveryGrace widens wall-clock due checks for deadlines rebuilt after
	// a restart, absorbing small clock skews conservatively. Defaults to 5s.
	RecoveryGrace time.Duration
	// MaxTick is the longest idle wait when no deadline is pending, so the
	// reaper still re-scans after external changes. Defaults to 30s.
	MaxTick time.Duration
	// OnError, when set, receives non-fatal per-session errors (conflicts,
	// hook failures). The scan always continues with the next session.
	OnError func(sessionID string, err error)
}

// Reaper scans persisted sessions, applies due timeout transitions through
// the Manager's compare-and-update path, and drives the destroy pipeline for
// sessions in StateDestroying. It holds no per-session timer state: after a
// restart the next wake-up is recomputed purely from the persisted records.
type Reaper struct {
	manager *Manager
	hooks   DestroyHooks
	cfg     ReaperConfig
}

// NewReaper builds a reaper. hooks may be nil, in which case NoopHooks is
// used.
func NewReaper(manager *Manager, hooks DestroyHooks, cfg *ReaperConfig) *Reaper {
	r := &Reaper{manager: manager, hooks: NoopHooks{}}
	if hooks != nil {
		r.hooks = hooks
	}
	if cfg != nil {
		r.cfg = *cfg
	}
	if r.cfg.BatchSize <= 0 {
		r.cfg.BatchSize = 100
	}
	if r.cfg.BatchPause <= 0 {
		r.cfg.BatchPause = 10 * time.Millisecond
	}
	if r.cfg.RecoveryGrace <= 0 {
		r.cfg.RecoveryGrace = 5 * time.Second
	}
	if r.cfg.MaxTick <= 0 {
		r.cfg.MaxTick = 30 * time.Second
	}
	return r
}

// ScanResult summarizes one scan.
type ScanResult struct {
	// Scanned is the number of sessions inspected.
	Scanned int
	// Reaped is the number of timeout transitions that committed.
	Reaped int
	// Finalized is the number of destroy pipelines completed to
	// StateDestroyed.
	Finalized int
	// Batches is the number of batches the scan was split into.
	Batches int
	// Errors collects non-fatal per-session errors.
	Errors []error
}

// ScanOnce lists every session and processes them in batches of BatchSize.
// Each session is handled independently: a failure is recorded and the scan
// continues, so one broken session cannot block mass reclamation.
func (r *Reaper) ScanOnce(ctx context.Context) (ScanResult, error) {
	sessions, err := r.manager.Store().List(ctx)
	if err != nil {
		return ScanResult{}, err
	}

	result := ScanResult{Batches: 1}
	for i, session := range sessions {
		if i > 0 && i%r.cfg.BatchSize == 0 {
			result.Batches++
			if err := sleepCtx(ctx, r.cfg.BatchPause); err != nil {
				return result, err
			}
		}
		result.Scanned++

		reaped, err := r.handleSession(ctx, session)
		if err != nil {
			if errors.Is(err, ErrVersionConflict) {
				// The record moved between List and compare (typically an
				// activity renewal). The stale decision is simply dropped;
				// the next scan re-evaluates the fresh record.
				continue
			}
			result.Errors = append(result.Errors, err)
			if r.cfg.OnError != nil {
				r.cfg.OnError(session.ID, err)
			}
		}
		if reaped {
			result.Reaped++
		}

		// ReapDue may have just moved a timed-out session into destroying;
		// re-read so the pipeline runs in the same scan when due.
		fresh, getErr := r.manager.Store().Get(ctx, session.ID)
		if getErr != nil {
			if errors.Is(getErr, ErrSessionNotFound) {
				continue
			}
			result.Errors = append(result.Errors, getErr)
			continue
		}
		if fresh.State == StateDestroying {
			done, pipeErr := r.RunDestroyPipeline(ctx, fresh)
			if pipeErr != nil {
				result.Errors = append(result.Errors, pipeErr)
				if r.cfg.OnError != nil {
					r.cfg.OnError(fresh.ID, pipeErr)
				}
				continue
			}
			if done {
				result.Finalized++
			}
		}
	}
	return result, nil
}

// handleSession applies a due timeout transition, if any, and reports whether
// a transition committed.
func (r *Reaper) handleSession(ctx context.Context, session *Session) (bool, error) {
	return r.manager.ReapDue(ctx, session, r.cfg.RecoveryGrace)
}

// RunDestroyPipeline executes the idempotent release steps for a session in
// StateDestroying and finalizes the state when every step succeeds. It
// returns done=false when the session is not in StateDestroying (e.g. a
// concurrent worker already finalized it). Any hook failure leaves the
// session in StateDestroying and the error is returned for retry on the next
// scan; nothing rolls back to a live state.
func (r *Reaper) RunDestroyPipeline(ctx context.Context, session *Session) (bool, error) {
	if session.State != StateDestroying {
		return false, nil
	}

	// The steps run in a fixed order, each guarded for idempotency by the
	// implementer. A failure short-circuits: already-completed steps will be
	// invoked again on retry and must no-op.
	steps := []struct {
		name string
		fn   func(context.Context, string) error
	}{
		{"release-resources", r.hooks.ReleaseResources},
		{"close-connections", r.hooks.CloseConnections},
		{"cleanup-storage", r.hooks.CleanupStorage},
	}
	for _, step := range steps {
		if err := step.fn(ctx, session.ID); err != nil {
			return false, &PipelineError{
				ID: session.ID, Step: step.name, Err: err,
			}
		}
	}

	if _, err := r.manager.FinalizeDestroy(ctx, session.ID); err != nil {
		return false, err
	}
	return true, nil
}

// PipelineError names the release step that failed. The session remains in
// StateDestroying and the whole pipeline is retried later.
type PipelineError struct {
	ID   string
	Step string
	Err  error
}

func (e *PipelineError) Error() string {
	return "sessionlifecycle: destroy pipeline step " + e.Step +
		" failed for session " + e.ID + ": " + e.Err.Error()
}

// Unwrap exposes the underlying hook error.
func (e *PipelineError) Unwrap() error { return e.Err }

// NextWakeup recomputes how long the reaper should sleep before the next
// scan from the persisted deadlines alone, so it is safe to call on a freshly
// restarted process with no in-memory timers. It returns MaxTick when nothing
// is due and false; a non-positive duration with true means "due now".
func (r *Reaper) NextWakeup(ctx context.Context) (time.Duration, bool) {
	sessions, err := r.manager.Store().List(ctx)
	if err != nil {
		return r.cfg.MaxTick, false
	}
	clock := r.manager.Clock()
	// soonest tracks the smallest remaining time across sessions, starting
	// unbounded; it is only capped to MaxTick on return so an empty or
	// untimed store still produces a periodic scan.
	var soonest time.Duration = -1
	due := false
	for _, session := range sessions {
		deadline := session.effectiveDeadline()
		if deadline == nil {
			if session.State == StateDestroying {
				// A stuck destroy must be retried promptly.
				due = true
				soonest = 0
			}
			continue
		}
		if deadline.Due(clock, r.cfg.RecoveryGrace) {
			due = true
			soonest = 0
			continue
		}
		remaining := time.Duration(deadline.Mono - clock.Mono())
		if remaining < 0 {
			// A stale monotonic basis from a previous process must not
			// produce a negative sleep; rehydration rebuilds these values.
			remaining = 0
		}
		if soonest < 0 || remaining < soonest {
			soonest = remaining
		}
	}
	if soonest < 0 {
		return r.cfg.MaxTick, due
	}
	return soonest, due
}

// Run loops scans until ctx is canceled. The wait is computed from persisted
// state on every iteration, which makes process restart trivial; with a
// FakeClock the wait is additionally interrupted the moment the clock moves,
// so tests never sleep for real deadlines.
func (r *Reaper) Run(ctx context.Context) error {
	for {
		if _, err := r.ScanOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if r.cfg.OnError != nil {
				r.cfg.OnError("", err)
			}
		}

		wait, _ := r.NextWakeup(ctx)
		if wait <= 0 {
			wait = r.cfg.BatchPause
		}

		var wake <-chan time.Time
		if fc, ok := r.manager.Clock().(*FakeClock); ok {
			// Deterministic tests: wake immediately on any clock movement.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-fc.onChange():
			}
			continue
		}
		timer := time.NewTimer(wait)
		wake = timer.C
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-wake:
		}
	}
}

// sleepCtx pauses for d but returns early with the context error.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
