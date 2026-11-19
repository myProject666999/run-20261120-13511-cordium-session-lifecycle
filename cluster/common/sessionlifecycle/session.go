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
	"fmt"
	"sync"
	"time"
)

// IdleAction decides what the reaper does with a running session whose idle
// deadline has elapsed.
type IdleAction int

const (
	// IdleSuspend moves an idle session to StateSuspended.
	IdleSuspend IdleAction = iota
	// IdleDestroy moves an idle session straight to StateDestroying.
	IdleDestroy
)

// Policy holds the per-session timing policy. Zero durations mean "no such
// timeout". IdleTimeoutAction only applies when IdleTimeout is set.
type Policy struct {
	// CreateTimeout bounds StateCreating. On expiry the session moves to
	// StateDestroying.
	CreateTimeout time.Duration
	// IdleTimeout bounds the time between recorded activities while running.
	IdleTimeout time.Duration
	// IdleTimeoutAction selects suspend vs destroy on idle expiry.
	IdleTimeoutAction IdleAction
	// SuspendedMaxLifetime bounds StateSuspended. On expiry the session is
	// destroyed.
	SuspendedMaxLifetime time.Duration
}

// Session is the persisted lifecycle record. All fields are safe to serialize;
// there are no in-memory-only timers to lose on restart.
type Session struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
	State   State  `json:"state"`
	Policy  Policy `json:"policy"`

	// LastActivityAt is the wall-clock instant of the latest recorded
	// activity; it is the human-visible anchor for the idle deadline.
	LastActivityAt time.Time `json:"lastActivityAt"`

	// CreateDeadline is set while in StateCreating.
	CreateDeadline *Deadline `json:"createDeadline,omitempty"`
	// IdleDeadline is set while in StateRunning and reset on every activity.
	IdleDeadline *Deadline `json:"idleDeadline,omitempty"`
	// SuspendDeadline is set while in StateSuspended.
	SuspendDeadline *Deadline `json:"suspendDeadline,omitempty"`

	// DestroyingSince records when StateDestroying was entered; it is kept
	// purely for observability and retry accounting.
	DestroyingSince time.Time `json:"destroyingSince,omitempty"`
}

// Clone returns a deep copy so callers can never mutate store contents
// through a read result.
func (s *Session) Clone() *Session {
	if s == nil {
		return nil
	}
	cp := *s
	if s.CreateDeadline != nil {
		dl := *s.CreateDeadline
		cp.CreateDeadline = &dl
	}
	if s.IdleDeadline != nil {
		dl := *s.IdleDeadline
		cp.IdleDeadline = &dl
	}
	if s.SuspendDeadline != nil {
		dl := *s.SuspendDeadline
		cp.SuspendDeadline = &dl
	}
	return &cp
}

// effectiveDeadline returns the deadline that applies in the session's
// current state, or nil when the state is untimed.
func (s *Session) effectiveDeadline() *Deadline {
	switch s.State {
	case StateCreating:
		return s.CreateDeadline
	case StateRunning:
		return s.IdleDeadline
	case StateSuspended:
		return s.SuspendDeadline
	default:
		return nil
	}
}

// ErrVersionConflict is returned by a compare-and-update when the stored
// version no longer matches the version the mutation was computed against.
// The Manager treats it as "re-read and retry", never as a hard failure.
var ErrVersionConflict = errors.New("sessionlifecycle: stored version changed")

// MutateFunc receives a private copy of the current record and returns the
// record to persist, or an error to abort. It must not perform side effects:
// on version conflict it runs again with a fresh copy.
type MutateFunc func(current *Session) (*Session, error)

// Store is the persistence boundary. All implementations must be safe for
// concurrent use by multiple instances sharing the same backing data.
type Store interface {
	// Create inserts a new session. It fails if the ID already exists.
	Create(ctx context.Context, session *Session) error
	// Get returns a private copy of the session, or ErrSessionNotFound.
	Get(ctx context.Context, id string) (*Session, error)
	// CompareAndUpdate re-reads the record, invokes mutate on a private copy
	// and writes it back only if the version observed at read time is still
	// current, bumping the version on success. On a stale version it returns
	// ErrVersionConflict without calling or persisting anything destructive.
	CompareAndUpdate(ctx context.Context, id string, expectedVersion int64, mutate MutateFunc) (*Session, error)
	// List returns private copies of all sessions.
	List(ctx context.Context) ([]*Session, error)
	// Delete removes the record entirely (optional tombstone semantics: a
	// store may instead retain a StateDestroyed record).
	Delete(ctx context.Context, id string) error
}

// MemoryStore is the reference in-process Store. A single mutex serializes
// compare-and-updates, which makes the read/decide/write sequence atomic
// against other goroutines; optimistic versioning still matters because a
// distributed backend would only guarantee the compare-and-update itself,
// and tests exercise conflicts explicitly.
type MemoryStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{sessions: make(map[string]*Session)}
}

// Create implements Store.
func (m *MemoryStore) Create(_ context.Context, session *Session) error {
	if session == nil {
		return errors.New("sessionlifecycle: cannot create nil session")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[session.ID]; ok {
		return fmt.Errorf("sessionlifecycle: session %q already exists", session.ID)
	}
	stored := session.Clone()
	if stored.Version == 0 {
		stored.Version = 1
	}
	m.sessions[stored.ID] = stored
	return nil
}

// Get implements Store.
func (m *MemoryStore) Get(_ context.Context, id string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	session, ok := m.sessions[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	return session.Clone(), nil
}

// CompareAndUpdate implements Store.
func (m *MemoryStore) CompareAndUpdate(_ context.Context, id string, expectedVersion int64, mutate MutateFunc) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.sessions[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	if current.Version != expectedVersion {
		return nil, fmt.Errorf("%w: session %s at version %d, expected %d",
			ErrVersionConflict, id, current.Version, expectedVersion)
	}

	next, err := mutate(current.Clone())
	if err != nil {
		return nil, err
	}
	if next == nil {
		return nil, errors.New("sessionlifecycle: mutate returned nil session")
	}
	if next.ID != id {
		return nil, fmt.Errorf("sessionlifecycle: mutate changed session id %q -> %q", id, next.ID)
	}
	next.Version = current.Version + 1
	m.sessions[id] = next
	return next.Clone(), nil
}

// List implements Store.
func (m *MemoryStore) List(_ context.Context) ([]*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Session, 0, len(m.sessions))
	for _, session := range m.sessions {
		out = append(out, session.Clone())
	}
	return out, nil
}

// Delete implements Store. Deleting a missing session is a no-op so cleanup
// retries stay idempotent.
func (m *MemoryStore) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, id)
	return nil
}

// ExportSessions returns deep copies of all records. Used together with
// ImportSessions to simulate process restarts.
func (m *MemoryStore) ExportSessions(ctx context.Context) ([]*Session, error) {
	return m.List(ctx)
}

// ImportSessions replaces all contents with the given records (deep-copied).
func (m *MemoryStore) ImportSessions(_ context.Context, sessions []*Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions = make(map[string]*Session, len(sessions))
	for _, session := range sessions {
		m.sessions[session.ID] = session.Clone()
	}
	return nil
}
