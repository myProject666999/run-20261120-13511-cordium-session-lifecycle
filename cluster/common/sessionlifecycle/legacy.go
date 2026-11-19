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
	"time"
)

// LegacyRecord is the pre-state-machine persistence shape: independent
// booleans plus timestamps. It exists solely to map old data onto the five
// explicit states.
type LegacyRecord struct {
	ID string `json:"id"`

	Created   bool `json:"created"`
	Running   bool `json:"running"`
	Suspended bool `json:"suspended"`
	Destroyed bool `json:"destroyed"`

	CreatedAt      time.Time `json:"createdAt,omitempty"`
	LastActivityAt time.Time `json:"lastActivityAt,omitempty"`
	SuspendedAt    time.Time `json:"suspendedAt,omitempty"`
	DestroyedAt    time.Time `json:"destroyedAt,omitempty"`

	// Version, when present, is carried over; otherwise the migrated record
	// starts at version 1.
	Version int64 `json:"version,omitempty"`

	// Policy is optional; old records may have none, in which case the
	// migrated session has no timeouts and the reaper leaves it alone until
	// an operator attaches a policy.
	Policy Policy `json:"policy,omitempty"`
}

// MigrationAmbiguityError reports a legacy record whose booleans are
// contradictory. The mapping never guesses silently: callers either fix the
// record or explicitly opt into the documented precedence via
// MigrateLegacyWithPrecedence.
type MigrationAmbiguityError struct {
	ID      string
	Record  LegacyRecord
	Reasons []string
}

func (e *MigrationAmbiguityError) Error() string {
	msg := fmt.Sprintf("sessionlifecycle: ambiguous legacy record %q", e.ID)
	for i, reason := range e.Reasons {
		if i == 0 {
			msg += ": "
		} else {
			msg += "; "
		}
		msg += reason
	}
	return msg
}

// MigrateAll maps every legacy record once, in deterministic ID order, and
// inserts the resulting sessions into store. The first ambiguous record (or
// any store error) aborts the batch so partial migrations are visible rather
// than partially applied.
func MigrateAll(ctx context.Context, store Store, records []LegacyRecord) error {
	for _, record := range records {
		session, err := MigrateLegacy(record)
		if err != nil {
			return err
		}
		if err := store.Create(ctx, session); err != nil {
			return err
		}
	}
	return nil
}

// MigrateLegacy maps one legacy record, rejecting contradictory booleans.
//
// Precedence for non-contradictory inputs is destroyed > suspended > running
// > created, matching the lifecycle direction: once destruction was recorded
// nothing can have happened afterwards.
func MigrateLegacy(record LegacyRecord) (*Session, error) {
	reasons := ambiguityReasons(record)
	if len(reasons) > 0 {
		return nil, &MigrationAmbiguityError{
			ID: record.ID, Record: record, Reasons: reasons,
		}
	}
	return legacyToSession(record), nil
}

// MigrateLegacyWithPrecedence maps an ambiguous record deterministically,
// applying destroyed > suspended > running > created explicitly and dropping
// the contradictory lower-precedence flags. It is the documented escape hatch
// when an operator has inspected the record and accepts the precedence
// mapping.
func MigrateLegacyWithPrecedence(record LegacyRecord) *Session {
	return legacyToSession(record)
}

// ambiguityReasons flags legacy records that cannot be mapped even under the
// documented precedence. Booleans in the old schema are cumulative ("the
// session once reached this state"), so the combination created+running+
// suspended is a normal lifecycle trajectory, not a contradiction: the
// highest-precedence set flag simply names the current state. Truly
// ambiguous inputs are an empty flag set and a later state reached without
// the created flag, neither of which has a defensible mapping.
func ambiguityReasons(r LegacyRecord) []string {
	var reasons []string
	if !r.Created && !r.Running && !r.Suspended && !r.Destroyed {
		reasons = append(reasons, "no lifecycle flag is set; cannot tell whether the session was ever created")
	}
	if !r.Created && (r.Running || r.Suspended || r.Destroyed) {
		reasons = append(reasons, "session reached a later state without the created flag")
	}
	return reasons
}

func legacyToSession(r LegacyRecord) *Session {
	session := &Session{
		ID:      r.ID,
		Version: r.Version,
		Policy:  r.Policy,
	}
	if session.Version == 0 {
		session.Version = 1
	}

	// Precedence: destroyed > suspended > running > created.
	switch {
	case r.Destroyed:
		session.State = StateDestroyed
		session.DestroyingSince = r.DestroyedAt
	case r.Suspended:
		session.State = StateSuspended
	case r.Running:
		session.State = StateRunning
	default:
		session.State = StateCreating
	}

	// Timestamps: prefer the field matching the mapped state, then fall back
	// to the most recent known timestamp so LastActivityAt is never zero on a
	// migrated session. Deadlines cannot be reconstructed without the policy
	// history; they are rebuilt lazily by the reaper/manager once a policy is
	// attached, and legacy sessions without timeouts simply have no deadline.
	switch session.State {
	case StateSuspended:
		if !r.SuspendedAt.IsZero() {
			session.LastActivityAt = r.SuspendedAt
		}
	case StateDestroyed:
		if !r.DestroyedAt.IsZero() {
			session.LastActivityAt = r.DestroyedAt
		}
	}
	if session.LastActivityAt.IsZero() {
		session.LastActivityAt = latestNonZero(r.CreatedAt, r.LastActivityAt, r.SuspendedAt, r.DestroyedAt)
	}
	return session
}

func latestNonZero(times ...time.Time) time.Time {
	var latest time.Time
	for _, candidate := range times {
		if candidate.After(latest) {
			latest = candidate
		}
	}
	return latest
}

// LegacySource is the read side of a legacy backend used by LazyLegacyStore.
type LegacySource interface {
	// LoadLegacy returns the legacy record for id, or an error wrapping
	// ErrSessionNotFound when it is absent.
	LoadLegacy(ctx context.Context, id string) (*LegacyRecord, error)
	// ListLegacy returns all remaining legacy records.
	ListLegacy(ctx context.Context) ([]LegacyRecord, error)
	// DeleteLegacy removes a record once it has been migrated.
	DeleteLegacy(ctx context.Context, id string) error
}

// LazyLegacyStore wraps a new-style Store and transparently migrates legacy
// records on first access, so no one-shot migration window is required.
// Reads and writes of an unmigrated ID trigger exactly one migration; the
// migrated record is written through the normal Create path and the legacy
// copy is then deleted.
type LazyLegacyStore struct {
	next   Store
	legacy LegacySource
	// AllowAmbiguous applies the documented precedence mapping instead of
	// surfacing MigrationAmbiguityError when contradictory booleans are met.
	AllowAmbiguous bool
}

// NewLazyLegacyStore wraps next with on-access migration from legacy.
func NewLazyLegacyStore(next Store, legacy LegacySource, allowAmbiguous bool) *LazyLegacyStore {
	return &LazyLegacyStore{next: next, legacy: legacy, AllowAmbiguous: allowAmbiguous}
}

func (l *LazyLegacyStore) migrateOnMiss(ctx context.Context, id string) error {
	record, err := l.legacy.LoadLegacy(ctx, id)
	if err != nil {
		return err
	}

	var session *Session
	if l.AllowAmbiguous {
		session = MigrateLegacyWithPrecedence(*record)
	} else {
		session, err = MigrateLegacy(*record)
		if err != nil {
			return err
		}
	}
	if err := l.next.Create(ctx, session); err != nil {
		return err
	}
	return l.legacy.DeleteLegacy(ctx, id)
}

// Create implements Store; a legacy ID is migrated first and the explicit
// create then follows the normal duplicate handling.
func (l *LazyLegacyStore) Create(ctx context.Context, session *Session) error {
	if _, err := l.next.Get(ctx, session.ID); err != nil {
		if !IsNotFound(err) {
			return err
		}
		if legacyErr := l.migrateOnMiss(ctx, session.ID); legacyErr != nil && !IsNotFound(legacyErr) {
			return legacyErr
		}
	}
	return l.next.Create(ctx, session)
}

// Get implements Store with lazy migration on a cache miss.
func (l *LazyLegacyStore) Get(ctx context.Context, id string) (*Session, error) {
	session, err := l.next.Get(ctx, id)
	if err == nil {
		return session, nil
	}
	if !IsNotFound(err) {
		return nil, err
	}
	if migrateErr := l.migrateOnMiss(ctx, id); migrateErr != nil {
		return nil, migrateErr
	}
	return l.next.Get(ctx, id)
}

// CompareAndUpdate implements Store; an unmigrated ID is migrated before the
// version-checked update runs.
func (l *LazyLegacyStore) CompareAndUpdate(ctx context.Context, id string, expectedVersion int64, mutate MutateFunc) (*Session, error) {
	if _, err := l.next.Get(ctx, id); err != nil {
		if !IsNotFound(err) {
			return nil, err
		}
		if migrateErr := l.migrateOnMiss(ctx, id); migrateErr != nil {
			return nil, migrateErr
		}
	}
	return l.next.CompareAndUpdate(ctx, id, expectedVersion, mutate)
}

// List implements Store and migrates every still-unmigrated legacy record
// before listing, so scans see the union of old and new data.
func (l *LazyLegacyStore) List(ctx context.Context) ([]*Session, error) {
	records, err := l.legacy.ListLegacy(ctx)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if _, err := l.next.Get(ctx, record.ID); err == nil {
			continue // already migrated
		} else if !IsNotFound(err) {
			return nil, err
		}
		if migrateErr := l.migrateOnMiss(ctx, record.ID); migrateErr != nil {
			return nil, migrateErr
		}
	}
	return l.next.List(ctx)
}

// Delete implements Store; the legacy copy is removed as well so a deleted
// session cannot resurrect on the next lazy lookup.
func (l *LazyLegacyStore) Delete(ctx context.Context, id string) error {
	if err := l.next.Delete(ctx, id); err != nil {
		return err
	}
	if err := l.legacy.DeleteLegacy(ctx, id); err != nil && !IsNotFound(err) {
		return err
	}
	return nil
}

// IsNotFound reports whether err wraps ErrSessionNotFound.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrSessionNotFound)
}
