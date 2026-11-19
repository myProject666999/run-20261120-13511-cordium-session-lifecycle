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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateLegacyStatePrecedence(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name   string
		record LegacyRecord
		state  State
	}{
		{
			name:   "created only maps to creating",
			record: LegacyRecord{ID: "a", Created: true, CreatedAt: base},
			state:  StateCreating,
		},
		{
			name:   "created and running maps to running",
			record: LegacyRecord{ID: "b", Created: true, Running: true, LastActivityAt: base},
			state:  StateRunning,
		},
		{
			name:   "suspended maps to suspended",
			record: LegacyRecord{ID: "c", Created: true, Running: true, Suspended: true, SuspendedAt: base},
			state:  StateSuspended,
		},
		{
			name:   "clean destroyed record maps to destroyed",
			record: LegacyRecord{ID: "d", Created: true, Destroyed: true, DestroyedAt: base},
			state:  StateDestroyed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			session, err := MigrateLegacy(tc.record)
			require.NoError(t, err)
			assert.Equal(t, tc.state, session.State)
			assert.Equal(t, int64(1), session.Version)
			assert.False(t, session.LastActivityAt.IsZero())
			assert.Nil(t, session.effectiveDeadline(), "deadlines cannot be reconstructed from booleans")
		})
	}
}

func TestMigrateLegacyRejectsAmbiguity(t *testing.T) {
	ambiguous := []LegacyRecord{
		{ID: "none"}, // no flags at all
		{ID: "uncreated-running", Running: true},
		{ID: "uncreated-suspended", Suspended: true},
	}
	for _, record := range ambiguous {
		t.Run(record.ID, func(t *testing.T) {
			_, err := MigrateLegacy(record)
			require.Error(t, err)
			var ambiguity *MigrationAmbiguityError
			require.True(t, errors.As(err, &ambiguity))
			assert.Equal(t, record.ID, ambiguity.ID)
			assert.NotEmpty(t, ambiguity.Reasons)
		})
	}
}

// TestMigrateLegacyCumulativeFlags documents the old schema's cumulative
// semantics: a trajectory of flags names its highest-precedence state.
func TestMigrateLegacyCumulativeFlags(t *testing.T) {
	session, err := MigrateLegacy(LegacyRecord{
		ID: "trajectory", Created: true, Running: true, Suspended: true, Destroyed: true,
	})
	require.NoError(t, err)
	assert.Equal(t, StateDestroyed, session.State)
}

func TestMigrateLegacyWithPrecedenceForcesMapping(t *testing.T) {
	// A record with no flags is ambiguous under strict mapping but resolves
	// deterministically under explicit precedence (lowest state wins).
	session := MigrateLegacyWithPrecedence(LegacyRecord{ID: "a"})
	assert.Equal(t, StateCreating, session.State)

	session = MigrateLegacyWithPrecedence(LegacyRecord{ID: "b", Created: true, Running: true, Suspended: true})
	assert.Equal(t, StateSuspended, session.State)
}

func TestMigrateAllBatchAndAbort(t *testing.T) {
	store := NewMemoryStore()
	ctx := t.Context()
	records := []LegacyRecord{
		{ID: "a", Created: true},
		{ID: "b", Created: true, Running: true},
	}
	require.NoError(t, MigrateAll(ctx, store, records))

	got, err := store.Get(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, StateCreating, got.State)
	got, err = store.Get(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, StateRunning, got.State)

	// A later batch containing an ambiguous record aborts without touching
	// the already-migrated data.
	err = MigrateAll(ctx, store, []LegacyRecord{{ID: "c"}})
	require.Error(t, err)
	_, err = store.Get(ctx, "c")
	assert.True(t, errors.Is(err, ErrSessionNotFound))
}

// memoryLegacySource is a LegacySource backed by a plain map for tests.
type memoryLegacySource struct {
	records map[string]LegacyRecord
}

func (m *memoryLegacySource) LoadLegacy(_ context.Context, id string) (*LegacyRecord, error) {
	record, ok := m.records[id]
	if !ok {
		return nil, errors.Join(ErrSessionNotFound, errors.New(id))
	}
	return &record, nil
}

func (m *memoryLegacySource) ListLegacy(_ context.Context) ([]LegacyRecord, error) {
	out := make([]LegacyRecord, 0, len(m.records))
	for _, record := range m.records {
		out = append(out, record)
	}
	return out, nil
}

func (m *memoryLegacySource) DeleteLegacy(_ context.Context, id string) error {
	delete(m.records, id)
	return nil
}

func TestLazyLegacyStoreMigratesOnFirstAccess(t *testing.T) {
	ctx := t.Context()
	legacy := &memoryLegacySource{records: map[string]LegacyRecord{
		"old": {ID: "old", Created: true, Running: true},
	}}
	store := NewLazyLegacyStore(NewMemoryStore(), legacy, false)

	// First read migrates through the normal create path.
	session, err := store.Get(ctx, "old")
	require.NoError(t, err)
	assert.Equal(t, StateRunning, session.State)
	assert.Equal(t, int64(1), session.Version)

	// The legacy copy is gone and subsequent reads hit the new store only.
	_, legacyErr := legacy.LoadLegacy(ctx, "old")
	assert.True(t, errors.Is(legacyErr, ErrSessionNotFound))
	session2, err := store.Get(ctx, "old")
	require.NoError(t, err)
	assert.Equal(t, session.Version, session2.Version)

	// List migrates anything still pending.
	legacy.records["other"] = LegacyRecord{ID: "other", Created: true}
	all, err := store.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Empty(t, legacy.records)
}

func TestLazyLegacyStoreSurfacesAmbiguity(t *testing.T) {
	ctx := t.Context()
	legacy := &memoryLegacySource{records: map[string]LegacyRecord{
		"weird": {ID: "weird"}, // no flags: ambiguous
	}}

	strict := NewLazyLegacyStore(NewMemoryStore(), legacy, false)
	_, err := strict.Get(ctx, "weird")
	require.Error(t, err)
	var ambiguity *MigrationAmbiguityError
	assert.True(t, errors.As(err, &ambiguity))

	// The same record maps deterministically once precedence is accepted;
	// with no flag set at all it lands on the lowest-precedence state.
	legacy.records["weird"] = LegacyRecord{ID: "weird"}
	permissive := NewLazyLegacyStore(NewMemoryStore(), legacy, true)
	session, err := permissive.Get(ctx, "weird")
	require.NoError(t, err)
	assert.Equal(t, StateCreating, session.State)
}

func TestLazyLegacyStoreDeleteRemovesBothCopies(t *testing.T) {
	ctx := t.Context()
	legacy := &memoryLegacySource{records: map[string]LegacyRecord{
		"old": {ID: "old", Created: true},
	}}
	store := NewLazyLegacyStore(NewMemoryStore(), legacy, false)

	require.NoError(t, store.Delete(ctx, "old"))
	_, err := store.Get(ctx, "old")
	assert.True(t, errors.Is(err, ErrSessionNotFound))
	assert.Empty(t, legacy.records)
}

func TestLazyLegacyStoreCreateMigratesThenDedupes(t *testing.T) {
	ctx := t.Context()
	legacy := &memoryLegacySource{records: map[string]LegacyRecord{
		"old": {ID: "old", Created: true, Running: true},
	}}
	store := NewLazyLegacyStore(NewMemoryStore(), legacy, false)

	// A brand-new ID is created directly without touching the legacy backend.
	require.NoError(t, store.Create(ctx, &Session{ID: "new", State: StateCreating, Version: 1}))
	created, err := store.Get(ctx, "new")
	require.NoError(t, err)
	assert.Equal(t, StateCreating, created.State)

	// Creating an ID that only exists in legacy storage first migrates it,
	// and the explicit create then hits the normal duplicate rule rather than
	// silently overwriting the migrated record.
	err = store.Create(ctx, &Session{ID: "old", State: StateCreating, Version: 1})
	require.Error(t, err)

	migrated, err := store.Get(ctx, "old")
	require.NoError(t, err)
	assert.Equal(t, StateRunning, migrated.State, "the legacy record wins over the rejected create")
}

func TestLazyLegacyStoreCompareAndUpdateMigratesFirst(t *testing.T) {
	ctx := t.Context()
	legacy := &memoryLegacySource{records: map[string]LegacyRecord{
		"old": {ID: "old", Created: true, Running: true},
	}}
	store := NewLazyLegacyStore(NewMemoryStore(), legacy, false)

	// First CAS triggers lazy migration at version 1; the update then bumps
	// to version 2 through the normal versioned path.
	updated, err := store.CompareAndUpdate(ctx, "old", 1, func(stored *Session) (*Session, error) {
		stored.State = StateSuspended
		return stored, nil
	})
	require.NoError(t, err)
	assert.Equal(t, StateSuspended, updated.State)
	assert.Equal(t, int64(2), updated.Version)
	assert.Empty(t, legacy.records, "the legacy copy is removed after migration")

	// A stale version is now rejected exactly as on a plain store.
	_, err = store.CompareAndUpdate(ctx, "old", 1, func(stored *Session) (*Session, error) {
		return stored, nil
	})
	assert.True(t, errors.Is(err, ErrVersionConflict))
}

func TestMemoryStoreCompareAndUpdateConflicts(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryStore()
	require.NoError(t, store.Create(ctx, &Session{ID: "s", State: StateCreating}))

	first, err := store.Get(ctx, "s")
	require.NoError(t, err)

	// A competing write bumps the version.
	_, err = store.CompareAndUpdate(ctx, "s", first.Version, func(stored *Session) (*Session, error) {
		stored.State = StateRunning
		return stored, nil
	})
	require.NoError(t, err)

	// The stale expected version is rejected without invoking the mutation's
	// effect.
	_, err = store.CompareAndUpdate(ctx, "s", first.Version, func(stored *Session) (*Session, error) {
		t.Fatal("mutation must not run on a version conflict")
		return stored, nil
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrVersionConflict))

	current, err := store.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateRunning, current.State)
	assert.Equal(t, int64(2), current.Version)
}

func TestMemoryStoreMissingAndDuplicate(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryStore()

	_, err := store.Get(ctx, "missing")
	assert.True(t, errors.Is(err, ErrSessionNotFound))

	require.NoError(t, store.Create(ctx, &Session{ID: "s", State: StateCreating}))
	err = store.Create(ctx, &Session{ID: "s", State: StateCreating})
	require.Error(t, err)

	// Delete is idempotent.
	require.NoError(t, store.Delete(ctx, "s"))
	require.NoError(t, store.Delete(ctx, "s"))
	_, err = store.CompareAndUpdate(ctx, "s", 1, func(stored *Session) (*Session, error) {
		return stored, nil
	})
	assert.True(t, errors.Is(err, ErrSessionNotFound))
}
