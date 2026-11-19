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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gateStore wraps MemoryStore and can run an interceptor immediately before a
// compare-and-update commits. It models two instances racing on one session:
// the interceptor performs the competing operation through the same CAS
// primitive, and a depth guard prevents the interceptor's own CAS from
// re-entering.
type gateStore struct {
	*MemoryStore
	mu        sync.Mutex
	beforeCAS func(id string, expectedVersion int64)
	depth     int
}

func (g *gateStore) CompareAndUpdate(ctx context.Context, id string, expectedVersion int64, mutate MutateFunc) (*Session, error) {
	g.mu.Lock()
	fn := g.beforeCAS
	depth := g.depth
	g.depth++
	g.mu.Unlock()

	if fn != nil && depth == 0 {
		fn(id, expectedVersion)
	}
	return g.MemoryStore.CompareAndUpdate(ctx, id, expectedVersion, mutate)
}

func (g *gateStore) setGate(fn func(id string, expectedVersion int64)) {
	g.mu.Lock()
	g.beforeCAS = fn
	g.depth = 0
	g.mu.Unlock()
}

func TestCreateSessionArmsCreationDeadline(t *testing.T) {
	mgr, clock, store := newHarness(t, false)
	ctx := t.Context()

	session, err := mgr.CreateSession(ctx, "c", testPolicy())
	require.NoError(t, err)
	assert.Equal(t, StateCreating, session.State)
	require.NotNil(t, session.CreateDeadline)
	assert.True(t, session.CreateDeadline.Wall.Equal(clock.Now().Add(time.Minute)))
	assert.False(t, session.CreateDeadline.Recovered)
	assert.Nil(t, session.IdleDeadline)

	// A zero create timeout means an untimed creating state.
	_, err = mgr.CreateSession(ctx, "untimed", Policy{})
	require.NoError(t, err)
	untimed, err := store.Get(ctx, "untimed")
	require.NoError(t, err)
	assert.Nil(t, untimed.CreateDeadline)
}

func TestTransitionsArmAndClearDeadlines(t *testing.T) {
	mgr, clock, _ := newHarness(t, false)
	ctx := t.Context()

	sessionInState(t, mgr, "s", StateCreating)
	session, err := mgr.MarkRunning(ctx, "s")
	require.NoError(t, err)
	assert.Nil(t, session.CreateDeadline)
	require.NotNil(t, session.IdleDeadline)
	assert.True(t, session.LastActivityAt.Equal(clock.Now()))

	session, err = mgr.Suspend(ctx, "s")
	require.NoError(t, err)
	assert.Nil(t, session.IdleDeadline)
	require.NotNil(t, session.SuspendDeadline)

	clock.Advance(10 * time.Second)
	session, err = mgr.Resume(ctx, "s")
	require.NoError(t, err)
	assert.Nil(t, session.SuspendDeadline)
	require.NotNil(t, session.IdleDeadline)
	assert.True(t, session.LastActivityAt.Equal(clock.Now()))
	assert.Equal(t, clock.Mono()+MonoTime(time.Minute), session.IdleDeadline.Mono)

	session, err = mgr.Destroy(ctx, "s")
	require.NoError(t, err)
	assert.Nil(t, session.CreateDeadline)
	assert.Nil(t, session.IdleDeadline)
	assert.Nil(t, session.SuspendDeadline)
	assert.False(t, session.DestroyingSince.IsZero())
}

func TestRecordActivityResetsIdleDeadline(t *testing.T) {
	mgr, clock, store := newHarness(t, false)
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateRunning)

	clock.Advance(30 * time.Second)
	before, err := store.Get(ctx, "s")
	require.NoError(t, err)

	updated, err := mgr.RecordActivity(ctx, "s")
	require.NoError(t, err)
	assert.True(t, updated.LastActivityAt.Equal(clock.Now()))
	assert.NotEqual(t, before.IdleDeadline.Mono, updated.IdleDeadline.Mono)
	assert.Equal(t, clock.Mono()+MonoTime(time.Minute), updated.IdleDeadline.Mono)
	assert.Equal(t, before.Version+1, updated.Version)
}

func TestRecordActivityRejectedOutsideRunning(t *testing.T) {
	mgr, _, _ := newHarness(t, false)
	ctx := t.Context()

	for _, state := range []State{StateCreating, StateSuspended, StateDestroying, StateDestroyed} {
		t.Run(state.String(), func(t *testing.T) {
			id := "s-" + state.String()
			sessionInState(t, mgr, id, state)
			_, err := mgr.RecordActivity(ctx, id)
			require.Error(t, err)
			if state == StateDestroying {
				assert.True(t, errors.Is(err, ErrDestroyInProgress))
			} else {
				assert.True(t, errors.Is(err, ErrInvalidTransition))
			}
		})
	}
}

// TestActivityRenewalInvalidatesStaleReapDecision is the headline race:
// an idle check has read the old version and decided to reap, but before its
// compare-and-update commits, an activity update commits through the same
// primitive. The stale reaping decision must fail and the renewed session
// must survive with a reset idle timer.
func TestActivityRenewalInvalidatesStaleReapDecision(t *testing.T) {
	clock := NewFakeClock()
	store := &gateStore{MemoryStore: NewMemoryStore()}
	mgr := NewManager(store, &Options{Clock: clock})
	ctx := t.Context()

	_, err := mgr.CreateSession(ctx, "s", testPolicy())
	require.NoError(t, err)
	_, err = mgr.MarkRunning(ctx, "s")
	require.NoError(t, err)

	// The idle deadline elapses, so a reaper reading now decides to suspend.
	clock.Advance(time.Minute + time.Second)
	snapshot, err := store.Get(ctx, "s")
	require.NoError(t, err)
	require.True(t, snapshot.IdleDeadline.Due(clock, 0))

	var renewCommitted atomic.Bool
	store.setGate(func(id string, _ int64) {
		if id != "s" || renewCommitted.Load() {
			return
		}
		// This runs inside the reaper's CAS, before its write: the competing
		// activity renewal commits first and bumps the version.
		_, renewErr := mgr.RecordActivity(ctx, id)
		require.NoError(t, renewErr)
		renewCommitted.Store(true)
	})

	reaped, err := mgr.ReapDue(ctx, snapshot, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrVersionConflict), "stale decision must be rejected, got %v", err)
	assert.False(t, reaped)
	assert.True(t, renewCommitted.Load())

	final, err := store.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateRunning, final.State, "the renewed session must not be reaped")
	assert.True(t, final.LastActivityAt.Equal(clock.Now()))
	// The renewed deadline is once again a full idle timeout away.
	assert.Equal(t, clock.Mono()+MonoTime(time.Minute), final.IdleDeadline.Mono)

	// A subsequent scan sees a fresh, not-due record.
	reaped, err = mgr.ReapDue(ctx, final, 0)
	require.NoError(t, err)
	assert.False(t, reaped)
}

// TestReapCommittedFirstDefeatsLateActivity is the reverse interleaving:
// the reaper commits the suspend transition first, so a late activity update
// retries against the suspended state and is rejected with a distinguishable
// error instead of resurrecting the session.
func TestReapCommittedFirstDefeatsLateActivity(t *testing.T) {
	clock := NewFakeClock()
	store := &gateStore{MemoryStore: NewMemoryStore()}
	mgr := NewManager(store, &Options{Clock: clock})
	ctx := t.Context()

	_, err := mgr.CreateSession(ctx, "s", testPolicy())
	require.NoError(t, err)
	_, err = mgr.MarkRunning(ctx, "s")
	require.NoError(t, err)
	clock.Advance(time.Minute + time.Second)

	snapshot, err := store.Get(ctx, "s")
	require.NoError(t, err)
	reaped, err := mgr.ReapDue(ctx, snapshot, 0)
	require.NoError(t, err)
	assert.True(t, reaped)

	committed, err := store.Get(ctx, "s")
	require.NoError(t, err)
	require.Equal(t, StateSuspended, committed.State)

	// An activity update based on the old running-version now conflicts,
	// retries against the suspended record, and is cleanly rejected.
	_, err = mgr.RecordActivity(ctx, "s")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidTransition))

	again, err := store.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateSuspended, again.State)
	assert.Equal(t, committed.Version, again.Version, "a rejected activity update writes nothing")
}

// TestOptimisticConflictRetriesWithReread verifies the generic retry loop:
// an external writer bumping the version causes the transition to re-read and
// still succeed rather than overwrite the concurrent change.
func TestOptimisticConflictRetriesWithReread(t *testing.T) {
	clock := NewFakeClock()
	store := &gateStore{MemoryStore: NewMemoryStore()}
	mgr := NewManager(store, &Options{Clock: clock})
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateRunning)

	var attempts atomic.Int32
	store.setGate(func(id string, expectedVersion int64) {
		if id != "s" || attempts.Load() > 0 {
			return
		}
		attempts.Store(1)
		// A competing writer changes the record between our read and write.
		current, getErr := store.MemoryStore.Get(ctx, id)
		require.NoError(t, getErr)
		require.Equal(t, expectedVersion, current.Version)
		_, casErr := store.MemoryStore.CompareAndUpdate(ctx, id, current.Version,
			func(stored *Session) (*Session, error) {
				stored.DestroyingSince = clock.Now()
				return stored, nil
			})
		require.NoError(t, casErr)
	})

	session, err := mgr.Suspend(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateSuspended, session.State)
	assert.Equal(t, int32(1), attempts.Load())
	// Initial create (v1) -> activate (v2) -> external competing write (v3)
	// -> retry re-read and suspend (v4).
	assert.Equal(t, int64(4), session.Version)
	assert.False(t, session.DestroyingSince.IsZero(), "the external mutation survived and was not overwritten")
}

// TestPurgeOnFinalizeVsTombstone pins the two documented answers to a
// repeated destroy of a destroyed session.
func TestPurgeOnFinalizeVsTombstone(t *testing.T) {
	t.Run("tombstone store returns idempotent success", func(t *testing.T) {
		mgr, _, _ := newHarness(t, false)
		ctx := t.Context()
		sessionInState(t, mgr, "s", StateDestroyed)

		again, err := mgr.Destroy(ctx, "s")
		require.NoError(t, err)
		assert.Equal(t, StateDestroyed, again.State)

		got, err := mgr.Get(ctx, "s")
		require.NoError(t, err)
		assert.Equal(t, StateDestroyed, got.State)
	})

	t.Run("purging store returns not found", func(t *testing.T) {
		mgr, _, _ := newHarness(t, true)
		ctx := t.Context()
		sessionInState(t, mgr, "s", StateDestroyed)

		_, err := mgr.Get(ctx, "s")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrSessionNotFound))

		// A repeated destroy now surfaces "not found", matching an upstream
		// cleaner that already removed the record itself.
		_, err = mgr.Destroy(ctx, "s")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrSessionNotFound))
	})
}

// TestConcurrentSuspendsCommitOnce stresses the CAS path: many concurrent
// identical suspend requests must produce exactly one version increment and
// all callers observe the suspended state.
func TestConcurrentSuspendsCommitOnce(t *testing.T) {
	mgr, _, _ := newHarness(t, false)
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateRunning)
	before, err := mgr.Get(ctx, "s")
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, suspendErr := mgr.Suspend(ctx, "s")
			errs <- suspendErr
		}()
	}
	wg.Wait()
	close(errs)
	for suspendErr := range errs {
		require.NoError(t, suspendErr)
	}

	after, err := mgr.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateSuspended, after.State)
	assert.Equal(t, before.Version+1, after.Version)
}
