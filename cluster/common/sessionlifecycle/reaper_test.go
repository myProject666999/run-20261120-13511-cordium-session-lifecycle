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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestReaper(mgr *Manager, hooks DestroyHooks, cfg *ReaperConfig) *Reaper {
	if cfg == nil {
		cfg = &ReaperConfig{
			BatchSize:     10,
			BatchPause:    time.Millisecond,
			RecoveryGrace: 5 * time.Second,
			MaxTick:       time.Second,
		}
	}
	return NewReaper(mgr, hooks, cfg)
}

func TestCreateTimeoutDrivesDestruction(t *testing.T) {
	mgr, clock, _ := newHarness(t, false)
	ctx := t.Context()
	_, err := mgr.CreateSession(ctx, "c", Policy{CreateTimeout: time.Minute})
	require.NoError(t, err)
	reaper := newTestReaper(mgr, NoopHooks{}, nil)

	result, err := reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Reaped)

	clock.Advance(time.Minute + time.Second)
	result, err = reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Reaped)
	assert.Equal(t, 1, result.Finalized)
	assert.Empty(t, result.Errors)

	final, err := mgr.Get(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, StateDestroyed, final.State)
}

func TestIdleTimeoutSuspendsByPolicy(t *testing.T) {
	mgr, clock, _ := newHarness(t, false)
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateRunning)
	reaper := newTestReaper(mgr, NoopHooks{}, nil)

	clock.Advance(time.Minute + time.Second)
	result, err := reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Reaped)
	assert.Equal(t, 0, result.Finalized)

	final, err := mgr.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateSuspended, final.State)
	require.NotNil(t, final.SuspendDeadline)
}

func TestIdleTimeoutDestroysByPolicy(t *testing.T) {
	clock := NewFakeClock()
	store := NewMemoryStore()
	policy := testPolicy()
	policy.IdleTimeoutAction = IdleDestroy
	mgr := NewManager(store, &Options{Clock: clock})
	ctx := t.Context()

	_, err := mgr.CreateSession(ctx, "s", policy)
	require.NoError(t, err)
	_, err = mgr.MarkRunning(ctx, "s")
	require.NoError(t, err)

	clock.Advance(time.Minute + time.Second)
	reaper := newTestReaper(mgr, NoopHooks{}, nil)
	result, err := reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Reaped)
	assert.Equal(t, 1, result.Finalized)

	final, err := mgr.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateDestroyed, final.State)
}

func TestSuspendedRetentionExpiryDestroys(t *testing.T) {
	mgr, clock, _ := newHarness(t, false)
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateSuspended)
	reaper := newTestReaper(mgr, NoopHooks{}, nil)

	clock.Advance(time.Minute + time.Second)
	result, err := reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Reaped)
	assert.Equal(t, 1, result.Finalized)

	final, err := mgr.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateDestroyed, final.State)
}

func TestMassExpiryIsProcessedInBatches(t *testing.T) {
	clock := NewFakeClock()
	store := NewMemoryStore()
	mgr := NewManager(store, &Options{Clock: clock})
	ctx := t.Context()

	const total = 25
	for i := range total {
		id := fmt.Sprintf("s-%02d", i)
		_, err := mgr.CreateSession(ctx, id, testPolicy())
		require.NoError(t, err)
		_, err = mgr.MarkRunning(ctx, id)
		require.NoError(t, err)
	}

	cfg := &ReaperConfig{
		BatchSize:     10,
		BatchPause:    time.Millisecond,
		RecoveryGrace: 5 * time.Second,
		MaxTick:       time.Second,
	}
	reaper := newTestReaper(mgr, NoopHooks{}, cfg)

	clock.Advance(time.Minute + time.Second)
	result, err := reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, total, result.Scanned)
	assert.Equal(t, total, result.Reaped)
	assert.Equal(t, 3, result.Batches, "25 sessions at batch size 10 span three batches")
	assert.Empty(t, result.Errors)

	sessions, err := store.List(ctx)
	require.NoError(t, err)
	for _, session := range sessions {
		assert.Equal(t, StateSuspended, session.State)
	}
}

// flakyHooks fails CloseConnections until the test flips it, and counts every
// step invocation so the test can prove the whole pipeline is retried and
// each step is idempotently re-invoked.
type flakyHooks struct {
	mu           sync.Mutex
	failClose    bool
	calls        map[string]int
	storageCalls int
}

func (h *flakyHooks) ReleaseResources(_ context.Context, id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls["release:"+id]++
	return nil
}

func (h *flakyHooks) CloseConnections(_ context.Context, id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls["close:"+id]++
	if h.failClose {
		return errors.New("simulated connection drain failure")
	}
	return nil
}

func (h *flakyHooks) CleanupStorage(_ context.Context, id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls["storage:"+id]++
	h.storageCalls++
	return nil
}

func (h *flakyHooks) count(key string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[key]
}

func TestDestroyPipelineFailureStaysDestroyingAndRetries(t *testing.T) {
	mgr, _, _ := newHarness(t, false)
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateDestroying)

	hooks := &flakyHooks{failClose: true, calls: map[string]int{}}
	reaper := newTestReaper(mgr, hooks, nil)

	done, err := reaper.RunDestroyPipeline(ctx, mustGet(t, mgr, "s"))
	require.Error(t, err)
	assert.False(t, done)

	var pipeErr *PipelineError
	require.True(t, errors.As(err, &pipeErr))
	assert.Equal(t, "close-connections", pipeErr.Step)

	stuck, err := mgr.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateDestroying, stuck.State, "a failed release step never rolls back to a live state")
	assert.Equal(t, 1, hooks.count("release:s"))
	assert.Equal(t, 1, hooks.count("close:s"))
	assert.Equal(t, 0, hooks.count("storage:s"), "later steps must not run after a failure")

	// The transient fault clears; the next scan retries the whole pipeline.
	hooks.mu.Lock()
	hooks.failClose = false
	hooks.mu.Unlock()

	result, err := reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Finalized)

	final, err := mgr.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateDestroyed, final.State)
	assert.Equal(t, 2, hooks.count("release:s"), "completed steps are idempotently re-invoked")
	assert.Equal(t, 2, hooks.count("close:s"))
	assert.Equal(t, 1, hooks.count("storage:s"))
}

func TestResumeRejectedAfterPartialDestruction(t *testing.T) {
	mgr, _, _ := newHarness(t, false)
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateDestroying)

	hooks := &flakyHooks{failClose: true, calls: map[string]int{}}
	reaper := newTestReaper(mgr, hooks, nil)
	_, err := reaper.RunDestroyPipeline(ctx, mustGet(t, mgr, "s"))
	require.Error(t, err)

	_, err = mgr.Resume(ctx, "s")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDestroyInProgress))
	assert.Contains(t, err.Error(), "destruction already started")

	// Suspend and activate are rejected for the same reason; the session
	// cannot leave destroying except through finalization.
	_, err = mgr.Suspend(ctx, "s")
	assert.True(t, errors.Is(err, ErrDestroyInProgress))
	_, err = mgr.MarkRunning(ctx, "s")
	assert.True(t, errors.Is(err, ErrDestroyInProgress))

	unchanged, err := mgr.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateDestroying, unchanged.State)
}

func TestReaperRestartRecomputesDeadlinesFromPersistedState(t *testing.T) {
	ctx := t.Context()

	// "Old process": arm a running session with an idle deadline.
	oldClock := NewFakeClock()
	oldStore := NewMemoryStore()
	oldMgr := NewManager(oldStore, &Options{Clock: oldClock})
	_, err := oldMgr.CreateSession(ctx, "s", testPolicy())
	require.NoError(t, err)
	_, err = oldMgr.MarkRunning(ctx, "s")
	require.NoError(t, err)
	oldClock.Advance(20 * time.Second) // 40s of idle budget remains in wall time

	exported, err := oldStore.ExportSessions(ctx)
	require.NoError(t, err)
	require.Len(t, exported, 1)

	// "New process": fresh monotonic domain (resets to zero), wall clock
	// aligned to where the old process stopped.
	newClock := NewFakeClock()
	newClock.SetWall(oldClock.Now())
	newStore := NewMemoryStore()
	require.NoError(t, newStore.ImportSessions(ctx, exported))
	newMgr := NewManager(newStore, &Options{Clock: newClock})
	reaper := newTestReaper(newMgr, NoopHooks{}, nil)

	// Before rehydration the stale monotonic basis from the previous process
	// is meaningless in this clock domain and must never be trusted as a
	// scheduling signal.
	staleWait, staleDue := reaper.NextWakeup(ctx)
	assert.False(t, staleDue)
	assert.NotEqual(t, 40*time.Second, staleWait,
		"the old-process mono value cannot produce the correct remaining time")

	// Rehydration rebuilds the basis from the persisted wall instant.
	require.NoError(t, newMgr.Rehydrate(ctx))
	rebuilt, err := newStore.Get(ctx, "s")
	require.NoError(t, err)
	require.NotNil(t, rebuilt.IdleDeadline)
	assert.True(t, rebuilt.IdleDeadline.Recovered)
	assert.Equal(t, newClock.Mono()+MonoTime(40*time.Second), rebuilt.IdleDeadline.Mono)

	wait, due := reaper.NextWakeup(ctx)
	assert.False(t, due)
	assert.Equal(t, 40*time.Second, wait)

	// Reaching the persisted instant is not enough for a recovered deadline:
	// the conservative grace applies.
	newClock.Advance(40 * time.Second)
	result, err := reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Reaped)

	newClock.Advance(5 * time.Second)
	result, err = reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Reaped)
	final, err := newStore.Get(ctx, "s")
	require.NoError(t, err)
	assert.Equal(t, StateSuspended, final.State)
}

func TestNextWakeupSchedulesStuckDestroyPromptly(t *testing.T) {
	mgr, _, _ := newHarness(t, false)
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateDestroying)
	reaper := newTestReaper(mgr, &flakyHooks{failClose: true, calls: map[string]int{}}, nil)

	wait, due := reaper.NextWakeup(ctx)
	assert.True(t, due)
	assert.Equal(t, time.Duration(0), wait)
}

func TestNextWakeupWithEmptyStore(t *testing.T) {
	mgr, _, _ := newHarness(t, false)
	reaper := newTestReaper(mgr, NoopHooks{}, nil)
	wait, due := reaper.NextWakeup(t.Context())
	assert.False(t, due)
	assert.Equal(t, time.Second, wait)
}

func TestReaperRunWakesOnControlledClock(t *testing.T) {
	mgr, clock, _ := newHarness(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, err := mgr.CreateSession(ctx, "c", Policy{CreateTimeout: time.Minute})
	require.NoError(t, err)
	reaper := newTestReaper(mgr, NoopHooks{}, nil)

	runErr := make(chan error, 1)
	go func() { runErr <- reaper.Run(ctx) }()

	clock.Advance(time.Minute + time.Second)
	require.Eventually(t, func() bool {
		session, getErr := mgr.Get(ctx, "c")
		return getErr == nil && session.State == StateDestroyed
	}, 2*time.Second, time.Millisecond)

	cancel()
	select {
	case err := <-runErr:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("reaper did not stop after context cancellation")
	}
}

func mustGet(t *testing.T, mgr *Manager, id string) *Session {
	t.Helper()
	session, err := mgr.Get(t.Context(), id)
	require.NoError(t, err)
	return session
}
