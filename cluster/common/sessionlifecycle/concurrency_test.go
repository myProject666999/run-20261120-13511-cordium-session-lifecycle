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

// TestEndToEndTimeoutsFollowMonotonicClockAcrossWallJumps proves through the
// full Manager+Reaper path that an aggressive wall-clock jump (NTP step)
// neither triggers a timeout early nor suppresses a genuinely elapsed one:
// in-process expiry is driven by the monotonic reading.
func TestEndToEndTimeoutsFollowMonotonicClockAcrossWallJumps(t *testing.T) {
	mgr, clock, _ := newHarness(t, false)
	ctx := t.Context()
	reaper := newTestReaper(mgr, NoopHooks{}, nil)

	_, err := mgr.CreateSession(ctx, "creating", Policy{CreateTimeout: time.Minute})
	require.NoError(t, err)
	sessionInState(t, mgr, "suspended", StateSuspended)

	// A huge forward wall jump must not reap anything, because the
	// monotonic clock has not moved.
	clock.JumpWall(24 * time.Hour)
	result, err := reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Reaped)

	creating, err := mgr.Get(ctx, "creating")
	require.NoError(t, err)
	assert.Equal(t, StateCreating, creating.State)
	suspended, err := mgr.Get(ctx, "suspended")
	require.NoError(t, err)
	assert.Equal(t, StateSuspended, suspended.State)

	// A backward wall jump must not delay the genuinely elapsed timeouts
	// either: advancing the monotonic clock by the real budget reaps both.
	clock.JumpWall(-48 * time.Hour)
	clock.Advance(time.Minute + time.Second)
	result, err = reaper.ScanOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Reaped)

	creating, err = mgr.Get(ctx, "creating")
	require.NoError(t, err)
	assert.Equal(t, StateDestroyed, creating.State)
	suspended, err = mgr.Get(ctx, "suspended")
	require.NoError(t, err)
	assert.Equal(t, StateDestroyed, suspended.State)
}

// TestActivityAndReapConcurrentInterleaving stress-tests the single
// compare-and-update primitive shared by activity recording and idle
// reclamation. Many goroutines of both kinds race on the same sessions. The
// hard, deterministic proof that a stale reaping decision is rejected after a
// renewal lives in TestActivityRenewalInvalidatesStaleReapDecision (which
// forces the exact interleaving); here the assertion is the concurrency
// invariant: under arbitrary interleaving every final record is internally
// consistent — renewed sessions carry a fresh, not-due idle timer, and
// suspended sessions carry only their retention timer. Version conflicts may
// or may not occur depending on scheduling, but they must never corrupt the
// outcome.
func TestActivityAndReapConcurrentInterleaving(t *testing.T) {
	clock := NewFakeClock()
	store := NewMemoryStore()
	mgr := NewManager(store, &Options{Clock: clock})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const sessions = 20
	for i := range sessions {
		id := "s-" + itoa(i)
		_, err := mgr.CreateSession(ctx, id, testPolicy())
		require.NoError(t, err)
		_, err = mgr.MarkRunning(ctx, id)
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	var conflicts, renewals atomic.Int64
	stop := make(chan struct{})

	// Activity writers keep renewing random sessions.
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := "s-" + itoa(int(fastRand())%sessions)
				if _, err := mgr.RecordActivity(ctx, id); err == nil {
					renewals.Add(1)
				} else if errors.Is(err, ErrInvalidTransition) || errors.Is(err, ErrDestroyInProgress) {
					// Reaped concurrently: an expected outcome.
				}
			}
		}()
	}

	// Reaper goroutines repeatedly list and attempt to reap due sessions.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snapshots, _ := store.List(ctx)
				for _, snapshot := range snapshots {
					if _, err := mgr.ReapDue(ctx, snapshot, 0); err != nil {
						if errors.Is(err, ErrVersionConflict) {
							conflicts.Add(1)
						}
					}
				}
			}
		}()
	}

	// Drive the clock across the idle boundary in small steps so renewals and
	// reaps interleave many times.
	for range 40 {
		clock.Advance(5 * time.Second)
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	// Renewal traffic definitely ran. Conflict counts are reported for
	// visibility but not asserted, since their occurrence is scheduling-
	// dependent; correctness must hold either way.
	assert.Greater(t, renewals.Load(), int64(0))
	t.Logf("interleaving completed with %d version conflicts handled", conflicts.Load())

	// Final invariant: every record is internally consistent regardless of
	// which goroutine won each race. A renewed session never carries a due
	// (stale) idle timer, and a suspended one never retains an idle timer.
	final, err := store.List(ctx)
	require.NoError(t, err)
	states := map[State]int{}
	for _, session := range final {
		states[session.State]++
		switch session.State {
		case StateRunning:
			require.NotNil(t, session.IdleDeadline)
			assert.False(t, session.IdleDeadline.Due(clock, 0),
				"renewed session %s must have a reset, not-due timer", session.ID)
		case StateSuspended:
			assert.Nil(t, session.IdleDeadline)
			require.NotNil(t, session.SuspendDeadline)
		case StateDestroying, StateDestroyed:
			assert.Nil(t, session.IdleDeadline)
		}
	}
	t.Logf("final state distribution: %+v", states)
}

// itoa is a tiny allocation-free int-to-string helper so the stress test does
// not depend on strconv for session naming.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// fastRand is a minimal mutex-protected xorshift used only inside the
// deterministic stress test, avoiding global math/rand seeding concerns.
var (
	randMu    sync.Mutex
	randState uint64 = 0x9e3779b97f4a7c15
)

func fastRand() uint64 {
	randMu.Lock()
	defer randMu.Unlock()
	randState ^= randState << 13
	randState ^= randState >> 7
	randState ^= randState << 17
	return randState
}
