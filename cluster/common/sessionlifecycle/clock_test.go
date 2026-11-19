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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeClockAdvancesBothReadings(t *testing.T) {
	clock := NewFakeClock()
	wall0, mono0 := clock.Now(), clock.Mono()
	clock.Advance(3 * time.Second)
	assert.Equal(t, mono0+MonoTime(3*time.Second), clock.Mono())
	assert.True(t, clock.Now().Equal(wall0.Add(3*time.Second)))
}

func TestSystemClockMonotonic(t *testing.T) {
	clock := NewSystemClock()
	wallBefore := clock.Now()
	monoBefore := clock.Mono()

	time.Sleep(2 * time.Millisecond)

	// Monotonic and wall readings both advance; the monotonic delta is
	// positive and immune to any wall adjustment during the sleep.
	assert.Greater(t, int64(clock.Mono()-monoBefore), int64(0))
	assert.False(t, clock.Now().Before(wallBefore))
}

func TestFakeClockSetWallLeavesMonotonicUntouched(t *testing.T) {
	clock := NewFakeClock()
	mono := clock.Mono()
	clock.SetWall(time.Date(2030, 12, 31, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, mono, clock.Mono())
	assert.Equal(t, 2030, clock.Now().Year())
}

func TestWallJumpDoesNotMoveMonotonicClock(t *testing.T) {
	clock := NewFakeClock()
	monoBefore := clock.Mono()
	wallBefore := clock.Now()

	clock.JumpWall(2 * time.Hour)

	assert.Equal(t, monoBefore, clock.Mono(), "monotonic reading must ignore wall jumps")
	assert.True(t, clock.Now().Equal(wallBefore.Add(2*time.Hour)))

	clock.JumpWall(-90 * time.Minute)
	assert.Equal(t, monoBefore, clock.Mono())
}

// TestMonotonicDeadlineIgnoresWallJump proves in-process expiry decisions
// follow the monotonic clock: a wall jump cannot make a deadline due early
// or delay it.
func TestMonotonicDeadlineIgnoresWallJump(t *testing.T) {
	clock := NewFakeClock()
	dl := NewDeadline(clock, 10*time.Second)

	clock.JumpWall(time.Hour)
	assert.False(t, dl.DueMono(clock), "a wall jump must not trigger the deadline")
	assert.False(t, dl.Due(clock, time.Minute))

	clock.Advance(9 * time.Second)
	assert.False(t, dl.Due(clock, 0))

	clock.Advance(time.Second)
	assert.True(t, dl.DueMono(clock))
	assert.True(t, dl.Due(clock, 0))
}

// TestFreshDeadlineIgnoresRecoveryGrace ensures a deadline created inside the
// current process fires at its exact instant, without the grace that only
// applies to recovered deadlines.
func TestFreshDeadlineIgnoresRecoveryGrace(t *testing.T) {
	clock := NewFakeClock()
	dl := NewDeadline(clock, 10*time.Second)
	require.False(t, dl.Recovered)

	clock.Advance(10 * time.Second)
	assert.True(t, dl.Due(clock, time.Hour),
		"fresh deadlines use zero grace regardless of the configured grace")
}

// TestRecoveredDeadlineIsConservative drives a restart scenario. The new
// instance rebuilds the monotonic basis from the persisted wall instant and
// only reaps after wall deadline + grace, so an ahead-of-writer clock cannot
// destroy early.
func TestRecoveredDeadlineIsConservative(t *testing.T) {
	grace := 5 * time.Second

	t.Run("ahead clock waits for grace past the persisted instant", func(t *testing.T) {
		oldClock := NewFakeClock()
		dl := NewDeadline(oldClock, 10*time.Second)

		newClock := NewFakeClock()
		newClock.SetWall(oldClock.Now().Add(10 * time.Second)) // locally "at" the deadline
		rebuilt := dl.Rehydrate(newClock)
		require.True(t, rebuilt.Recovered)

		assert.False(t, rebuilt.Due(newClock, grace),
			"at the persisted instant a recovered deadline must not yet be due")

		newClock.Advance(4 * time.Second)
		assert.False(t, rebuilt.Due(newClock, grace))

		newClock.Advance(time.Second)
		assert.True(t, rebuilt.Due(newClock, grace))
	})

	t.Run("behind clock simply reaps late", func(t *testing.T) {
		oldClock := NewFakeClock()
		dl := NewDeadline(oldClock, 10*time.Second)

		newClock := NewFakeClock()
		newClock.SetWall(oldClock.Now()) // starts ten seconds before expiry
		rebuilt := dl.Rehydrate(newClock)

		assert.False(t, rebuilt.Due(newClock, grace))
		newClock.Advance(10 * time.Second) // now at the persisted instant
		assert.False(t, rebuilt.Due(newClock, grace))
		newClock.Advance(5 * time.Second) // grace elapsed
		assert.True(t, rebuilt.Due(newClock, grace))
	})

	t.Run("already expired deadline is due after one grace interval", func(t *testing.T) {
		oldClock := NewFakeClock()
		dl := NewDeadline(oldClock, time.Second)

		newClock := NewFakeClock()
		newClock.SetWall(oldClock.Now().Add(time.Hour))
		rebuilt := dl.Rehydrate(newClock)
		// Mono anchoring lands in the past; only the wall arm still gates.
		assert.True(t, rebuilt.DueMono(newClock))
		assert.True(t, rebuilt.Due(newClock, grace))
	})
}
