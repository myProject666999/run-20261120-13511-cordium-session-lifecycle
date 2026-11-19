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
	"sync"
	"time"
)

// MonoTime is a monotonic reading in nanoseconds, comparable only with other
// readings taken from the same Clock instance / process. It is never
// persisted: monotonic values have no meaning after a restart.
type MonoTime int64

// Clock is the injectable time source. Now returns a wall-clock instant
// (used for persistence and cross-process deadlines) while Mono returns the
// monotonic reading used for every in-process elapsed-time decision.
//
// Keeping the two readings separate mirrors Go's own time.Time semantics,
// where the monotonic component is deliberately stripped on
// marshal/unmarshal.
type Clock interface {
	Now() time.Time
	Mono() MonoTime
}

// SystemClock is the production Clock. Mono is derived from time.Now(), whose
// monotonic reading is immune to wall-clock adjustments.
type SystemClock struct{}

// NewSystemClock returns the default wall/monotonic clock.
func NewSystemClock() SystemClock { return SystemClock{} }

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// Mono returns the current monotonic reading in nanoseconds.
func (SystemClock) Mono() MonoTime {
	// time.Time.UnixNano includes the monotonic reading when the value was
	// obtained from time.Now(); subtracting two such values yields monotonic
	// elapsed time even across wall-clock jumps.
	now := time.Now()
	return MonoTime(now.UnixNano())
}

// FakeClock is a fully controllable clock for tests. It advances both a wall
// clock and an independent monotonic clock together by default, but allows
// wall-only jumps (simulating NTP adjustments) so tests can prove that
// in-process deadlines follow the monotonic clock.
type FakeClock struct {
	mu       sync.Mutex
	wall     time.Time
	mono     MonoTime
	notifyCh chan struct{}
}

// NewFakeClock starts at a fixed wall instant with a monotonic reading of
// zero.
func NewFakeClock() *FakeClock {
	return &FakeClock{
		wall: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// Now returns the controlled wall-clock time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wall
}

// Mono returns the controlled monotonic reading.
func (c *FakeClock) Mono() MonoTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

// Advance moves both the wall and monotonic clocks by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.wall = c.wall.Add(d)
	c.mono += MonoTime(d)
	ch := c.notifyCh
	c.mu.Unlock()
	if ch != nil {
		// Non-blocking; the watcher uses a buffered channel and simply
		// re-evaluates the full wait.
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// JumpWall moves only the wall clock, modeling an NTP step or a skewed
// neighbor instance. Monotonic deadlines must be unaffected.
func (c *FakeClock) JumpWall(d time.Duration) {
	c.mu.Lock()
	c.wall = c.wall.Add(d)
	ch := c.notifyCh
	c.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// SetWall overwrites the wall clock without touching the monotonic reading.
func (c *FakeClock) SetWall(t time.Time) {
	c.mu.Lock()
	c.wall = t
	c.mu.Unlock()
}

// onChange returns a channel that receives one element whenever the clock is
// adjusted. It exists for the reaper's interruptible wait loop.
func (c *FakeClock) onChange() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.notifyCh == nil {
		c.notifyCh = make(chan struct{}, 1)
	}
	return c.notifyCh
}

// Deadline is the recoverable expiry basis of a timed state.
//
// Wall is the absolute wall-clock instant of expiry and is safe to persist.
// Mono is the monotonic instant used while the creating process stays alive;
// it is transient and must be rebuilt from Wall after a restart (see
// Rehydrate). Both bases are always written together. Recovered marks
// deadlines whose monotonic basis was rebuilt in a different process: only
// those take the conservative recovery grace on the wall-clock arm, while
// deadlines created by the current process expire at the exact instant.
type Deadline struct {
	Wall      time.Time `json:"wall"`
	Mono      MonoTime  `json:"mono"`
	Recovered bool      `json:"recovered,omitempty"`
}

// NewDeadline computes a deadline d from now/mono.
func NewDeadline(clock Clock, d time.Duration) Deadline {
	return Deadline{
		Wall: clock.Now().Add(d),
		Mono: clock.Mono() + MonoTime(d),
	}
}

// DueMono reports whether the deadline has elapsed according to the
// monotonic clock. It is the authoritative check within a process.
func (dl Deadline) DueMono(clock Clock) bool {
	return clock.Mono() >= dl.Mono
}

// Due is the due test used by the reaper.
//
// For a deadline created inside this process (Recovered == false) the
// monotonic reading is the sole authority, exactly as the time package
// treats time.Time subtraction: wall-clock adjustments, forward or backward,
// can neither trigger nor suppress an in-process timeout.
//
// For a deadline rebuilt after a restart (Recovered == true) the wall arm is
// required as well and widened by recoveryGrace, because the persisted wall
// instant is the only comparable basis across two monotonic clock domains.
// A skewed new instance therefore never reaps early; it can only reap late.
func (dl Deadline) Due(clock Clock, recoveryGrace time.Duration) bool {
	if !dl.DueMono(clock) {
		return false
	}
	if !dl.Recovered {
		return true
	}
	return dl.DueWall(clock, recoveryGrace)
}

// DueWall reports whether the deadline has elapsed according to the wall
// clock, conservatively widened by grace: a recovered deadline is only due
// once the local wall clock is grace past the persisted expiry instant.
//
// The asymmetry is deliberate and makes early destruction impossible:
//
//   - if the new instance's clock is behind the writer's clock, the deadline
//     simply lies in the future and reaping happens late;
//   - if it is ahead, reaping still waits until the local clock passes the
//     persisted instant plus grace, which is even later in objective time;
//   - a wall jump inside a single process is irrelevant because the
//     monotonic reading remains the authoritative check;
//   - multiple instances racing on the same session still serialize through
//     the version-based compare-and-update, so at most one reaping decision
//     commits and any decision based on a stale record is rejected.
func (dl Deadline) DueWall(clock Clock, grace time.Duration) bool {
	return !clock.Now().Before(dl.Wall.Add(grace))
}

// Rehydrate rebuilds the monotonic basis after a process restart: the
// persisted wall instant remains the expiry reference and the new process's
// monotonic clock is anchored at "now" read from the injected clock (never
// the global time package, which would defeat test control). If the wall
// deadline is already in the past relative to the new clock, the rebuilt
// monotonic deadline is anchored in the past as well so the very next scan
// reaps the session (subject to DueWall's recovery grace). The result is
// marked Recovered so due checks treat it conservatively.
func (dl Deadline) Rehydrate(clock Clock) Deadline {
	remaining := dl.Wall.Sub(clock.Now())
	return Deadline{
		Wall:      dl.Wall,
		Mono:      clock.Mono() + MonoTime(remaining),
		Recovered: true,
	}
}
