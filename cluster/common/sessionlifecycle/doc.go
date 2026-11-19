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

// Package sessionlifecycle implements the sandbox session lifecycle as an
// explicit five-state machine instead of a set of independent booleans.
//
// # State model
//
// A session is always in exactly one of five states:
//
//   - StateCreating: the session is being provisioned and has a creation
//     deadline. On deadline it moves to StateDestroying.
//   - StateRunning: usable; an idle deadline is maintained from the latest
//     recorded activity. On idle timeout the session policy decides whether
//     the session is suspended or destroyed.
//   - StateSuspended: frozen; carries a maximum retention deadline after
//     which it is destroyed.
//   - StateDestroying: resource release is in progress. The session cannot
//     leave this state except for StateDestroyed; failed release steps keep
//     the session here so the whole destroy pipeline can be retried.
//   - StateDestroyed: terminal; no further transition is possible.
//
// Every state change goes through the single Manager transition entry point,
// which validates the transition against legalTransitions. Illegal requests
// fail with a distinguishable *TransitionError (unwrapping
// ErrInvalidTransition, or ErrDestroyInProgress for a resume that races with
// destruction); the stored state is never silently rewritten. Repeating an
// already-completed transition is idempotent, so callers can safely retry on
// timeout.
//
// # Clocks and deadlines
//
// Clock is injectable. In-process elapsed time is measured exclusively with
// the monotonic reading (see MonoTime): adjustments of the wall clock cannot
// trigger premature or delayed reaping while a process stays alive. A
// Deadline additionally persists the absolute wall-clock instant it is based
// on, because monotonic readings are meaningless after a process restart.
//
// On recovery a new process rebuilds each monotonic deadline from the
// persisted wall-clock instant. Because wall clocks can jump and two
// instances' clocks can disagree, wall-based due decisions are conservative:
//
//   - a recovered deadline counts as due only once the local wall clock is a
//     configurable RecoveryGrace past the persisted instant, absorbing small
//     skews instead of destroying early;
//   - if the new instance's clock is behind, the deadline simply lands in the
//     future and reaping happens late, never early;
//   - within a single process a backward or forward wall jump is irrelevant,
//     because the monotonic reading remains authoritative;
//   - multiple instances racing on the same session still serialize through
//     the version-based compare-and-update, so at most one reaping decision
//     commits and any decision based on a stale record is rejected.
//
// # Activity versus idle reclamation
//
// Recording activity and committing an idle-reclamation decision use the same
// atomic compare-and-update primitive. A reaper may only mark a running
// session for reclamation if the record still carries the version and the
// still-due idle deadline on which its decision was based. If an activity
// update commits first, the version changes and the idle timer is reset, the
// stale reclamation decision fails and is re-evaluated; a renewed session is
// never reaped against an old decision.
//
// # Legacy data
//
// Sessions persisted with only booleans and timestamps are mapped once
// (MigrateAll / MigrateLegacy) or lazily on first access (LazyLegacyStore).
// Contradictory boolean combinations resolve by precedence
// (destroyed > suspended > running > created) and are reported as
// MigrationAmbiguityError instead of being hidden.
package sessionlifecycle
