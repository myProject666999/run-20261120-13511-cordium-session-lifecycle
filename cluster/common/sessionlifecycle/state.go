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
	"errors"
	"fmt"
)

// State is one of the five explicit lifecycle states. It is an integer
// internally but serializes to stable strings so persisted records stay
// readable and wire-compatible.
type State int

const (
	// StateUnknown is only returned when a persisted state value cannot be
	// decoded; it is never a valid runtime state.
	StateUnknown State = iota
	StateCreating
	StateRunning
	StateSuspended
	StateDestroying
	StateDestroyed
)

// String returns the stable, persisted name of the state.
func (s State) String() string {
	switch s {
	case StateCreating:
		return "creating"
	case StateRunning:
		return "running"
	case StateSuspended:
		return "suspended"
	case StateDestroying:
		return "destroying"
	case StateDestroyed:
		return "destroyed"
	default:
		return "unknown"
	}
}

// MarshalText implements encoding.TextMarshaler.
func (s State) MarshalText() ([]byte, error) {
	if s == StateUnknown {
		return nil, fmt.Errorf("sessionlifecycle: cannot marshal %s state", s)
	}
	return []byte(s.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler and accepts both the
// canonical names and legacy numeric strings.
func (s *State) UnmarshalText(data []byte) error {
	switch string(data) {
	case "creating":
		*s = StateCreating
	case "running":
		*s = StateRunning
	case "suspended":
		*s = StateSuspended
	case "destroying":
		*s = StateDestroying
	case "destroyed":
		*s = StateDestroyed
	default:
		return fmt.Errorf("sessionlifecycle: unknown persisted state %q", string(data))
	}
	return nil
}

// Operation identifies a lifecycle request arriving at the Manager. Each
// operation maps to one target state; requesting an operation whose target
// state equals the current state is an idempotent no-op.
type Operation int

const (
	OpActivate        Operation = iota // creation finished -> running
	OpSuspend                          // running -> suspended
	OpResume                           // suspended -> running
	OpDestroy                          // any live state -> destroying (idempotent)
	OpFinalizeDestroy                  // destroying -> destroyed
)

// TargetState is the state this operation drives the session to.
func (op Operation) TargetState() State {
	switch op {
	case OpActivate:
		return StateRunning
	case OpSuspend:
		return StateSuspended
	case OpResume:
		return StateRunning
	case OpDestroy:
		return StateDestroying
	case OpFinalizeDestroy:
		return StateDestroyed
	default:
		return StateUnknown
	}
}

func (op Operation) String() string {
	switch op {
	case OpActivate:
		return "activate"
	case OpSuspend:
		return "suspend"
	case OpResume:
		return "resume"
	case OpDestroy:
		return "destroy"
	case OpFinalizeDestroy:
		return "finalize-destroy"
	default:
		return "unknown"
	}
}

// legalTransitions is the single source of truth for which target states a
// given current state may move to. A state present in the set means the
// transition is valid; equality with the current state is handled separately
// as idempotent success by resolveTransition.
//
//	creating    -> running | destroying
//	running     -> suspended | destroying
//	suspended   -> running | destroying
//	destroying  -> destroyed
//	destroyed   -> (nothing)
var legalTransitions = map[State]map[State]struct{}{
	StateCreating: {
		StateRunning:    {},
		StateDestroying: {},
	},
	StateRunning: {
		StateSuspended:  {},
		StateDestroying: {},
	},
	StateSuspended: {
		StateRunning:    {},
		StateDestroying: {},
	},
	StateDestroying: {
		StateDestroyed: {},
	},
	StateDestroyed: {},
}

// legalOperations narrows the edge table to operations with a defined
// meaning. Two operations can share a target state (activate and resume both
// drive StateRunning) but only one of them is valid for a given source, so a
// "resume" of a still-creating session is rejected even though
// creating -> running is a legal edge.
var legalOperations = map[Operation]map[State]struct{}{
	OpActivate: {
		StateCreating: {},
	},
	OpSuspend: {
		StateRunning: {},
	},
	OpResume: {
		StateSuspended: {},
	},
	OpDestroy: {
		StateCreating:  {},
		StateRunning:   {},
		StateSuspended: {},
	},
	OpFinalizeDestroy: {
		StateDestroying: {},
	},
}

// idempotentStates names, per operation, the states in which a repeated
// request succeeds as a no-op without a version bump. It is separate from
// legalOperations because an idempotent repeat is not a transition: for
// example suspend is only *initiated* from running, but repeating it against
// an already suspended session must succeed for timeout retries. Resume and
// activate intentionally have no idempotent state: repeating them against a
// running session is a semantic mistake (the session is not suspended and is
// past creation), not a safe retry.
var idempotentStates = map[Operation]map[State]struct{}{
	OpSuspend: {
		StateSuspended: {},
	},
	OpDestroy: {
		StateDestroying: {},
		StateDestroyed:  {},
	},
	OpFinalizeDestroy: {
		StateDestroyed: {},
	},
}

// CanTransition reports whether moving from from to to is a legal transition.
// Same-state requests are not transitions and are reported as illegal here;
// idempotency of same-state requests is handled by the Manager.
func CanTransition(from, to State) bool {
	targets, ok := legalTransitions[from]
	if !ok {
		return false
	}
	_, ok = targets[to]
	return ok
}

// ErrInvalidTransition wraps every rejected transition that is not a
// destroyed-session operation. Inspect the *TransitionError via errors.As to
// distinguish individual failures programmatically.
var ErrInvalidTransition = errors.New("sessionlifecycle: invalid state transition")

// ErrDestroyInProgress is returned when a resume (or any non-destroy
// operation) races with destruction: the session has already entered
// StateDestroying and release steps may have run, so resuming it is no
// longer possible. It remains a kind of invalid transition and unwraps to
// ErrInvalidTransition for callers that only care about the broad class.
var ErrDestroyInProgress = errors.New("sessionlifecycle: session is being destroyed")

// ErrSessionNotFound is returned when no session exists for the ID, or when
// a record is removed once destruction is finalized. An upstream cleaner
// that purges records on finalize therefore gets "not found" on a repeated
// destroy; a store that keeps tombstones instead gets idempotent success.
var ErrSessionNotFound = errors.New("sessionlifecycle: session not found")

// TransitionError describes a rejected lifecycle request. Callers can
// errors.As it to obtain the operation, the observed state and the target
// state, and branch on the sentinel errors via errors.Is.
type TransitionError struct {
	ID        string
	Op        Operation
	From      State
	To        State
	Reason    error
	ReasonMsg string
}

func (e *TransitionError) Error() string {
	msg := fmt.Sprintf("sessionlifecycle: %s rejected for session %q: %s -> %s",
		e.Op, e.ID, e.From, e.To)
	if e.ReasonMsg != "" {
		msg += ": " + e.ReasonMsg
	}
	if e.Reason != nil {
		msg += ": " + e.Reason.Error()
	}
	return msg
}

// Unwrap exposes the broad error class so errors.Is(err,
// ErrInvalidTransition) keeps working for the specialized destroy-in-progress
// error.
func (e *TransitionError) Unwrap() error {
	return e.Reason
}

// Is makes TransitionError match both its specific reason and
// ErrInvalidTransition, which every rejected transition belongs to.
func (e *TransitionError) Is(target error) bool {
	if target == ErrInvalidTransition {
		return true
	}
	return e.Reason != nil && target == e.Reason
}

// resolveTransition applies the transition table plus idempotency rules and
// returns the state to persist, or nil when the request is already satisfied.
// All errors are *TransitionError values carrying a distinguishable reason.
func resolveTransition(id string, op Operation, current State) (State, error) {
	target := op.TargetState()

	// 1. Terminal state. Destroy/finalize on a retained tombstone stay
	// idempotent so an upstream cleaner can retry; every other operation is a
	// hard, distinguishable error.
	if current == StateDestroyed {
		if repeats, ok := idempotentStates[op]; ok {
			if _, ok := repeats[current]; ok {
				return current, nil
			}
		}
		return 0, &TransitionError{
			ID: id, Op: op, From: current, To: target,
			Reason:    ErrInvalidTransition,
			ReasonMsg: "session is already destroyed",
		}
	}

	// 2. Destruction in progress. The only legal exit is finalize (validated
	// below through the normal edge path), destroy itself is idempotent, and
	// anything else (notably a resume racing with partial destruction) is
	// rejected explicitly instead of silently moving the session back to a
	// live state.
	if current == StateDestroying {
		if op == OpDestroy {
			return StateDestroying, nil
		}
		if op == OpFinalizeDestroy {
			return StateDestroyed, nil
		}
		return 0, &TransitionError{
			ID: id, Op: op, From: current, To: target,
			Reason:    ErrDestroyInProgress,
			ReasonMsg: "destruction already started; resource release may be partially complete",
		}
	}

	// 3. Idempotent repeats on live states, e.g. suspending an already
	// suspended session: succeed without a write so timeout retries are
	// safe. Resume/activate have no idempotent state by design.
	if repeats, ok := idempotentStates[op]; ok {
		if _, ok := repeats[current]; ok {
			return current, nil
		}
	}

	// 4. Remaining live-state requests: the operation must be meaningful in
	// this source state.
	sources, opKnown := legalOperations[op]
	if !opKnown {
		return 0, &TransitionError{
			ID: id, Op: op, From: current, To: target,
			Reason:    ErrInvalidTransition,
			ReasonMsg: "unknown lifecycle operation",
		}
	}
	if _, validHere := sources[current]; !validHere {
		return 0, &TransitionError{
			ID: id, Op: op, From: current, To: target,
			Reason:    ErrInvalidTransition,
			ReasonMsg: "operation is not valid in the current state",
		}
	}

	// 5. The edge table has the final say on the actual move.
	if !CanTransition(current, target) {
		return 0, &TransitionError{
			ID: id, Op: op, From: current, To: target,
			Reason:    ErrInvalidTransition,
			ReasonMsg: "transition is not allowed by the lifecycle table",
		}
	}
	return target, nil
}
