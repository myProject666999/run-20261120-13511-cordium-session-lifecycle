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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPolicy() Policy {
	return Policy{
		CreateTimeout:        time.Minute,
		IdleTimeout:          time.Minute,
		IdleTimeoutAction:    IdleSuspend,
		SuspendedMaxLifetime: time.Minute,
	}
}

func newHarness(t *testing.T, purge bool) (*Manager, *FakeClock, *MemoryStore) {
	t.Helper()
	clock := NewFakeClock()
	store := NewMemoryStore()
	mgr := NewManager(store, &Options{Clock: clock, PurgeOnFinalize: purge})
	return mgr, clock, store
}

// sessionInState drives a fresh session through real manager transitions up
// to the requested state, so tests never hand-craft invalid records.
func sessionInState(t *testing.T, mgr *Manager, id string, state State) *Session {
	t.Helper()
	ctx := t.Context()
	session, err := mgr.CreateSession(ctx, id, testPolicy())
	require.NoError(t, err)
	if state == StateCreating {
		return session
	}
	session, err = mgr.MarkRunning(ctx, id)
	require.NoError(t, err)
	if state == StateRunning {
		return session
	}
	session, err = mgr.Suspend(ctx, id)
	require.NoError(t, err)
	if state == StateSuspended {
		return session
	}
	session, err = mgr.Destroy(ctx, id)
	require.NoError(t, err)
	if state == StateDestroying {
		return session
	}
	session, err = mgr.FinalizeDestroy(ctx, id)
	require.NoError(t, err)
	require.Equal(t, StateDestroyed, session.State)
	return session
}

// TestTransitionTableExhaustive walks the full 5 states x 5 operations
// matrix and pins down legality, the resulting state, idempotency (no
// version bump) and the distinguishable error class of every cell.
func TestTransitionTableExhaustive(t *testing.T) {
	mgr, _, _ := newHarness(t, false)
	ctx := context.Background()

	type cell struct {
		state      State
		wantState  State
		wantErr    error // nil means success
		idempotent bool  // success without a version increment
	}

	states := []State{StateCreating, StateRunning, StateSuspended, StateDestroying, StateDestroyed}
	ops := []Operation{OpActivate, OpSuspend, OpResume, OpDestroy, OpFinalizeDestroy}

	matrix := map[State]map[Operation]cell{
		StateCreating: {
			OpActivate:        {StateCreating, StateRunning, nil, false},
			OpSuspend:         {StateCreating, StateCreating, ErrInvalidTransition, false},
			OpResume:          {StateCreating, StateCreating, ErrInvalidTransition, false},
			OpDestroy:         {StateCreating, StateDestroying, nil, false},
			OpFinalizeDestroy: {StateCreating, StateCreating, ErrInvalidTransition, false},
		},
		StateRunning: {
			OpActivate:        {StateRunning, StateRunning, ErrInvalidTransition, false},
			OpSuspend:         {StateRunning, StateSuspended, nil, false},
			OpResume:          {StateRunning, StateRunning, ErrInvalidTransition, false},
			OpDestroy:         {StateRunning, StateDestroying, nil, false},
			OpFinalizeDestroy: {StateRunning, StateRunning, ErrInvalidTransition, false},
		},
		StateSuspended: {
			OpActivate:        {StateSuspended, StateSuspended, ErrInvalidTransition, false},
			OpSuspend:         {StateSuspended, StateSuspended, nil, true},
			OpResume:          {StateSuspended, StateRunning, nil, false},
			OpDestroy:         {StateSuspended, StateDestroying, nil, false},
			OpFinalizeDestroy: {StateSuspended, StateSuspended, ErrInvalidTransition, false},
		},
		StateDestroying: {
			OpActivate:        {StateDestroying, StateDestroying, ErrDestroyInProgress, false},
			OpSuspend:         {StateDestroying, StateDestroying, ErrDestroyInProgress, false},
			OpResume:          {StateDestroying, StateDestroying, ErrDestroyInProgress, false},
			OpDestroy:         {StateDestroying, StateDestroying, nil, true},
			OpFinalizeDestroy: {StateDestroying, StateDestroyed, nil, false},
		},
		StateDestroyed: {
			OpActivate:        {StateDestroyed, StateDestroyed, ErrInvalidTransition, false},
			OpSuspend:         {StateDestroyed, StateDestroyed, ErrInvalidTransition, false},
			OpResume:          {StateDestroyed, StateDestroyed, ErrInvalidTransition, false},
			OpDestroy:         {StateDestroyed, StateDestroyed, nil, true},
			OpFinalizeDestroy: {StateDestroyed, StateDestroyed, nil, true},
		},
	}

	dispatch := map[Operation]func(context.Context, string) (*Session, error){
		OpActivate:        mgr.MarkRunning,
		OpSuspend:         mgr.Suspend,
		OpResume:          mgr.Resume,
		OpDestroy:         mgr.Destroy,
		OpFinalizeDestroy: mgr.FinalizeDestroy,
	}

	for si, from := range states {
		for oi, op := range ops {
			expected := matrix[from][op]
			name := from.String() + "/" + op.String()
			t.Run(name, func(t *testing.T) {
				id := "s-" + name
				before := sessionInState(t, mgr, id, from)
				after, err := dispatch[op](ctx, id)

				if expected.wantErr != nil {
					require.Error(t, err, "cell must fail")
					assert.True(t, errors.Is(err, expected.wantErr),
						"error %v must wrap %v", err, expected.wantErr)

					var te *TransitionError
					require.True(t, errors.As(err, &te))
					assert.Equal(t, op, te.Op)
					assert.Equal(t, from, te.From)

					// Rejected requests must never rewrite state or version.
					unchanged, getErr := mgr.Get(ctx, id)
					require.NoError(t, getErr)
					assert.Equal(t, before.State, unchanged.State)
					assert.Equal(t, before.Version, unchanged.Version)
					return
				}

				require.NoError(t, err)
				require.NotNil(t, after)
				assert.Equal(t, expected.wantState, after.State)

				if expected.idempotent {
					assert.Equal(t, before.Version, after.Version,
						"idempotent request must not bump the version")
				} else {
					assert.Equal(t, before.Version+1, after.Version)
				}
			})
			_ = si
			_ = oi
		}
	}
}

// TestDestroyInProgressIsAlsoInvalidTransition documents the error
// hierarchy: the specialized sentinel still belongs to the broad class.
func TestDestroyInProgressIsAlsoInvalidTransition(t *testing.T) {
	mgr, _, _ := newHarness(t, false)
	ctx := t.Context()
	sessionInState(t, mgr, "s", StateDestroying)

	_, err := mgr.Resume(ctx, "s")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDestroyInProgress))
	assert.True(t, errors.Is(err, ErrInvalidTransition))
}

// TestStateJSONRoundTrip pins the persisted state names.
func TestStateJSONRoundTrip(t *testing.T) {
	for _, state := range []State{StateCreating, StateRunning, StateSuspended, StateDestroying, StateDestroyed} {
		data, err := json.Marshal(state)
		require.NoError(t, err)
		var decoded State
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, state, decoded)
	}
	var unknown State
	require.Error(t, json.Unmarshal([]byte(`"bogus"`), &unknown))
}

// TestCanTransitionEdges checks the raw edge table independently of the
// operation semantics table.
func TestCanTransitionEdges(t *testing.T) {
	legal := []struct{ from, to State }{
		{StateCreating, StateRunning},
		{StateCreating, StateDestroying},
		{StateRunning, StateSuspended},
		{StateRunning, StateDestroying},
		{StateSuspended, StateRunning},
		{StateSuspended, StateDestroying},
		{StateDestroying, StateDestroyed},
	}
	for _, edge := range legal {
		assert.True(t, CanTransition(edge.from, edge.to), edge.from.String()+"->"+edge.to.String())
	}
	for _, edge := range []struct{ from, to State }{
		{StateDestroyed, StateRunning},
		{StateDestroyed, StateDestroying},
		{StateDestroying, StateRunning},
		{StateCreating, StateSuspended},
		{StateRunning, StateCreating},
		{StateSuspended, StateCreating},
	} {
		assert.False(t, CanTransition(edge.from, edge.to), edge.from.String()+"->"+edge.to.String())
	}
}
