package domain

import (
	"errors"
	"strings"
	"testing"
)

var allActors = []Actor{ActorAPI, ActorMaterializer, ActorPromoter, ActorDispatcher, ActorReaper}

// expectedJobTransitions restates LLD §4 independently of the implementation so that any
// edit to the state machine must be made deliberately in both places.
var expectedJobTransitions = []struct {
	id       string
	from, to JobState
	actors   []Actor
}{
	{"T1", StateNew, StateScheduled, []Actor{ActorAPI, ActorMaterializer}},
	{"T2", StateNew, StateReady, []Actor{ActorAPI}},
	{"T3", StateScheduled, StateReady, []Actor{ActorPromoter}},
	{"T4", StateRetryPending, StateReady, []Actor{ActorPromoter}},
	{"T5", StateReady, StateRunning, []Actor{ActorDispatcher}},
	{"T6", StateRunning, StateSucceeded, []Actor{ActorDispatcher}},
	{"T7", StateRunning, StateRetryPending, []Actor{ActorDispatcher, ActorReaper}},
	{"T8", StateRunning, StateFailed, []Actor{ActorDispatcher, ActorReaper}},
	{"T9", StateRunning, StateDeadLettered, []Actor{ActorDispatcher, ActorReaper}},
	{"T10", StateRunning, StateCancelled, []Actor{ActorDispatcher, ActorReaper}},
	{"T11", StateScheduled, StatePaused, []Actor{ActorAPI}},
	{"T12", StateReady, StatePaused, []Actor{ActorAPI}},
	{"T13", StatePaused, StateScheduled, []Actor{ActorAPI}},
	{"T14", StateScheduled, StateCancelled, []Actor{ActorAPI}},
	{"T15", StateReady, StateCancelled, []Actor{ActorAPI}},
	{"T16", StatePaused, StateCancelled, []Actor{ActorAPI}},
	{"T17", StateRetryPending, StateCancelled, []Actor{ActorAPI}},
	{"T18", StateScheduled, StateExpired, []Actor{ActorPromoter, ActorDispatcher}},
	{"T19", StateReady, StateExpired, []Actor{ActorPromoter, ActorDispatcher}},
	{"T20", StateScheduled, StateSkipped, []Actor{ActorPromoter}},
	{"T21", StateFailed, StateReady, []Actor{ActorAPI}},
	{"T22", StateDeadLettered, StateReady, []Actor{ActorAPI}},
}

func TestJobTransitionsMatchLLD(t *testing.T) {
	allowed := map[jobEdge]map[Actor]bool{}
	for _, tr := range expectedJobTransitions {
		allowed[jobEdge{tr.from, tr.to}] = map[Actor]bool{}
		for _, a := range tr.actors {
			allowed[jobEdge{tr.from, tr.to}][a] = true
		}
	}
	states := append([]JobState{StateNew}, JobStates()...)
	for _, from := range states {
		for _, to := range states {
			for _, actor := range allActors {
				want := allowed[jobEdge{from, to}][actor]
				err := ValidateJobTransition(from, to, actor)
				switch {
				case want && err != nil:
					t.Errorf("%s -> %s by %s rejected: %v", from, to, actor, err)
				case !want && err == nil:
					t.Errorf("%s -> %s by %s allowed, but it is not in LLD §4", from, to, actor)
				case !want && !errors.Is(err, ErrInvalidTransition):
					t.Errorf("%s -> %s by %s: error %v does not wrap ErrInvalidTransition", from, to, actor, err)
				}
			}
		}
	}
	if len(jobTransitions) != len(expectedJobTransitions) {
		t.Errorf("implementation has %d edges, LLD lists %d", len(jobTransitions), len(expectedJobTransitions))
	}
}

func TestInvalidTransitionErrorNamesStates(t *testing.T) {
	err := ValidateJobTransition(StateNew, StateRunning, ActorAPI)
	if err == nil || !strings.Contains(err.Error(), "job (new) -> RUNNING by api") {
		t.Errorf("err = %v", err)
	}
}

func successors(from JobState) []JobState {
	var out []JobState
	for e := range jobTransitions {
		if e.from == from {
			out = append(out, e.to)
		}
	}
	return out
}

func reachableFrom(start JobState) map[JobState]bool {
	seen := map[JobState]bool{start: true}
	queue := []JobState{start}
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		for _, next := range successors(s) {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return seen
}

func TestEveryStateIsReachable(t *testing.T) {
	reached := reachableFrom(StateNew)
	for _, s := range JobStates() {
		if !reached[s] {
			t.Errorf("%s is unreachable from (new)", s)
		}
	}
}

// Supports invariant I4: no non-terminal state can trap a job forever.
func TestNoTrapStates(t *testing.T) {
	for _, s := range JobStates() {
		if s.Terminal() {
			continue
		}
		escapes := false
		for r := range reachableFrom(s) {
			if r.Terminal() {
				escapes = true
				break
			}
		}
		if !escapes {
			t.Errorf("%s cannot reach any terminal state", s)
		}
	}
}

// Supports invariant I3: only the dispatcher starts attempts, and only from READY.
func TestRunningEnteredOnlyByDispatcherFromReady(t *testing.T) {
	for e, actors := range jobTransitions {
		if e.to != StateRunning {
			continue
		}
		if e.from != StateReady || len(actors) != 1 || actors[0] != ActorDispatcher {
			t.Errorf("RUNNING entered from %s by %v", e.from, actors)
		}
	}
}

func TestTerminalStatesExitOnlyThroughManualRetry(t *testing.T) {
	for e, actors := range jobTransitions {
		if !e.from.Terminal() {
			continue
		}
		manualRetry := (e.from == StateFailed || e.from == StateDeadLettered) &&
			e.to == StateReady && len(actors) == 1 && actors[0] == ActorAPI
		if !manualRetry {
			t.Errorf("terminal %s has outgoing edge to %s by %v", e.from, e.to, actors)
		}
	}
}

func TestJobStateClassification(t *testing.T) {
	terminal := map[JobState]bool{
		StateSucceeded: true, StateFailed: true, StateDeadLettered: true,
		StateCancelled: true, StateExpired: true, StateSkipped: true,
	}
	for _, s := range JobStates() {
		if !s.Valid() {
			t.Errorf("%s not valid", s)
		}
		if s.Terminal() != terminal[s] {
			t.Errorf("%s.Terminal() = %v", s, s.Terminal())
		}
	}
	for _, s := range []JobState{StateNew, "QUEUED", "running"} {
		if s.Valid() {
			t.Errorf("%q reported valid", s)
		}
	}
}

func TestAttemptTransitions(t *testing.T) {
	want := map[AttemptState][]Actor{
		AttemptSucceeded: {ActorDispatcher},
		AttemptFailed:    {ActorDispatcher},
		AttemptCancelled: {ActorDispatcher},
		AttemptTimedOut:  {ActorDispatcher, ActorReaper},
		AttemptLost:      {ActorDispatcher, ActorReaper},
	}
	states := []AttemptState{AttemptRunning, AttemptSucceeded, AttemptFailed, AttemptCancelled, AttemptTimedOut, AttemptLost}
	for _, from := range states {
		for _, to := range states {
			for _, actor := range allActors {
				ok := from == AttemptRunning && contains(want[to], actor)
				err := ValidateAttemptTransition(from, to, actor)
				if ok != (err == nil) {
					t.Errorf("attempt %s -> %s by %s: err = %v, want allowed=%v", from, to, actor, err, ok)
				}
			}
		}
		if from.Terminal() == (from == AttemptRunning) {
			t.Errorf("%s.Terminal() = %v", from, from.Terminal())
		}
		if !from.Valid() {
			t.Errorf("%s not valid", from)
		}
	}
	if AttemptState("PENDING").Valid() {
		t.Error("unknown attempt state reported valid")
	}
}

func contains(actors []Actor, a Actor) bool {
	for _, x := range actors {
		if x == a {
			return true
		}
	}
	return false
}
