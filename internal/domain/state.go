// Package domain holds the platform's pure domain logic: job and attempt state machines,
// retry decisions and priority selection. It depends only on the standard library.
package domain

import (
	"errors"
	"fmt"
	"slices"
)

// ErrInvalidTransition reports a state change the state machine does not allow.
var ErrInvalidTransition = errors.New("invalid state transition")

// Actor identifies the component performing a state transition.
type Actor string

const (
	ActorAPI          Actor = "api"
	ActorMaterializer Actor = "materializer"
	ActorPromoter     Actor = "promoter"
	ActorDispatcher   Actor = "dispatcher"
	ActorReaper       Actor = "reaper"
)

// JobState is a job's lifecycle state; values are persisted and exposed by the API.
type JobState string

const (
	// StateNew is the pseudo-state a job is created from; it is never persisted.
	StateNew          JobState = ""
	StateScheduled    JobState = "SCHEDULED"
	StateReady        JobState = "READY"
	StateRunning      JobState = "RUNNING"
	StateRetryPending JobState = "RETRY_PENDING"
	StatePaused       JobState = "PAUSED"
	StateSucceeded    JobState = "SUCCEEDED"
	StateFailed       JobState = "FAILED"
	StateDeadLettered JobState = "DEAD_LETTERED"
	StateCancelled    JobState = "CANCELLED"
	StateExpired      JobState = "EXPIRED"
	StateSkipped      JobState = "SKIPPED"
)

var jobStates = []JobState{
	StateScheduled, StateReady, StateRunning, StateRetryPending, StatePaused,
	StateSucceeded, StateFailed, StateDeadLettered, StateCancelled, StateExpired, StateSkipped,
}

// JobStates returns every persisted job state.
func JobStates() []JobState { return slices.Clone(jobStates) }

func (s JobState) Valid() bool { return slices.Contains(jobStates, s) }

// Terminal reports whether s ends the job's lifecycle. FAILED and DEAD_LETTERED are
// terminal even though an operator may retry them.
func (s JobState) Terminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateDeadLettered, StateCancelled, StateExpired, StateSkipped:
		return true
	}
	return false
}

func (s JobState) String() string {
	if s == StateNew {
		return "(new)"
	}
	return string(s)
}

type jobEdge struct{ from, to JobState }

// jobTransitions is the complete job state machine (LLD §4); anything absent is invalid.
var jobTransitions = map[jobEdge][]Actor{
	{StateNew, StateScheduled}:          {ActorAPI, ActorMaterializer},    // T1
	{StateNew, StateReady}:              {ActorAPI},                       // T2
	{StateScheduled, StateReady}:        {ActorPromoter},                  // T3
	{StateRetryPending, StateReady}:     {ActorPromoter},                  // T4
	{StateReady, StateRunning}:          {ActorDispatcher},                // T5
	{StateRunning, StateSucceeded}:      {ActorDispatcher},                // T6
	{StateRunning, StateRetryPending}:   {ActorDispatcher, ActorReaper},   // T7
	{StateRunning, StateFailed}:         {ActorDispatcher, ActorReaper},   // T8
	{StateRunning, StateDeadLettered}:   {ActorDispatcher, ActorReaper},   // T9
	{StateRunning, StateCancelled}:      {ActorDispatcher, ActorReaper},   // T10
	{StateScheduled, StatePaused}:       {ActorAPI},                       // T11
	{StateReady, StatePaused}:           {ActorAPI},                       // T12
	{StatePaused, StateScheduled}:       {ActorAPI},                       // T13
	{StateScheduled, StateCancelled}:    {ActorAPI},                       // T14
	{StateReady, StateCancelled}:        {ActorAPI, ActorPromoter},        // T15
	{StatePaused, StateCancelled}:       {ActorAPI},                       // T16
	{StateRetryPending, StateCancelled}: {ActorAPI, ActorPromoter},        // T17
	{StateScheduled, StateExpired}:      {ActorPromoter, ActorDispatcher}, // T18
	{StateReady, StateExpired}:          {ActorPromoter, ActorDispatcher}, // T19
	{StateScheduled, StateSkipped}:      {ActorPromoter},                  // T20
	{StateFailed, StateReady}:           {ActorAPI},                       // T21
	{StateDeadLettered, StateReady}:     {ActorAPI},                       // T22
}

// ValidateJobTransition returns nil if actor may move a job from one state to another,
// and an error wrapping ErrInvalidTransition otherwise.
func ValidateJobTransition(from, to JobState, actor Actor) error {
	if slices.Contains(jobTransitions[jobEdge{from, to}], actor) {
		return nil
	}
	return fmt.Errorf("%w: job %s -> %s by %s", ErrInvalidTransition, from, to, actor)
}

// AttemptState is the state of one execution attempt; values are persisted.
type AttemptState string

const (
	AttemptRunning   AttemptState = "RUNNING"
	AttemptSucceeded AttemptState = "SUCCEEDED"
	AttemptFailed    AttemptState = "FAILED"
	AttemptCancelled AttemptState = "CANCELLED"
	AttemptTimedOut  AttemptState = "TIMED_OUT"
	AttemptLost      AttemptState = "LOST"
)

// attemptTransitions maps each terminal attempt state to the actors that may set it;
// RUNNING is the only state an attempt can leave (LLD §5).
var attemptTransitions = map[AttemptState][]Actor{
	AttemptSucceeded: {ActorDispatcher},
	AttemptFailed:    {ActorDispatcher},
	AttemptCancelled: {ActorDispatcher},
	AttemptTimedOut:  {ActorDispatcher, ActorReaper},
	AttemptLost:      {ActorDispatcher, ActorReaper},
}

func (s AttemptState) Valid() bool {
	_, terminal := attemptTransitions[s]
	return s == AttemptRunning || terminal
}

func (s AttemptState) Terminal() bool {
	_, terminal := attemptTransitions[s]
	return terminal
}

// ValidateAttemptTransition returns nil if actor may move an attempt between the states,
// and an error wrapping ErrInvalidTransition otherwise.
func ValidateAttemptTransition(from, to AttemptState, actor Actor) error {
	if from == AttemptRunning && slices.Contains(attemptTransitions[to], actor) {
		return nil
	}
	return fmt.Errorf("%w: attempt %s -> %s by %s", ErrInvalidTransition, from, to, actor)
}
