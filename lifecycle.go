package agentwrap

// LifecycleState is the caller-visible state vocabulary for runs and sessions.
//
// Retry, fallback, validation, repair, cleanup, and health-check behavior is
// implemented by later sprints; the states are defined now to keep the public
// vocabulary stable.
type LifecycleState string

const (
	StateInitialized    LifecycleState = "initialized"
	StateHealthChecking LifecycleState = "health_checking"
	StateReady          LifecycleState = "ready"
	StateStarting       LifecycleState = "starting"
	StateRunning        LifecycleState = "running"
	StateWaiting        LifecycleState = "waiting"
	StateRetrying       LifecycleState = "retrying"
	StateFallback       LifecycleState = "fallback"
	StateValidating     LifecycleState = "validating"
	StateRepairing      LifecycleState = "repairing"
	StateCompleted      LifecycleState = "completed"
	StateFailed         LifecycleState = "failed"
	StateCancelled      LifecycleState = "cancelled"
	StateCleanedUp      LifecycleState = "cleaned_up"
)

// Terminal reports whether the state ends a run from a caller perspective.
func (s LifecycleState) Terminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateCancelled, StateCleanedUp:
		return true
	default:
		return false
	}
}
