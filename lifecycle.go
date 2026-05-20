package agentwrap

// RunStatus is the caller-visible status vocabulary for runtime and wrapper
// orchestration phases.
type RunStatus string

const (
	StatusStarting   RunStatus = "starting"
	StatusRunning    RunStatus = "running"
	StatusValidating RunStatus = "validating"
	StatusRepairing  RunStatus = "repairing"
	StatusCompleted  RunStatus = "completed"
	StatusFailed     RunStatus = "failed"
	StatusCancelled  RunStatus = "cancelled"
)

// Terminal reports whether the status ends a run from a caller perspective.
func (s RunStatus) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}
