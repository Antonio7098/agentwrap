package agentwrap

// RunStatus is the small caller-visible status vocabulary for a runtime run.
// Recovery work such as retry, fallback, validation, repair, and cleanup is
// reported as events or metadata, not as core run states.
type RunStatus string

const (
	StatusStarting  RunStatus = "starting"
	StatusRunning   RunStatus = "running"
	StatusCompleted RunStatus = "completed"
	StatusFailed    RunStatus = "failed"
	StatusCancelled RunStatus = "cancelled"
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
