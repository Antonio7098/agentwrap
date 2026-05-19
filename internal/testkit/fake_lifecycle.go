package testkit

import "fmt"

// RunStatus is harness-local test vocabulary, not the Sprint 2 public
// lifecycle contract.
type RunStatus string

const (
	StatusStarting  RunStatus = "starting"
	StatusRunning   RunStatus = "running"
	StatusCompleted RunStatus = "completed"
	StatusFailed    RunStatus = "failed"
)

// RunFakeLifecycle derives a deterministic harness-local lifecycle from
// structured fixture records.
func RunFakeLifecycle(records []EventRecord) ([]RunStatus, error) {
	states := []RunStatus{StatusStarting}
	for _, record := range records {
		if record.Err != nil {
			states = append(states, StatusFailed)
			return states, record.Err
		}
		switch record.Type {
		case "run.started":
			states = append(states, StatusRunning)
		case "run.completed":
			states = append(states, StatusCompleted)
			return states, nil
		case "run.failed":
			states = append(states, StatusFailed)
			return states, fmt.Errorf("fake lifecycle failed at event %d", record.Position)
		default:
			continue
		}
	}
	states = append(states, StatusFailed)
	return states, fmt.Errorf("fake lifecycle ended without completion")
}
