package testkit

import "fmt"

// LifecycleState is harness-local test vocabulary, not the Sprint 2 public
// lifecycle contract.
type LifecycleState string

const (
	StateStarting  LifecycleState = "starting"
	StateRunning   LifecycleState = "running"
	StateCompleted LifecycleState = "completed"
	StateFailed    LifecycleState = "failed"
)

// RunFakeLifecycle derives a deterministic harness-local lifecycle from
// structured fixture records.
func RunFakeLifecycle(records []EventRecord) ([]LifecycleState, error) {
	states := []LifecycleState{StateStarting}
	for _, record := range records {
		if record.Err != nil {
			states = append(states, StateFailed)
			return states, record.Err
		}
		switch record.Type {
		case "run.started":
			states = append(states, StateRunning)
		case "run.completed":
			states = append(states, StateCompleted)
			return states, nil
		case "run.failed":
			states = append(states, StateFailed)
			return states, fmt.Errorf("fake lifecycle failed at event %d", record.Position)
		default:
			continue
		}
	}
	states = append(states, StateFailed)
	return states, fmt.Errorf("fake lifecycle ended without completion")
}
