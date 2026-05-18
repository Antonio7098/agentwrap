package agentwrap

import "testing"

func TestLifecycleTerminalStates(t *testing.T) {
	for _, tc := range []struct {
		state LifecycleState
		want  bool
	}{
		{state: StateInitialized},
		{state: StateRunning},
		{state: StateRetrying},
		{state: StateCompleted, want: true},
		{state: StateFailed, want: true},
		{state: StateCancelled, want: true},
		{state: StateCleanedUp, want: true},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			if got := tc.state.Terminal(); got != tc.want {
				t.Fatalf("Terminal() = %v, want %v", got, tc.want)
			}
		})
	}
}
