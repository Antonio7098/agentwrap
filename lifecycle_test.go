package agentwrap

import "testing"

func TestLifecycleTerminalStates(t *testing.T) {
	for _, tc := range []struct {
		state RunStatus
		want  bool
	}{
		{state: StatusStarting},
		{state: StatusRunning},
		{state: StatusCompleted, want: true},
		{state: StatusFailed, want: true},
		{state: StatusCancelled, want: true},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			if got := tc.state.Terminal(); got != tc.want {
				t.Fatalf("Terminal() = %v, want %v", got, tc.want)
			}
		})
	}
}
