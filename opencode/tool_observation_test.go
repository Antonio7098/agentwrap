package opencode

import "testing"

func TestToolObservationFromNestedOpenCodePart(t *testing.T) {
	tool := toolObservationFrom(map[string]any{
		"part": map[string]any{
			"callID": "call-7",
			"tool":   "bash",
			"state": map[string]any{
				"status": "completed",
				"input":  map[string]any{"command": "go test ./..."},
				"output": "ok",
			},
		},
	})
	if tool.CallID != "call-7" || tool.Name != "bash" || tool.Status != "completed" {
		t.Fatalf("identity = %#v", tool)
	}
	if tool.Arguments.(map[string]any)["command"] != "go test ./..." || tool.Result != "ok" {
		t.Fatalf("details = %#v", tool)
	}
}
