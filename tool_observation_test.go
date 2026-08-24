package agentwrap_test

import (
	"testing"

	agentwrap "github.com/Antonio7098/agentwrap"
)

func TestToolObservationNormalizesAndRedactsStructuredFields(t *testing.T) {
	payload := agentwrap.EventPayloadWithToolObservation(
		agentwrap.EventPayloadWithKind(agentwrap.EventTool, nil),
		agentwrap.ToolObservation{
			CallID: "call-1",
			Name:   "shell",
			Status: "completed",
			Arguments: map[string]any{
				"command":       "go test ./...",
				"authorization": "Bearer private-value",
			},
			Result: map[string]any{"output": "ok", "api_token": "secret"},
		},
	)
	event := agentwrap.Event{Payload: payload}
	tool, ok := event.ToolObservation()
	if !ok || tool.CallID != "call-1" || tool.Name != "shell" || tool.Status != "completed" {
		t.Fatalf("tool observation = %#v, ok=%v", tool, ok)
	}
	args := tool.Arguments.(map[string]any)
	result := tool.Result.(map[string]any)
	if args["command"] != "go test ./..." || args["authorization"] != "[REDACTED]" {
		t.Fatalf("arguments = %#v", args)
	}
	if result["output"] != "ok" || result["api_token"] != "[REDACTED]" {
		t.Fatalf("result = %#v", result)
	}
}
