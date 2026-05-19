package agentwrap

import (
	"testing"
	"time"
)

func TestLifecycleEventPayload(t *testing.T) {
	event := LifecycleEvent("run-1", "session-1", "turn-1", RuntimeContext{RuntimeKind: "fake"}, 7, time.Unix(1, 0), StatusRunning, StatusCompleted, "done")
	if event.Kind() != EventLifecycle || event.Type != "lifecycle.transition" {
		t.Fatalf("event = %#v", event)
	}
	if event.Payload["from"] != "running" || event.Payload["to"] != "completed" || event.Payload["reason"] != "done" {
		t.Fatalf("payload = %#v", event.Payload)
	}
}

func TestSessionEventPayload(t *testing.T) {
	metadata := SessionMetadata{
		ID:              "session-2",
		RequestedID:     "session-1",
		RequestedAction: SessionActionContinue,
		Relationship:    SessionRelationshipBestEffort,
		BestEffort:      true,
	}
	event := SessionEvent("run-1", "session-2", "turn-1", RuntimeContext{RuntimeKind: "fake"}, 8, time.Unix(1, 0), metadata)
	if event.Kind() != EventSession || event.Type != "session.relationship" {
		t.Fatalf("event = %#v", event)
	}
	if event.Payload["requested_action"] != "continue" || event.Payload["relationship"] != "best_effort" || event.Payload["best_effort"] != true {
		t.Fatalf("payload = %#v", event.Payload)
	}
}
