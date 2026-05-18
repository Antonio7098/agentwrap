package testkit

import (
	"context"
	"errors"
	"testing"

	agentwrap "github.com/antonioborgerees/agentwrap"
)

func TestFakeRuntimeContractNormalStream(t *testing.T) {
	runtime := &FakeRuntime{
		Provider: "fake-provider",
		Model:    "fake-model",
		Script: []agentwrap.Event{
			lifecycleEvent(agentwrap.StateStarting),
			lifecycleEvent(agentwrap.StateRunning),
			{
				Category: agentwrap.EventMessage,
				Type:     "message.delta",
				Payload:  agentwrap.EventPayload{"text": "working"},
				Raw:      &agentwrap.RawPayload{Source: "fake", Encoding: "json", Data: []byte(`{"native":true}`), Safe: true},
			},
			{
				Category: agentwrap.EventArtifact,
				Type:     "artifact.created",
				Payload: agentwrap.EventPayload{"artifact": agentwrap.ArtifactRef{
					ID:   "artifact-1",
					URI:  "file:///tmp/report.md",
					Kind: "markdown",
				}},
			},
			{
				Category: agentwrap.EventUsage,
				Type:     "usage.update",
				Payload:  agentwrap.EventPayload{"usage": agentwrap.Usage{TotalTokens: int64Ptr(42)}},
			},
			lifecycleEvent(agentwrap.StateCompleted),
			{Category: agentwrap.EventFinalResult, Type: "run.final", Payload: agentwrap.EventPayload{"status": string(agentwrap.StateCompleted)}},
		},
	}
	var _ agentwrap.Runtime = runtime

	run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:      "write report",
		WantSession: true,
		TurnID:      "turn-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	var events []agentwrap.Event
	for event := range run.Events() {
		events = append(events, event)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != agentwrap.StateCompleted {
		t.Fatalf("Status = %q, want %q", result.Status, agentwrap.StateCompleted)
	}
	if result.SessionID == "" {
		t.Fatal("SessionID is empty")
	}
	if len(result.Artifacts) != 1 {
		t.Fatalf("artifacts = %d, want 1", len(result.Artifacts))
	}
	if result.Usage.TotalTokens == nil || *result.Usage.TotalTokens != 42 {
		t.Fatalf("TotalTokens = %#v, want 42", result.Usage.TotalTokens)
	}
	if len(events) != len(runtime.Script) {
		t.Fatalf("events = %d, want %d", len(events), len(runtime.Script))
	}
	if events[2].Raw == nil || string(events[2].Raw.Data) != `{"native":true}` {
		t.Fatalf("raw payload = %#v", events[2].Raw)
	}
	for i, event := range events {
		if event.Sequence != int64(i+1) {
			t.Fatalf("event %d sequence = %d", i, event.Sequence)
		}
		if event.RunID != run.ID() {
			t.Fatalf("event RunID = %q, want %q", event.RunID, run.ID())
		}
	}
}

func TestFakeRuntimeCapabilitiesAndUnsupportedRequirement(t *testing.T) {
	runtime := &FakeRuntime{}

	caps, err := runtime.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !caps.Supports(agentwrap.CapabilityStructuredEvents) {
		t.Fatal("structured events capability is not supported")
	}
	if caps.Supports(agentwrap.CapabilityPermissions) {
		t.Fatal("permissions capability should be unsupported in Sprint 2 fake")
	}

	_, err = runtime.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:      "needs permissions",
		RequireCaps: []agentwrap.Capability{agentwrap.CapabilityPermissions},
	})
	if err == nil {
		t.Fatal("StartRun returned nil error")
	}
	var sdkErr *agentwrap.SDKError
	if !errors.As(err, &sdkErr) {
		t.Fatal("error is not SDKError")
	}
	if sdkErr.Category != agentwrap.ErrorConfiguration {
		t.Fatalf("Category = %q, want %q", sdkErr.Category, agentwrap.ErrorConfiguration)
	}
}

func TestFakeRuntimeMalformedUnknownFailureAndCancellation(t *testing.T) {
	t.Run("malformed event classification", func(t *testing.T) {
		runtime := &FakeRuntime{
			Script: []agentwrap.Event{
				lifecycleEvent(agentwrap.StateRunning),
				{Category: agentwrap.EventRecoverableError, Type: "event.malformed", Payload: agentwrap.EventPayload{"category": string(agentwrap.ErrorMalformedEvent)}},
				{Category: agentwrap.EventUnknown, Type: "runtime.future_event", Raw: &agentwrap.RawPayload{Source: "fake", Data: []byte(`{"future":true}`)}},
				lifecycleEvent(agentwrap.StateCompleted),
			},
		}
		run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "malformed"})
		if err != nil {
			t.Fatal(err)
		}
		var sawMalformed bool
		var sawUnknown bool
		for event := range run.Events() {
			if event.Category == agentwrap.EventRecoverableError {
				sawMalformed = true
			}
			if event.Category == agentwrap.EventUnknown && event.Raw != nil {
				sawUnknown = true
			}
		}
		result, err := run.Wait(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != agentwrap.StateCompleted || !sawMalformed || !sawUnknown {
			t.Fatalf("status=%q malformed=%v unknown=%v", result.Status, sawMalformed, sawUnknown)
		}
	})

	t.Run("fatal event classification", func(t *testing.T) {
		runtime := &FakeRuntime{Script: []agentwrap.Event{
			lifecycleEvent(agentwrap.StateRunning),
			{Category: agentwrap.EventFatalError, Type: "run.failed"},
		}}
		run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "fail"})
		if err != nil {
			t.Fatal(err)
		}
		for range run.Events() {
		}
		result, err := run.Wait(context.Background())
		if err == nil {
			t.Fatal("Wait returned nil error")
		}
		if result.Status != agentwrap.StateFailed {
			t.Fatalf("Status = %q, want %q", result.Status, agentwrap.StateFailed)
		}
		var sdkErr *agentwrap.SDKError
		if !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorRuntimeExit {
			t.Fatalf("error = %#v, want runtime exit SDKError", err)
		}
	})

	t.Run("cancellation state", func(t *testing.T) {
		runtime := &FakeRuntime{Script: []agentwrap.Event{
			lifecycleEvent(agentwrap.StateRunning),
			lifecycleEvent(agentwrap.StateWaiting),
		}}
		run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "cancel"})
		if err != nil {
			t.Fatal(err)
		}
		if event := <-run.Events(); event.Category != agentwrap.EventLifecycle {
			t.Fatalf("first event category = %q", event.Category)
		}
		if err := run.Cancel(context.Background()); err != nil {
			t.Fatal(err)
		}
		for range run.Events() {
		}
		result, err := run.Wait(context.Background())
		if err == nil {
			t.Fatal("Wait returned nil error")
		}
		if result.Status != agentwrap.StateCancelled {
			t.Fatalf("Status = %q, want %q", result.Status, agentwrap.StateCancelled)
		}
	})
}

func lifecycleEvent(state agentwrap.LifecycleState) agentwrap.Event {
	return agentwrap.Event{
		Category: agentwrap.EventLifecycle,
		Type:     "lifecycle." + string(state),
		Payload:  agentwrap.EventPayload{"state": state},
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}
