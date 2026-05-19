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
			lifecycleEvent(agentwrap.StatusStarting),
			lifecycleEvent(agentwrap.StatusRunning),
			{
				Type:    "message.delta",
				Payload: agentwrap.EventPayloadWithKind(agentwrap.EventMessage, agentwrap.EventPayload{"text": "working"}),
				Raw:     &agentwrap.RawPayload{Source: "fake", Encoding: "json", Data: []byte(`{"native":true}`), Safe: true},
			},
			{
				Type: "artifact.created",
				Payload: agentwrap.EventPayloadWithKind(agentwrap.EventArtifact, agentwrap.EventPayload{"artifact": agentwrap.ArtifactRef{
					ID:   "artifact-1",
					URI:  "file:///tmp/report.md",
					Kind: "markdown",
				}}),
			},
			{
				Type:    "usage.update",
				Payload: agentwrap.EventPayloadWithKind(agentwrap.EventUsage, agentwrap.EventPayload{"usage": agentwrap.Usage{TotalTokens: int64Ptr(42)}}),
			},
			lifecycleEvent(agentwrap.StatusCompleted),
			{Type: "run.final", Payload: agentwrap.EventPayloadWithKind(agentwrap.EventFinalResult, agentwrap.EventPayload{"status": string(agentwrap.StatusCompleted)})},
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

	if result.Status != agentwrap.StatusCompleted {
		t.Fatalf("Status = %q, want %q", result.Status, agentwrap.StatusCompleted)
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
	for _, event := range events {
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
				lifecycleEvent(agentwrap.StatusRunning),
				{Type: "event.malformed", Payload: agentwrap.EventPayloadWithKind(agentwrap.EventWarning, agentwrap.EventPayload{"category": string(agentwrap.ErrorMalformedEvent)})},
				{Type: "runtime.future_event", Payload: agentwrap.EventPayloadWithKind(agentwrap.EventNativeExtension, nil), Raw: &agentwrap.RawPayload{Source: "fake", Data: []byte(`{"future":true}`)}},
				lifecycleEvent(agentwrap.StatusCompleted),
			},
		}
		run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "malformed"})
		if err != nil {
			t.Fatal(err)
		}
		var sawMalformed bool
		var sawUnknown bool
		for event := range run.Events() {
			if event.Kind() == agentwrap.EventWarning {
				sawMalformed = true
			}
			if event.Kind() == agentwrap.EventNativeExtension && event.Raw != nil {
				sawUnknown = true
			}
		}
		result, err := run.Wait(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != agentwrap.StatusCompleted || !sawMalformed || !sawUnknown {
			t.Fatalf("status=%q malformed=%v unknown=%v", result.Status, sawMalformed, sawUnknown)
		}
	})

	t.Run("fatal event classification", func(t *testing.T) {
		runtime := &FakeRuntime{Script: []agentwrap.Event{
			lifecycleEvent(agentwrap.StatusRunning),
			{Type: "run.failed", Payload: agentwrap.EventPayloadWithKind(agentwrap.EventFatalError, nil)},
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
		if result.Status != agentwrap.StatusFailed {
			t.Fatalf("Status = %q, want %q", result.Status, agentwrap.StatusFailed)
		}
		var sdkErr *agentwrap.SDKError
		if !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorRuntimeExit {
			t.Fatalf("error = %#v, want runtime exit SDKError", err)
		}
	})

	t.Run("cancellation state", func(t *testing.T) {
		runtime := &FakeRuntime{Script: []agentwrap.Event{
			lifecycleEvent(agentwrap.StatusRunning),
			lifecycleEvent(agentwrap.StatusRunning),
		}}
		run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "cancel"})
		if err != nil {
			t.Fatal(err)
		}
		if event := <-run.Events(); event.Kind() != agentwrap.EventLifecycle {
			t.Fatalf("first event category = %q", event.Kind())
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
		if result.Status != agentwrap.StatusCancelled {
			t.Fatalf("Status = %q, want %q", result.Status, agentwrap.StatusCancelled)
		}
	})
}

func lifecycleEvent(state agentwrap.RunStatus) agentwrap.Event {
	return agentwrap.Event{
		Type:    "lifecycle." + string(state),
		Payload: agentwrap.EventPayloadWithKind(agentwrap.EventLifecycle, agentwrap.EventPayload{"state": state}),
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}
