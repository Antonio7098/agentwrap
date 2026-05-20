package agentwrap_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	agentwrap "github.com/antonioborgerees/agentwrap"
	"github.com/antonioborgerees/agentwrap/internal/testkit"
)

func TestMemoryRunStoreActiveCompletedAndEventOrdering(t *testing.T) {
	store := agentwrap.NewMemoryRunStore()
	ctx := context.Background()
	active := agentwrap.RunRecord{RunID: "run-1", Status: agentwrap.StatusRunning}
	if err := store.UpsertRun(ctx, active); err != nil {
		t.Fatalf("upsert active: %v", err)
	}
	for i := int64(1); i <= 3; i++ {
		if err := store.AppendEvent(ctx, agentwrap.RunEventRecord{RunID: "run-1", Sequence: i, EventID: agentwrap.EventID(fmt.Sprintf("event-%d", i))}); err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}
	activeRuns, err := store.ListActiveRuns(ctx)
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(activeRuns) != 1 || activeRuns[0].RunID != "run-1" {
		t.Fatalf("active runs = %#v", activeRuns)
	}
	completed := active
	completed.Status = agentwrap.StatusCompleted
	completed.ParentRunID = "parent-1"
	if err := store.UpsertRun(ctx, completed); err != nil {
		t.Fatalf("upsert completed: %v", err)
	}
	activeRuns, err = store.ListActiveRuns(ctx)
	if err != nil {
		t.Fatalf("list active after completion: %v", err)
	}
	if len(activeRuns) != 0 {
		t.Fatalf("active after completion = %#v", activeRuns)
	}
	got, ok, err := store.GetCompletedRun(ctx, "run-1")
	if err != nil || !ok {
		t.Fatalf("completed lookup ok=%v err=%v", ok, err)
	}
	if got.ParentRunID != "parent-1" {
		t.Fatalf("parent run id = %q", got.ParentRunID)
	}
	events, err := store.ListRunEvents(ctx, "run-1")
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for i, event := range events {
		if event.Sequence != int64(i+1) {
			t.Fatalf("event %d sequence = %d", i, event.Sequence)
		}
	}
}

func TestObservingRuntimeStoresRecordsAndOmitsUnsafeRawPayload(t *testing.T) {
	store := agentwrap.NewMemoryRunStore()
	usageTokens := int64(42)
	runtime := agentwrap.ObservingRuntime{
		Runtime: &testkit.FakeRuntime{
			Kind:     "fake-kind",
			Provider: "provider-a",
			Model:    "model-a",
			Script: []agentwrap.Event{
				{
					Type: "run.started",
					Payload: agentwrap.EventPayloadWithKind(agentwrap.EventLifecycle, agentwrap.EventPayload{
						"state": agentwrap.StatusRunning,
					}),
					Raw: &agentwrap.RawPayload{Source: "native", Encoding: "json", Data: []byte(`{"secret":"value"}`), Safe: false},
				},
				{
					Type: "usage.update",
					Payload: agentwrap.EventPayloadWithKind(agentwrap.EventUsage, agentwrap.EventPayload{
						"usage": agentwrap.Usage{TotalTokens: &usageTokens},
					}),
				},
				{
					Type: "artifact.created",
					Payload: agentwrap.EventPayloadWithKind(agentwrap.EventArtifact, agentwrap.EventPayload{
						"artifact": agentwrap.ArtifactRef{ID: "report", URI: "file://report.md", Kind: "markdown"},
					}),
				},
			},
		},
		Store: store,
	}
	run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{
		TurnID:      "turn-1",
		Provider:    "provider-a",
		Model:       "model-a",
		WantSession: true,
		Metadata:    map[string]string{"parent_run_id": "parent-1"},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	drainEvents(t, run.Events())
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	record, ok, err := store.GetCompletedRun(context.Background(), result.RunID)
	if err != nil || !ok {
		t.Fatalf("completed lookup ok=%v err=%v", ok, err)
	}
	if record.Status != agentwrap.StatusCompleted {
		t.Fatalf("status = %q", record.Status)
	}
	if record.ParentRunID != "parent-1" {
		t.Fatalf("parent = %q", record.ParentRunID)
	}
	if record.Usage.TotalTokens == nil || *record.Usage.TotalTokens != usageTokens {
		t.Fatalf("usage = %#v", record.Usage)
	}
	if len(record.Artifacts) != 1 {
		t.Fatalf("artifacts = %#v", record.Artifacts)
	}
	metadata := record.Artifacts[0].Metadata
	if metadata["producer_run_id"] != string(result.RunID) || metadata["producer_provider"] != "provider-a" || metadata["producer_model"] != "model-a" {
		t.Fatalf("producer metadata = %#v", metadata)
	}
	events, err := store.ListRunEvents(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("event count = %d", len(events))
	}
	if !events[0].RawPresent || !events[0].RawOmitted || len(events[0].RawData) != 0 {
		t.Fatalf("raw omission = %#v", events[0])
	}
}

func TestObservingRuntimePreservesEventCollectedArtifacts(t *testing.T) {
	store := agentwrap.NewMemoryRunStore()
	runtime := agentwrap.ObservingRuntime{
		Runtime: staticRuntime{run: &staticRun{
			id: "event-artifact-run",
			events: []agentwrap.Event{{
				ID:    "artifact-event",
				RunID: "event-artifact-run",
				Type:  "artifact.created",
				Payload: agentwrap.EventPayloadWithKind(agentwrap.EventArtifact, agentwrap.EventPayload{
					"artifact": agentwrap.ArtifactRef{ID: "event-report", URI: "file://event-report.md", Kind: "markdown"},
				}),
			}},
			result: agentwrap.RunResult{
				RunID:      "event-artifact-run",
				Status:     agentwrap.StatusCompleted,
				StartedAt:  time.Now().Add(-time.Second).UTC(),
				FinishedAt: time.Now().UTC(),
			},
		}},
		Store: store,
	}
	run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	drainEvents(t, run.Events())
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	record, ok, err := store.GetCompletedRun(context.Background(), result.RunID)
	if err != nil || !ok {
		t.Fatalf("completed lookup ok=%v err=%v", ok, err)
	}
	if len(record.Artifacts) != 1 || record.Artifacts[0].ID != "event-report" {
		t.Fatalf("artifacts = %#v", record.Artifacts)
	}
}

func TestRunEventRecordPayloadIsDeepCloned(t *testing.T) {
	store := agentwrap.NewMemoryRunStore()
	ctx := context.Background()
	nested := map[string]any{"value": "original"}
	items := []any{map[string]any{"name": "first"}}
	record := agentwrap.RunEventRecord{
		RunID:    "run-1",
		EventID:  "event-1",
		Sequence: 1,
		Payload:  agentwrap.EventPayload{"nested": nested, "items": items},
	}
	if err := store.AppendEvent(ctx, record); err != nil {
		t.Fatalf("append: %v", err)
	}
	nested["value"] = "mutated"
	items[0].(map[string]any)["name"] = "mutated"
	events, err := store.ListRunEvents(ctx, "run-1")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	gotNested := events[0].Payload["nested"].(map[string]any)
	gotItems := events[0].Payload["items"].([]any)
	gotItem := gotItems[0].(map[string]any)
	if gotNested["value"] != "original" || gotItem["name"] != "first" {
		t.Fatalf("payload was not deeply cloned: %#v", events[0].Payload)
	}
}

func TestObservingRuntimeRequiredSinkFailureChangesWaitError(t *testing.T) {
	sinkErr := errors.New("sink down")
	runtime := agentwrap.ObservingRuntime{
		Runtime: &testkit.FakeRuntime{
			Script: []agentwrap.Event{{
				Type:    "run.started",
				Payload: agentwrap.EventPayloadWithKind(agentwrap.EventLifecycle, agentwrap.EventPayload{"state": agentwrap.StatusRunning}),
			}},
		},
		Store: agentwrap.NewMemoryRunStore(),
		Sinks: []agentwrap.NamedEventSink{{Name: "required", Sink: failingSink{err: sinkErr}, Required: true}},
	}
	run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	drainEvents(t, run.Events())
	_, err = run.Wait(context.Background())
	if err == nil {
		t.Fatal("expected required sink failure")
	}
	var sdkErr *agentwrap.SDKError
	if !errors.As(err, &sdkErr) {
		t.Fatalf("expected SDKError, got %T", err)
	}
	if sdkErr.Category != agentwrap.ErrorUnknown {
		t.Fatalf("category = %q", sdkErr.Category)
	}
}

func TestObservingRuntimeBestEffortSinkFailureIsRecorded(t *testing.T) {
	store := agentwrap.NewMemoryRunStore()
	runtime := agentwrap.ObservingRuntime{
		Runtime: &testkit.FakeRuntime{
			Script: []agentwrap.Event{{
				Type:    "run.started",
				Payload: agentwrap.EventPayloadWithKind(agentwrap.EventLifecycle, agentwrap.EventPayload{"state": agentwrap.StatusRunning}),
			}},
		},
		Store: store,
		Sinks: []agentwrap.NamedEventSink{{Name: "best-effort", Sink: failingSink{err: errors.New("optional sink down")}}},
	}
	run, err := runtime.StartRun(context.Background(), agentwrap.RunRequest{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	drainEvents(t, run.Events())
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("wait should preserve primary outcome: %v", err)
	}
	record, ok, err := store.GetCompletedRun(context.Background(), result.RunID)
	if err != nil || !ok {
		t.Fatalf("completed lookup ok=%v err=%v", ok, err)
	}
	if len(record.SinkFailures) != 1 || record.SinkFailures[0].Required {
		t.Fatalf("sink failures = %#v", record.SinkFailures)
	}
}

func TestMemoryRunStoreConcurrentRunIsolation(t *testing.T) {
	store := agentwrap.NewMemoryRunStore()
	ctx := context.Background()
	var wg sync.WaitGroup
	errCh := make(chan error, 60)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runID := agentwrap.RunID(fmt.Sprintf("run-%d", i))
			if err := store.UpsertRun(ctx, agentwrap.RunRecord{RunID: runID, Status: agentwrap.StatusRunning}); err != nil {
				errCh <- fmt.Errorf("upsert running %s: %w", runID, err)
				return
			}
			if err := store.AppendEvent(ctx, agentwrap.RunEventRecord{RunID: runID, Sequence: 1, EventID: agentwrap.EventID(fmt.Sprintf("event-%d", i))}); err != nil {
				errCh <- fmt.Errorf("append event %s: %w", runID, err)
				return
			}
			if err := store.UpsertRun(ctx, agentwrap.RunRecord{RunID: runID, Status: agentwrap.StatusCompleted, CompletedAt: time.Now().UTC()}); err != nil {
				errCh <- fmt.Errorf("upsert completed %s: %w", runID, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	active, err := store.ListActiveRuns(ctx)
	if err != nil {
		t.Fatalf("active: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active = %d", len(active))
	}
	for i := 0; i < 20; i++ {
		runID := agentwrap.RunID(fmt.Sprintf("run-%d", i))
		if _, ok, err := store.GetCompletedRun(ctx, runID); err != nil || !ok {
			t.Fatalf("completed %s ok=%v err=%v", runID, ok, err)
		}
		events, err := store.ListRunEvents(ctx, runID)
		if err != nil {
			t.Fatalf("events %s: %v", runID, err)
		}
		if len(events) != 1 || events[0].RunID != runID {
			t.Fatalf("events %s = %#v", runID, events)
		}
	}
}

type failingSink struct {
	err error
}

func (s failingSink) AppendEvent(context.Context, agentwrap.RunEventRecord) error {
	return s.err
}

type staticRuntime struct {
	run *staticRun
}

func (r staticRuntime) StartRun(context.Context, agentwrap.RunRequest) (agentwrap.Run, error) {
	return r.run, nil
}

func (r staticRuntime) Capabilities(context.Context) (agentwrap.Capabilities, error) {
	return agentwrap.Capabilities{}, nil
}

type staticRun struct {
	id     agentwrap.RunID
	events []agentwrap.Event
	result agentwrap.RunResult
}

func (r *staticRun) ID() agentwrap.RunID {
	return r.id
}

func (r *staticRun) Events() <-chan agentwrap.Event {
	events := make(chan agentwrap.Event, len(r.events))
	for _, event := range r.events {
		events <- event
	}
	close(events)
	return events
}

func (r *staticRun) Wait(context.Context) (agentwrap.RunResult, error) {
	if r.result.Err != nil {
		return r.result, r.result.Err
	}
	return r.result, nil
}

func (r *staticRun) Cancel(context.Context) error {
	return nil
}

func drainEvents(t *testing.T, events <-chan agentwrap.Event) {
	t.Helper()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-timeout.C:
			t.Fatal("timed out draining events channel")
		}
	}
}
