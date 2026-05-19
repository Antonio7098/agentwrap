package agentwrap

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPolicyRunnerRetriesThenSucceeds(t *testing.T) {
	retryErr := NewError(ErrorTimeout, "fake", "temporary timeout", nil)
	runtime := &scriptRuntime{results: []scriptResult{
		{err: retryErr},
		{},
	}}
	runner := PolicyRunner{
		Runtime: runtime,
		Policy:  BasicPolicy{MaxAttemptsPerTarget: 2, Backoff: FixedBackoff{DelayValue: time.Millisecond}},
		Sleep:   noSleep,
	}

	run, err := runner.StartRun(context.Background(), RunRequest{Provider: "openai", Model: "gpt"})
	if err != nil {
		t.Fatalf("StartRun error: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait error: %v", err)
	}
	if result.Status != StateCompleted {
		t.Fatalf("status = %s, want completed", result.Status)
	}
	if runtime.starts != 2 {
		t.Fatalf("starts = %d, want 2", runtime.starts)
	}
	if got := len(result.Metadata.Attempts); got != 2 {
		t.Fatalf("attempt count = %d, want 2", got)
	}
	if len(result.Metadata.Policy.Decisions) != 1 || result.Metadata.Policy.Decisions[0].Kind != PolicyDecisionRetry {
		t.Fatalf("decisions = %#v, want one retry", result.Metadata.Policy.Decisions)
	}
	events := collectEvents(run)
	if !hasCategory(events, EventRetry) {
		t.Fatalf("events missing retry event: %#v", events)
	}
}

func TestPolicyRunnerFallbackThenRetriesOnFallbackTarget(t *testing.T) {
	primaryErr := NewError(ErrorRuntimeExit, "primary", "primary failed", nil, WithRetryable(false), WithFallbackable(true))
	fallbackErr := NewError(ErrorTimeout, "fallback", "fallback timeout", nil)
	primary := &scriptRuntime{name: "primary", results: []scriptResult{{err: primaryErr}}}
	fallback := &scriptRuntime{name: "fallback", provider: "anthropic", model: "claude", results: []scriptResult{
		{err: fallbackErr},
		{},
	}}
	runner := PolicyRunner{
		Runtime: primary,
		Alternatives: []FallbackAlternative{{
			Name:    "fallback",
			Runtime: fallback,
			Request: RunRequest{Provider: "anthropic", Model: "claude", SessionAction: SessionActionFresh},
		}},
		Policy: BasicPolicy{MaxAttemptsPerTarget: 2, Backoff: FixedBackoff{}},
		Sleep:  noSleep,
	}

	run, err := runner.StartRun(context.Background(), RunRequest{Provider: "openai", Model: "gpt", WantSession: true})
	if err != nil {
		t.Fatalf("StartRun error: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait error: %v", err)
	}
	if primary.starts != 1 || fallback.starts != 2 {
		t.Fatalf("starts primary=%d fallback=%d, want 1/2", primary.starts, fallback.starts)
	}
	if got := len(result.Metadata.Attempts); got != 3 {
		t.Fatalf("attempt count = %d, want 3", got)
	}
	if result.Metadata.Attempts[1].TargetIndex != 1 || result.Metadata.Attempts[1].Request.Provider != "anthropic" {
		t.Fatalf("fallback attempt = %#v", result.Metadata.Attempts[1])
	}
	if result.Metadata.Attempts[1].Request.SessionAction != SessionActionFresh {
		t.Fatalf("fallback session action = %q, want fresh", result.Metadata.Attempts[1].Request.SessionAction)
	}
	if !hasDecision(result.Metadata.Policy.Decisions, PolicyDecisionFallback) || !hasDecision(result.Metadata.Policy.Decisions, PolicyDecisionRetry) {
		t.Fatalf("decisions = %#v, want fallback and retry", result.Metadata.Policy.Decisions)
	}
}

func TestBasicPolicyDoesNotRetryUnknownByDefault(t *testing.T) {
	unknown := NewError(ErrorUnknown, "fake", "unknown failure", nil)
	runtime := &scriptRuntime{results: []scriptResult{{err: unknown}, {}}}
	runner := PolicyRunner{
		Runtime: runtime,
		Policy:  BasicPolicy{MaxAttemptsPerTarget: 3, Backoff: FixedBackoff{}},
		Sleep:   noSleep,
	}

	run, err := runner.StartRun(context.Background(), RunRequest{})
	if err != nil {
		t.Fatalf("StartRun error: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err == nil {
		t.Fatalf("Wait error = nil, want unknown failure")
	}
	if runtime.starts != 1 {
		t.Fatalf("starts = %d, want 1", runtime.starts)
	}
	if got := len(result.Metadata.Attempts); got != 1 {
		t.Fatalf("attempt count = %d, want 1", got)
	}
	if result.Metadata.Policy.Decisions[0].Kind != PolicyDecisionStop {
		t.Fatalf("decision = %#v, want stop", result.Metadata.Policy.Decisions[0])
	}
	if hasCategory(collectEvents(run), EventRetry) {
		t.Fatalf("stop decision emitted retry event")
	}
}

func TestPolicyRunnerHonorsRateLimitRetryAfter(t *testing.T) {
	rateErr := NewError(ErrorRateLimit, "fake", "provider rate limited", nil)
	runtime := &scriptRuntime{provider: "openai", model: "gpt", results: []scriptResult{{err: rateErr}, {}}}
	var slept time.Duration
	runner := PolicyRunner{
		Runtime: runtime,
		Policy:  BasicPolicy{MaxAttemptsPerTarget: 2, RetryRateLimits: true, Backoff: FixedBackoff{DelayValue: time.Hour}},
		Sleep: func(ctx context.Context, delay time.Duration) error {
			slept = delay
			return nil
		},
	}

	run, err := runner.StartRun(context.Background(), RunRequest{Provider: "openai", Model: "gpt"})
	if err != nil {
		t.Fatalf("StartRun error: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait error: %v", err)
	}
	if slept != time.Hour {
		t.Fatalf("sleep = %s, want configured backoff", slept)
	}
	if result.Metadata.Attempts[0].RateLimit == nil {
		t.Fatalf("first attempt missing rate-limit metadata")
	}
	if !hasCategory(collectEvents(run), EventRateLimit) {
		t.Fatalf("events missing rate-limit event")
	}
}

func TestPolicyRunnerCancellationDuringBackoff(t *testing.T) {
	retryErr := NewError(ErrorTimeout, "fake", "temporary timeout", nil)
	runtime := &scriptRuntime{results: []scriptResult{{err: retryErr}, {}}}
	sleepStarted := make(chan struct{})
	runner := PolicyRunner{
		Runtime: runtime,
		Policy:  BasicPolicy{MaxAttemptsPerTarget: 2, Backoff: FixedBackoff{DelayValue: time.Hour}},
		Sleep: func(ctx context.Context, delay time.Duration) error {
			close(sleepStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run, err := runner.StartRun(ctx, RunRequest{})
	if err != nil {
		t.Fatalf("StartRun error: %v", err)
	}
	<-sleepStarted
	cancel()
	result, err := run.Wait(context.Background())
	if err == nil {
		t.Fatalf("Wait error = nil, want cancellation")
	}
	if result.Status != StateFailed && result.Status != StateCancelled {
		t.Fatalf("status = %s, want failed or cancelled", result.Status)
	}
	if runtime.starts != 1 {
		t.Fatalf("starts = %d, want 1", runtime.starts)
	}
}

func TestPolicyRunnerDoesNotBlockOnNoisyRuntimeEvents(t *testing.T) {
	events := make([]Event, 128)
	for i := range events {
		events[i] = Event{Category: EventProgress, Type: "progress"}
	}
	runtime := &scriptRuntime{results: []scriptResult{{events: events}}}
	runner := PolicyRunner{
		Runtime: runtime,
		Policy:  BasicPolicy{},
		Sleep:   noSleep,
	}

	run, err := runner.StartRun(context.Background(), RunRequest{})
	if err != nil {
		t.Fatalf("StartRun error: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait error: %v", err)
	}
	if len(result.Metadata.Policy.DroppedEvents) == 0 {
		t.Fatalf("expected dropped event metadata for noisy runtime")
	}
}

func TestExponentialBackoffCapsDelay(t *testing.T) {
	backoff := ExponentialBackoff{Initial: time.Second, Factor: 2, Max: 3 * time.Second}
	delay := backoff.Delay(PolicyContext{AttemptOnTarget: 4})
	if delay != 3*time.Second {
		t.Fatalf("delay = %s, want 3s", delay)
	}
}

func TestBasicPolicyHonorsRateLimitRetryAfter(t *testing.T) {
	retryAfter := 9 * time.Second
	decision, err := BasicPolicy{
		MaxAttemptsPerTarget: 2,
		RetryRateLimits:      true,
		Backoff:              FixedBackoff{DelayValue: time.Hour},
	}.Decide(context.Background(), PolicyContext{
		AttemptOnTarget: 1,
		Err:             NewError(ErrorRateLimit, "fake", "rate limited", nil),
		RateLimit:       &RateLimitInfo{RetryAfter: retryAfter},
	})
	if err != nil {
		t.Fatalf("Decide error: %v", err)
	}
	if decision.Kind != PolicyDecisionRetry || decision.Delay != retryAfter {
		t.Fatalf("decision = %#v, want retry with retry-after", decision)
	}
}

func noSleep(context.Context, time.Duration) error { return nil }

func collectEvents(run Run) []Event {
	var events []Event
	for event := range run.Events() {
		events = append(events, event)
	}
	return events
}

func hasCategory(events []Event, category EventCategory) bool {
	for _, event := range events {
		if event.Category == category {
			return true
		}
	}
	return false
}

func hasDecision(decisions []PolicyDecisionRecord, kind PolicyDecisionKind) bool {
	for _, decision := range decisions {
		if decision.Kind == kind {
			return true
		}
	}
	return false
}

type scriptRuntime struct {
	name     string
	provider ProviderID
	model    ModelID
	results  []scriptResult
	starts   int
	mu       sync.Mutex
}

func (r *scriptRuntime) StartRun(ctx context.Context, req RunRequest) (Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.starts++
	index := r.starts - 1
	result := scriptResult{}
	if index < len(r.results) {
		result = r.results[index]
	}
	runID := RunID(fmt.Sprintf("%s-run-%d", r.runtimeName(), r.starts))
	provider := firstProvider(req.Provider, r.provider)
	model := firstModel(req.Model, r.model)
	return newScriptRun(runID, req, RuntimeContext{
		RuntimeKind: "script",
		RuntimeName: r.runtimeName(),
		Provider:    provider,
		Model:       model,
	}, result), nil
}

func (r *scriptRuntime) Capabilities(context.Context) (Capabilities, error) {
	return Capabilities{RuntimeKind: "script", Features: map[Capability]CapabilitySupport{}}, nil
}

func (r *scriptRuntime) runtimeName() string {
	if r.name != "" {
		return r.name
	}
	return "script"
}

type scriptResult struct {
	err    *SDKError
	events []Event
}

type scriptRun struct {
	id     RunID
	req    RunRequest
	ctx    RuntimeContext
	result scriptResult
	events chan Event
	done   chan struct{}
}

func newScriptRun(id RunID, req RunRequest, runtimeCtx RuntimeContext, result scriptResult) *scriptRun {
	run := &scriptRun{id: id, req: req, ctx: runtimeCtx, result: result, events: make(chan Event, len(result.events)), done: make(chan struct{})}
	go run.play()
	return run
}

func (r *scriptRun) ID() RunID { return r.id }

func (r *scriptRun) Events() <-chan Event { return r.events }

func (r *scriptRun) Wait(context.Context) (RunResult, error) {
	<-r.done
	status := StateCompleted
	if r.result.err != nil {
		status = StateFailed
	}
	now := time.Now().UTC()
	result := RunResult{
		RunID:      r.id,
		SessionID:  r.req.SessionID,
		TurnID:     r.req.TurnID,
		Status:     status,
		StartedAt:  now,
		FinishedAt: now,
		Err:        r.result.err,
	}
	result.Metadata = RunMetadata{
		Context:    r.ctx,
		Status:     status,
		StartedAt:  result.StartedAt,
		FinishedAt: result.FinishedAt,
		Session: SessionMetadata{
			ID:              r.req.SessionID,
			RequestedID:     r.req.SessionID,
			RequestedAction: r.req.SessionAction,
			Relationship:    SessionRelationshipFresh,
			Retained:        r.req.SessionID != "",
		},
	}
	if r.result.err != nil {
		result.Metadata.Errors = []SDKError{*r.result.err}
		return result, r.result.err
	}
	return result, nil
}

func (r *scriptRun) Cancel(context.Context) error { return nil }

func (r *scriptRun) play() {
	defer close(r.events)
	defer close(r.done)
	for _, event := range r.result.events {
		if event.RunID == "" {
			event.RunID = r.id
		}
		r.events <- event
	}
}

func firstProvider(a, b ProviderID) ProviderID {
	if a != "" {
		return a
	}
	return b
}

func firstModel(a, b ModelID) ModelID {
	if a != "" {
		return a
	}
	return b
}
