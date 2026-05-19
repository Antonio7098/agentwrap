package testkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	agentwrap "github.com/antonioborgerees/agentwrap"
)

// FakeRuntime proves the public contract without launching a real runtime.
type FakeRuntime struct {
	Kind     agentwrap.RuntimeKind
	Provider agentwrap.ProviderID
	Model    agentwrap.ModelID
	Script   []agentwrap.Event
	Err      *agentwrap.SDKError
}

var _ agentwrap.Runtime = (*FakeRuntime)(nil)

func (f *FakeRuntime) Capabilities(context.Context) (agentwrap.Capabilities, error) {
	features := map[agentwrap.Capability]agentwrap.CapabilitySupport{
		agentwrap.CapabilitySessions:         {Supported: true, Detail: "fake retained session metadata"},
		agentwrap.CapabilityCancellation:     {Supported: true, Detail: "fake cancellation state"},
		agentwrap.CapabilityStructuredEvents: {Supported: true, Detail: "deterministic canonical events"},
		agentwrap.CapabilityRawPayloads:      {Supported: true, Detail: "raw fixture payload preservation"},
		agentwrap.CapabilityArtifacts:        {Supported: true, Detail: "artifact references only"},
		agentwrap.CapabilityPermissions:      {Supported: false, Detail: "permission policy is deferred"},
		agentwrap.CapabilityUsage:            {Supported: true, Detail: "placeholder usage metadata"},
		agentwrap.CapabilityValidationEvents: {Supported: true, Detail: "validation category only"},
	}
	return agentwrap.Capabilities{
		RuntimeKind: f.runtimeKind(),
		Features:    features,
		Unsupported: []agentwrap.UnsupportedCapability{{Capability: agentwrap.CapabilityPermissions, Reason: "deferred to permission sprint"}},
	}, nil
}

func (f *FakeRuntime) StartRun(ctx context.Context, req agentwrap.RunRequest) (agentwrap.Run, error) {
	caps, err := f.Capabilities(ctx)
	if err != nil {
		return nil, err
	}
	for _, capability := range req.RequireCaps {
		if !caps.Supports(capability) {
			return nil, agentwrap.NewError(agentwrap.ErrorConfiguration, "fake.StartRun", fmt.Sprintf("unsupported capability %s", capability), nil)
		}
	}
	run := &fakeRun{
		id:        agentwrap.RunID("fake-run-1"),
		sessionID: req.SessionID,
		turnID:    req.TurnID,
		context: agentwrap.RuntimeContext{
			RuntimeKind: f.runtimeKind(),
			RuntimeName: "fake",
			Provider:    f.Provider,
			Model:       f.Model,
		},
		script: append([]agentwrap.Event(nil), f.Script...),
		err:    f.Err,
		events: make(chan agentwrap.Event),
		cancel: make(chan struct{}),
		done:   make(chan struct{}),
		start:  time.Now().UTC(),
	}
	if run.sessionID == "" && req.WantSession {
		run.sessionID = agentwrap.SessionID("fake-session-1")
	}
	if run.turnID == "" {
		run.turnID = agentwrap.TurnID("fake-turn-1")
	}
	go run.play(ctx)
	return run, nil
}

func (f *FakeRuntime) runtimeKind() agentwrap.RuntimeKind {
	if f.Kind == "" {
		return agentwrap.RuntimeKind("fake")
	}
	return f.Kind
}

type fakeRun struct {
	id         agentwrap.RunID
	sessionID  agentwrap.SessionID
	turnID     agentwrap.TurnID
	context    agentwrap.RuntimeContext
	script     []agentwrap.Event
	err        *agentwrap.SDKError
	events     chan agentwrap.Event
	cancel     chan struct{}
	cancelOnce sync.Once
	done       chan struct{}
	start      time.Time
	finish     time.Time
	status     agentwrap.RunStatus
	artifacts  []agentwrap.ArtifactRef
	usage      agentwrap.Usage
	mu         sync.Mutex
}

func (r *fakeRun) ID() agentwrap.RunID {
	return r.id
}

func (r *fakeRun) Events() <-chan agentwrap.Event {
	return r.events
}

func (r *fakeRun) Wait(ctx context.Context) (agentwrap.RunResult, error) {
	select {
	case <-ctx.Done():
		return agentwrap.RunResult{}, agentwrap.NewError(agentwrap.ErrorTimeout, "fake.Wait", "wait context ended", ctx.Err())
	case <-r.done:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := agentwrap.RunResult{
		RunID:      r.id,
		SessionID:  r.sessionID,
		TurnID:     r.turnID,
		Status:     r.status,
		StartedAt:  r.start,
		FinishedAt: r.finish,
		Artifacts:  append([]agentwrap.ArtifactRef(nil), r.artifacts...),
		Usage:      r.usage,
		Err:        r.err,
	}
	result.Metadata = agentwrap.RunMetadata{
		Context:    r.context,
		Status:     r.status,
		StartedAt:  r.start,
		FinishedAt: r.finish,
		Duration:   r.finish.Sub(r.start),
		Session: agentwrap.SessionMetadata{
			ID:       r.sessionID,
			Retained: r.sessionID != "",
		},
		Artifacts: result.Artifacts,
		Usage:     r.usage,
	}
	if r.err != nil {
		result.Metadata.Errors = []agentwrap.SDKError{*r.err}
		return result, r.err
	}
	return result, nil
}

func (r *fakeRun) Cancel(context.Context) error {
	r.mu.Lock()
	if r.status.Terminal() {
		r.mu.Unlock()
		return nil
	}
	r.status = agentwrap.StatusCancelled
	r.err = &agentwrap.SDKError{
		Category:   agentwrap.ErrorCancellation,
		Operation:  "fake.Cancel",
		UserDetail: "run cancelled",
	}
	r.mu.Unlock()
	r.cancelOnce.Do(func() { close(r.cancel) })
	return nil
}

func (r *fakeRun) play(ctx context.Context) {
	defer close(r.done)
	defer close(r.events)
	for i, event := range r.script {
		select {
		case <-ctx.Done():
			r.markCancelled(ctx.Err())
			return
		case <-r.cancel:
			r.markCancelled(r.err)
			return
		default:
		}
		event.ID = agentwrap.EventID(fmt.Sprintf("fake-event-%d", i+1))
		event.RunID = r.id
		event.SessionID = r.sessionID
		if event.Payload == nil {
			event.Payload = agentwrap.EventPayload{}
		}
		event.Payload["turn_id"] = string(r.turnID)
		event.Payload["context"] = r.context
		if event.Time.IsZero() {
			event.Time = r.start.Add(time.Duration(i) * time.Millisecond)
		}
		r.observe(event)
		select {
		case <-ctx.Done():
			r.markCancelled(ctx.Err())
			return
		case <-r.cancel:
			r.markCancelled(r.err)
			return
		case r.events <- event:
		}
	}
	r.mu.Lock()
	if r.status == "" {
		r.status = agentwrap.StatusCompleted
	}
	r.finish = time.Now().UTC()
	r.mu.Unlock()
}

func (r *fakeRun) observe(event agentwrap.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if event.Kind() == agentwrap.EventLifecycle {
		if state, ok := event.Payload["state"].(agentwrap.RunStatus); ok {
			r.status = state
		}
		if state, ok := event.Payload["state"].(string); ok {
			r.status = agentwrap.RunStatus(state)
		}
	}
	if event.Kind() == agentwrap.EventArtifact {
		if artifact, ok := event.Payload["artifact"].(agentwrap.ArtifactRef); ok {
			r.artifacts = append(r.artifacts, artifact)
		}
	}
	if event.Kind() == agentwrap.EventUsage {
		if usage, ok := event.Payload["usage"].(agentwrap.Usage); ok {
			r.usage = usage
		}
	}
	if event.Kind() == agentwrap.EventFatalError && r.err == nil {
		r.status = agentwrap.StatusFailed
		r.err = agentwrap.NewError(agentwrap.ErrorRuntimeExit, "fake.play", "fake fatal event", nil)
	}
}

func (r *fakeRun) markCancelled(cause error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = agentwrap.StatusCancelled
	r.finish = time.Now().UTC()
	r.err = agentwrap.NewError(agentwrap.ErrorCancellation, "fake.play", "context cancelled", cause)
}
