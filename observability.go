package agentwrap

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// RunRecord is an inspectable snapshot of active or completed runtime work.
//
// Identity, status, runtime context, timing, and latest-event fields are
// expected to be present for observed runs. Usage, cost, throughput, native
// metadata, runtime version, and provider/model facts remain best-effort:
// unknown numeric values must remain nil rather than being coerced to zero.
type RunRecord struct {
	RunID                  RunID
	ParentRunID            RunID
	SessionID              SessionID
	TurnID                 TurnID
	Status                 RunStatus
	Context                RuntimeContext
	EffectiveConfigSummary map[string]string
	StartedAt              time.Time
	FinishedAt             time.Time
	Duration               time.Duration
	ObservedAt             time.Time
	CompletedAt            time.Time
	LatestEvent            EventSummary
	EventCount             int64
	DroppedEventCount      int64
	SinkFailures           []SinkFailure
	StoreFailures          []SinkFailure
	Attempts               []AttemptSummary
	Policy                 PolicyMetadata
	Session                SessionMetadata
	Permissions            PermissionMetadata
	Cleanup                CleanupMetadata
	Validation             ValidationMetadata
	Repair                 RepairMetadata
	Artifacts              []ArtifactRef
	Warnings               []string
	Errors                 []SDKError
	Usage                  Usage
	EstimatedCost          *CostEstimate
	ThroughputTPS          *float64
	NativeMetadata         map[string]any
}

// EventSummary is a compact latest-event projection for dashboard lists.
type EventSummary struct {
	EventID EventID
	Time    time.Time
	Kind    EventKind
	Type    string
	Status  RunStatus
}

// RunEventRecord is the persistence-safe representation of one canonical event.
type RunEventRecord struct {
	RunID             RunID
	EventID           EventID
	Sequence          int64
	Time              time.Time
	Kind              EventKind
	Type              string
	Payload           EventPayload
	RawPresent        bool
	RawSafe           bool
	RawOmitted        bool
	RawOmissionReason string
	RawSource         string
	RawEncoding       string
	RawData           []byte
	ObservedAt        time.Time
	StoredAt          time.Time
}

// SinkFailure records an observer/store failure without replacing the primary
// runtime outcome unless the observer is configured as required.
type SinkFailure struct {
	Name      string
	Required  bool
	Operation string
	At        time.Time
	Error     SDKError
}

// PersistencePolicy controls what the observing wrapper may retain.
type PersistencePolicy struct {
	PersistUnsafeRawPayloads bool
}

// EventSink consumes ordered event records. Implementations should treat calls
// as context-bound and should not mutate records after AppendEvent returns.
type EventSink interface {
	AppendEvent(context.Context, RunEventRecord) error
}

// RunStore is the optional backend-neutral persistence and inspection contract.
type RunStore interface {
	UpsertRun(context.Context, RunRecord) error
	AppendEvent(context.Context, RunEventRecord) error
	ListActiveRuns(context.Context) ([]RunRecord, error)
	GetCompletedRun(context.Context, RunID) (RunRecord, bool, error)
	ListRunEvents(context.Context, RunID) ([]RunEventRecord, error)
}

// RunInspector exposes active/completed run views without exposing store internals.
type RunInspector interface {
	ListActiveRuns(context.Context) ([]RunRecord, error)
	GetCompletedRun(context.Context, RunID) (RunRecord, bool, error)
	ListRunEvents(context.Context, RunID) ([]RunEventRecord, error)
}

// ObservingRuntime wraps any Runtime with optional run projection, event sinks,
// and persistence hooks.
type ObservingRuntime struct {
	Runtime Runtime
	Store   RunStore
	Sinks   []NamedEventSink
	Policy  PersistencePolicy
	Now     func() time.Time
}

// NamedEventSink configures sink identity and required/best-effort behavior.
type NamedEventSink struct {
	Name     string
	Sink     EventSink
	Required bool
}

// StartRun starts the wrapped runtime and observes canonical events.
func (r ObservingRuntime) StartRun(ctx context.Context, req RunRequest) (Run, error) {
	if r.Runtime == nil {
		return nil, NewError(ErrorConfiguration, "observing runner", "runtime is required", nil)
	}
	inner, err := r.Runtime.StartRun(ctx, req)
	if err != nil {
		return nil, err
	}
	now := r.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := &observedRun{
		inner:  inner,
		store:  r.Store,
		sinks:  append([]NamedEventSink(nil), r.Sinks...),
		policy: r.Policy,
		now:    now,
		ctx:    runCtx,
		cancel: cancel,
		events: make(chan Event, 64),
		done:   make(chan struct{}),
		record: RunRecord{
			RunID:                  inner.ID(),
			ParentRunID:            reqParentRunID(req),
			SessionID:              req.SessionID,
			TurnID:                 req.TurnID,
			Status:                 StatusStarting,
			Context:                RuntimeContext{Provider: req.Provider, Model: req.Model},
			EffectiveConfigSummary: effectiveConfigSummary(req),
			StartedAt:              now(),
			ObservedAt:             now(),
		},
	}
	run.upsert(runCtx)
	go run.forward()
	return run, nil
}

// Capabilities reports wrapped runtime capabilities unchanged.
func (r ObservingRuntime) Capabilities(ctx context.Context) (Capabilities, error) {
	if r.Runtime == nil {
		return Capabilities{}, NewError(ErrorConfiguration, "observing runner capabilities", "runtime is required", nil)
	}
	return r.Runtime.Capabilities(ctx)
}

// ListActiveRuns implements RunInspector when a store is configured.
func (r ObservingRuntime) ListActiveRuns(ctx context.Context) ([]RunRecord, error) {
	if r.Store == nil {
		return nil, nil
	}
	return r.Store.ListActiveRuns(ctx)
}

// GetCompletedRun implements RunInspector when a store is configured.
func (r ObservingRuntime) GetCompletedRun(ctx context.Context, id RunID) (RunRecord, bool, error) {
	if r.Store == nil {
		return RunRecord{}, false, nil
	}
	return r.Store.GetCompletedRun(ctx, id)
}

// ListRunEvents implements RunInspector when a store is configured.
func (r ObservingRuntime) ListRunEvents(ctx context.Context, id RunID) ([]RunEventRecord, error) {
	if r.Store == nil {
		return nil, nil
	}
	return r.Store.ListRunEvents(ctx, id)
}

type observedRun struct {
	inner  Run
	store  RunStore
	sinks  []NamedEventSink
	policy PersistencePolicy
	now    func() time.Time
	ctx    context.Context
	cancel context.CancelFunc
	events chan Event
	done   chan struct{}
	seq    atomic.Int64

	mu      sync.Mutex
	record  RunRecord
	result  RunResult
	waitErr error
}

func (r *observedRun) ID() RunID { return r.inner.ID() }

func (r *observedRun) Events() <-chan Event { return r.events }

func (r *observedRun) Wait(ctx context.Context) (RunResult, error) {
	result, err := r.inner.Wait(ctx)
	<-r.done
	r.mu.Lock()
	r.result = result
	r.waitErr = mergeRequiredObserverError(err, r.record)
	r.mergeResultLocked(result)
	r.mu.Unlock()
	r.upsert(ctx)
	return result, r.waitErr
}

func (r *observedRun) Cancel(ctx context.Context) error {
	r.cancel()
	return r.inner.Cancel(ctx)
}

func (r *observedRun) forward() {
	defer close(r.done)
	defer close(r.events)
	for event := range r.inner.Events() {
		record := r.eventRecord(event)
		r.appendRecord(record)
		select {
		case <-r.ctx.Done():
			return
		case r.events <- event:
		default:
			r.recordDroppedEvent()
		}
	}
}

func (r *observedRun) eventRecord(event Event) RunEventRecord {
	observedAt := r.now()
	record := RunEventRecord{
		RunID:      event.RunID,
		EventID:    event.ID,
		Sequence:   r.seq.Add(1),
		Time:       event.Time,
		Kind:       event.Kind(),
		Type:       event.Type,
		Payload:    clonePayload(event.Payload),
		ObservedAt: observedAt,
		StoredAt:   observedAt,
	}
	if record.RunID == "" {
		record.RunID = r.ID()
	}
	if event.Raw != nil {
		record.RawPresent = true
		record.RawSafe = event.Raw.Safe
		record.RawSource = event.Raw.Source
		record.RawEncoding = event.Raw.Encoding
		if event.Raw.Safe || r.policy.PersistUnsafeRawPayloads {
			record.RawData = append([]byte(nil), event.Raw.Data...)
		} else {
			record.RawOmitted = true
			record.RawOmissionReason = "unsafe raw payload omitted by persistence policy"
		}
	}
	return record
}

func (r *observedRun) appendRecord(event RunEventRecord) {
	r.mu.Lock()
	r.applyEventLocked(event)
	r.mu.Unlock()
	for _, sink := range r.sinks {
		if sink.Sink == nil {
			continue
		}
		if err := sink.Sink.AppendEvent(r.ctx, event); err != nil {
			r.recordFailure("event_sink", sink.Name, sink.Required, err)
		}
	}
	if r.store != nil {
		if err := r.store.AppendEvent(r.ctx, event); err != nil {
			r.recordFailure("store_append_event", "store", false, err)
		}
	}
	r.upsert(r.ctx)
}

func (r *observedRun) applyEventLocked(event RunEventRecord) {
	now := r.now()
	r.record.ObservedAt = now
	r.record.EventCount++
	r.record.LatestEvent = EventSummary{
		EventID: event.EventID,
		Time:    event.Time,
		Kind:    event.Kind,
		Type:    event.Type,
	}
	if event.Time.IsZero() {
		r.record.LatestEvent.Time = now
	}
	if status, ok := statusFromPayload(event.Payload); ok {
		r.record.Status = status
		r.record.LatestEvent.Status = status
	}
	if event.Kind == EventFatalError && r.record.Status == "" {
		r.record.Status = StatusFailed
	}
	if event.Kind == EventUsage {
		if usage, ok := event.Payload["usage"].(Usage); ok {
			r.record.Usage = usage
		}
	}
	if event.Kind == EventArtifact {
		if artifact, ok := event.Payload["artifact"].(ArtifactRef); ok {
			r.record.Artifacts = append(r.record.Artifacts, withProducerMetadata(artifact, r.record))
		}
	}
}

func (r *observedRun) mergeResultLocked(result RunResult) {
	r.record.RunID = firstRunID(result.RunID, r.record.RunID)
	r.record.SessionID = result.SessionID
	r.record.TurnID = result.TurnID
	if result.Status != "" {
		r.record.Status = result.Status
	}
	if !r.record.Status.Terminal() && result.Err == nil && r.waitErr == nil && !result.FinishedAt.IsZero() {
		r.record.Status = StatusCompleted
	}
	r.record.StartedAt = firstTime(result.StartedAt, r.record.StartedAt)
	r.record.FinishedAt = result.FinishedAt
	if !result.FinishedAt.IsZero() {
		r.record.CompletedAt = r.now()
		r.record.Duration = result.FinishedAt.Sub(r.record.StartedAt)
	}
	metadata := result.Metadata
	r.record.Context = firstContext(metadata.Context, r.record.Context)
	r.record.ParentRunID = firstRunID(metadata.ParentRunID, r.record.ParentRunID)
	r.record.Attempts = append([]AttemptSummary(nil), metadata.Attempts...)
	r.record.Policy = metadata.Policy
	r.record.Session = metadata.Session
	r.record.Permissions = metadata.Permissions
	r.record.Cleanup = metadata.Cleanup
	r.record.Validation = metadata.Validation
	r.record.Repair = metadata.Repair
	r.record.Artifacts = mergeArtifacts(r.record.Artifacts, result.Artifacts, metadata.Artifacts, r.record)
	r.record.Warnings = append([]string(nil), result.Warnings...)
	r.record.Warnings = append(r.record.Warnings, metadata.Warnings...)
	r.record.Errors = append([]SDKError(nil), metadata.Errors...)
	if result.Err != nil {
		r.record.Errors = append(r.record.Errors, *result.Err)
	}
	r.record.Usage = firstUsage(metadata.Usage, result.Usage, r.record.Usage)
	r.record.EstimatedCost = metadata.EstimatedCost
	r.record.ThroughputTPS = metadata.ThroughputTPS
	r.record.NativeMetadata = cloneAnyMap(metadata.NativeMetadata)
}

func (r *observedRun) upsert(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.mu.Lock()
	record := cloneRunRecord(r.record)
	r.mu.Unlock()
	if err := r.store.UpsertRun(ctx, record); err != nil {
		r.recordFailure("store_upsert_run", "store", false, err)
	}
}

func (r *observedRun) recordFailure(operation, name string, required bool, err error) {
	failure := SinkFailure{Name: name, Required: required, Operation: operation, At: r.now(), Error: sdkErrorValue(err, operation)}
	r.mu.Lock()
	defer r.mu.Unlock()
	if operation == "store_append_event" || operation == "store_upsert_run" {
		r.record.StoreFailures = append(r.record.StoreFailures, failure)
	} else {
		r.record.SinkFailures = append(r.record.SinkFailures, failure)
	}
	r.record.Warnings = append(r.record.Warnings, fmt.Sprintf("%s %q failed: %s", operation, name, err.Error()))
}

func (r *observedRun) recordDroppedEvent() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.record.DroppedEventCount++
}

// MemoryRunStore is a deterministic in-memory reference RunStore.
type MemoryRunStore struct {
	mu        sync.Mutex
	active    map[RunID]RunRecord
	completed map[RunID]RunRecord
	events    map[RunID][]RunEventRecord
}

// NewMemoryRunStore constructs an empty in-memory store.
func NewMemoryRunStore() *MemoryRunStore {
	return &MemoryRunStore{
		active:    map[RunID]RunRecord{},
		completed: map[RunID]RunRecord{},
		events:    map[RunID][]RunEventRecord{},
	}
}

func (s *MemoryRunStore) UpsertRun(_ context.Context, record RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	record = cloneRunRecord(record)
	if record.Status.Terminal() {
		delete(s.active, record.RunID)
		s.completed[record.RunID] = record
		return nil
	}
	s.active[record.RunID] = record
	return nil
}

func (s *MemoryRunStore) AppendEvent(_ context.Context, event RunEventRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	s.events[event.RunID] = append(s.events[event.RunID], cloneEventRecord(event))
	return nil
}

func (s *MemoryRunStore) ListActiveRuns(_ context.Context) ([]RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	records := make([]RunRecord, 0, len(s.active))
	for _, record := range s.active {
		records = append(records, cloneRunRecord(record))
	}
	return records, nil
}

func (s *MemoryRunStore) GetCompletedRun(_ context.Context, id RunID) (RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	record, ok := s.completed[id]
	return cloneRunRecord(record), ok, nil
}

func (s *MemoryRunStore) ListRunEvents(_ context.Context, id RunID) ([]RunEventRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	events := s.events[id]
	out := make([]RunEventRecord, len(events))
	for i := range events {
		out[i] = cloneEventRecord(events[i])
	}
	return out, nil
}

func (s *MemoryRunStore) initLocked() {
	if s.active == nil {
		s.active = map[RunID]RunRecord{}
	}
	if s.completed == nil {
		s.completed = map[RunID]RunRecord{}
	}
	if s.events == nil {
		s.events = map[RunID][]RunEventRecord{}
	}
}

func mergeRequiredObserverError(primary error, record RunRecord) error {
	if primary != nil {
		return primary
	}
	for _, failure := range record.SinkFailures {
		if failure.Required {
			return &failure.Error
		}
	}
	for _, failure := range record.StoreFailures {
		if failure.Required {
			return &failure.Error
		}
	}
	return nil
}

func sdkErrorValue(err error, operation string) SDKError {
	var sdk *SDKError
	if errors.As(err, &sdk) && sdk != nil {
		return *sdk
	}
	return *NewError(ErrorUnknown, operation, err.Error(), err)
}

func reqParentRunID(req RunRequest) RunID {
	if req.Metadata == nil {
		return ""
	}
	return RunID(req.Metadata["parent_run_id"])
}

func effectiveConfigSummary(req RunRequest) map[string]string {
	summary := map[string]string{}
	if req.WorkDir != "" {
		summary["workdir"] = req.WorkDir
	}
	if req.Provider != "" {
		summary["provider"] = string(req.Provider)
	}
	if req.Model != "" {
		summary["model"] = string(req.Model)
	}
	if req.Permissions != "" {
		summary["permissions"] = string(req.Permissions)
	}
	if req.Sandbox != "" {
		summary["sandbox"] = string(req.Sandbox)
	}
	if req.Timeout > 0 {
		summary["timeout"] = req.Timeout.String()
	}
	if req.SessionAction != "" {
		summary["session_action"] = string(req.SessionAction)
	}
	if len(req.Metadata) > 0 {
		summary["metadata_keys"] = fmt.Sprintf("%d", len(req.Metadata))
	}
	if len(summary) == 0 {
		return nil
	}
	return summary
}

func statusFromPayload(payload EventPayload) (RunStatus, bool) {
	for _, key := range []string{"state", "status", "to"} {
		if status, ok := coerceStatus(payload[key]); ok {
			return status, true
		}
	}
	return "", false
}

func coerceStatus(value any) (RunStatus, bool) {
	switch v := value.(type) {
	case RunStatus:
		return v, true
	case string:
		status := RunStatus(v)
		switch status {
		case StatusStarting, StatusRunning, StatusValidating, StatusRepairing, StatusCompleted, StatusFailed, StatusCancelled:
			return status, true
		}
	}
	return "", false
}

func withProducerMetadata(artifact ArtifactRef, record RunRecord) ArtifactRef {
	if artifact.Metadata == nil {
		artifact.Metadata = map[string]string{}
	} else {
		artifact.Metadata = cloneStringMap(artifact.Metadata)
	}
	if record.RunID != "" {
		artifact.Metadata["producer_run_id"] = string(record.RunID)
	}
	if record.Context.RuntimeKind != "" {
		artifact.Metadata["producer_runtime_kind"] = string(record.Context.RuntimeKind)
	}
	if record.Context.RuntimeName != "" {
		artifact.Metadata["producer_runtime_name"] = record.Context.RuntimeName
	}
	if record.Context.Provider != "" {
		artifact.Metadata["producer_provider"] = string(record.Context.Provider)
	}
	if record.Context.Model != "" {
		artifact.Metadata["producer_model"] = string(record.Context.Model)
	}
	return artifact
}

func mergeArtifacts(existingArtifacts, resultArtifacts, metadataArtifacts []ArtifactRef, record RunRecord) []ArtifactRef {
	var merged []ArtifactRef
	seen := map[ArtifactID]bool{}
	for _, artifact := range append(append(append([]ArtifactRef(nil), existingArtifacts...), resultArtifacts...), metadataArtifacts...) {
		if artifact.ID != "" && seen[artifact.ID] {
			continue
		}
		if artifact.ID != "" {
			seen[artifact.ID] = true
		}
		merged = append(merged, withProducerMetadata(artifact, record))
	}
	return merged
}

func firstRunID(primary, fallback RunID) RunID {
	if primary != "" {
		return primary
	}
	return fallback
}

func firstContext(primary, fallback RuntimeContext) RuntimeContext {
	if primary.RuntimeKind != "" || primary.RuntimeName != "" || primary.Provider != "" || primary.Model != "" {
		return primary
	}
	return fallback
}

func firstUsage(values ...Usage) Usage {
	for _, usage := range values {
		if usage.InputTokens != nil || usage.OutputTokens != nil || usage.TotalTokens != nil || len(usage.Native) > 0 {
			return usage
		}
	}
	return Usage{}
}

func cloneRunRecord(record RunRecord) RunRecord {
	record.EffectiveConfigSummary = cloneStringMap(record.EffectiveConfigSummary)
	record.SinkFailures = append([]SinkFailure(nil), record.SinkFailures...)
	record.StoreFailures = append([]SinkFailure(nil), record.StoreFailures...)
	record.Attempts = append([]AttemptSummary(nil), record.Attempts...)
	record.Artifacts = append([]ArtifactRef(nil), record.Artifacts...)
	for i := range record.Artifacts {
		record.Artifacts[i].Metadata = cloneStringMap(record.Artifacts[i].Metadata)
	}
	record.Warnings = append([]string(nil), record.Warnings...)
	record.Errors = append([]SDKError(nil), record.Errors...)
	record.NativeMetadata = cloneAnyMap(record.NativeMetadata)
	record.Usage.Native = cloneAnyMap(record.Usage.Native)
	return record
}

func cloneEventRecord(record RunEventRecord) RunEventRecord {
	record.Payload = clonePayload(record.Payload)
	record.RawData = append([]byte(nil), record.RawData...)
	return record
}

func clonePayload(src EventPayload) EventPayload {
	if len(src) == 0 {
		return nil
	}
	dst := make(EventPayload, len(src))
	for key, value := range src {
		dst[key] = clonePayloadValue(value)
	}
	return dst
}

func clonePayloadValue(value any) any {
	if value == nil {
		return nil
	}
	cloned := cloneReflectValue(reflect.ValueOf(value))
	if !cloned.IsValid() {
		return nil
	}
	return cloned.Interface()
}

func cloneReflectValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneReflectValue(value.Elem())
		if value.Kind() == reflect.Interface {
			return cloned
		}
		ptr := reflect.New(value.Type().Elem())
		ptr.Elem().Set(cloned)
		return ptr
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			cloned.SetMapIndex(cloneReflectValue(iter.Key()), cloneReflectValue(iter.Value()))
		}
		return cloned
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			cloned.Index(i).Set(cloneReflectValue(value.Index(i)))
		}
		return cloned
	case reflect.Array:
		cloned := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			cloned.Index(i).Set(cloneReflectValue(value.Index(i)))
		}
		return cloned
	case reflect.Struct:
		cloned := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			if !cloned.Field(i).CanSet() {
				return value
			}
			cloned.Field(i).Set(cloneReflectValue(value.Field(i)))
		}
		return cloned
	default:
		return value
	}
}
