package opencode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antonioborgerees/agentwrap"
)

var runCounter atomic.Int64

// StartRun launches OpenCode in JSON event mode and returns a streaming run
// handle. The returned run owns the subprocess and event decoding state.
func (r *Runtime) StartRun(ctx context.Context, req agentwrap.RunRequest) (agentwrap.Run, error) {
	if err := validateSessionRequest(req); err != nil {
		return nil, err
	}
	if err := r.requiredPreflight(ctx, req); err != nil {
		return nil, err
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if req.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, req.Timeout)
	} else {
		runCtx, cancel = context.WithCancel(ctx)
	}
	runID := agentwrap.RunID(fmt.Sprintf("opencode-%d", runCounter.Add(1)))
	started := r.now()
	spec := r.processSpec(req)
	proc, err := r.runner.Start(runCtx, spec)
	if err != nil {
		cancel()
		return nil, classifyStartError(err)
	}
	handle := &run{
		id:      runID,
		req:     req,
		ctx:     runCtx,
		cancel:  cancel,
		proc:    proc,
		events:  make(chan agentwrap.Event, 32),
		done:    make(chan struct{}),
		started: started,
		context: agentwrap.RuntimeContext{
			RuntimeKind: agentwrap.RuntimeKind("opencode"),
			RuntimeName: "opencode",
			Provider:    req.Provider,
			Model:       req.Model,
		},
		now:          r.now,
		stderrBuffer: newLimitBuffer(r.stderrLimit),
		stderrDone:   make(chan struct{}),
		lifecycle:    agentwrap.StateInitialized,
	}
	handle.emitLifecycle(agentwrap.StateRunning, "process_started")
	go handle.captureStderr()
	go handle.cancelOnContextDone()
	go handle.run()
	return handle, nil
}

func (r *Runtime) requiredPreflight(ctx context.Context, req agentwrap.RunRequest) error {
	if len(req.RequireHealth) == 0 {
		return nil
	}
	report, err := r.CheckHealth(ctx, agentwrap.HealthCheckRequest{
		Context: agentwrap.RuntimeContext{
			RuntimeKind: agentwrap.RuntimeKind("opencode"),
			RuntimeName: "opencode",
			Provider:    req.Provider,
			Model:       req.Model,
		},
		WorkDir:        req.WorkDir,
		Provider:       req.Provider,
		Model:          req.Model,
		Permissions:    req.Permissions,
		Sandbox:        req.Sandbox,
		Timeout:        req.Timeout,
		Metadata:       req.Metadata,
		Checks:         req.RequireHealth,
		RequiredChecks: req.RequireHealth,
	})
	if err != nil {
		return err
	}
	if failure := agentwrap.RequiredHealthFailure(report, req.RequireHealth); failure != nil {
		return failure
	}
	return nil
}

func (r *Runtime) processSpec(req agentwrap.RunRequest) processSpec {
	args := []string{"run", "--format", "json"}
	if req.WorkDir != "" {
		args = append(args, "--dir", req.WorkDir)
	}
	if req.Provider != "" || req.Model != "" {
		model := string(req.Model)
		if req.Provider != "" && !strings.Contains(model, "/") {
			model = string(req.Provider) + "/" + model
		}
		if model != "" {
			args = append(args, "--model", model)
		}
	}
	if req.SessionID != "" {
		args = append(args, "--session", string(req.SessionID))
	}
	args = append(args, r.extraArgs...)
	args = append(args, req.Prompt)
	return processSpec{
		Executable: r.executable,
		Args:       args,
		Env:        r.env,
		WorkDir:    req.WorkDir,
	}
}

type run struct {
	id     agentwrap.RunID
	req    agentwrap.RunRequest
	ctx    context.Context
	cancel context.CancelFunc
	proc   process

	events chan agentwrap.Event
	done   chan struct{}

	mu            sync.Mutex
	result        agentwrap.RunResult
	waitErr       error
	cleanupOnce   sync.Once
	cleanupResult agentwrap.CleanupMetadata
	lifecycle     agentwrap.LifecycleState
	started       time.Time
	finished      time.Time

	context      agentwrap.RuntimeContext
	eventMu      sync.Mutex
	seq          int64
	sawFinal     bool
	sessionID    agentwrap.SessionID
	artifacts    []agentwrap.ArtifactRef
	warnings     []string
	usage        agentwrap.Usage
	nativeTypes  map[string]int
	categories   map[string]int
	stderrBuffer *limitBuffer
	stderrDone   chan struct{}
	now          clock
}

func (r *run) ID() agentwrap.RunID            { return r.id }
func (r *run) Events() <-chan agentwrap.Event { return r.events }

func (r *run) Wait(ctx context.Context) (agentwrap.RunResult, error) {
	select {
	case <-r.done:
	case <-ctx.Done():
		return agentwrap.RunResult{}, classifyContextError(ctx.Err(), "opencode wait")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.result, r.waitErr
}

func (r *run) Cancel(ctx context.Context) error {
	r.emitLifecycle(agentwrap.StateCancelled, "caller_cancel")
	cleanup := r.cleanup(ctx, "caller_cancel")
	r.cancel()
	if cleanup.Error != nil {
		return cleanup.Error
	}
	return nil
}

func (r *run) captureStderr() {
	defer close(r.stderrDone)
	_, _ = io.Copy(r.stderrBuffer, r.proc.Stderr())
}

func (r *run) cancelOnContextDone() {
	<-r.ctx.Done()
	if r.currentLifecycle().Terminal() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.cleanup(ctx, "context_done")
}

func (r *run) run() {
	defer close(r.events)
	defer close(r.done)
	defer r.cancel()

	decodeErr := scanNativeRecords(r.ctx, r.proc.Stdout(), func(record nativeRecord) error {
		if r.updateSessionID(observedSessionID(record)) {
			r.emitSession()
		}
		seq := r.nextSequence()
		projected := projectNative(projectionInput{
			runID:  r.id,
			turnID: r.req.TurnID,
			ctx:    r.context,
			seq:    seq,
			now:    r.now(),
			record: record,
		})
		if projected.event.SessionID == "" {
			projected.event.SessionID = r.req.SessionID
		}
		r.recordEventStats(projected.event)
		if !r.sendEvent(projected.event) {
			return r.ctx.Err()
		}
		if projected.final {
			r.sawFinal = true
		}
		if projected.usage.Native != nil || projected.usage.InputTokens != nil || projected.usage.OutputTokens != nil || projected.usage.TotalTokens != nil {
			r.usage = projected.usage
		}
		r.artifacts = append(r.artifacts, projected.artifacts...)
		r.warnings = append(r.warnings, projected.warnings...)
		if projected.fatal != nil {
			return projected.fatal
		}
		return nil
	})
	processResult := r.proc.Wait()
	<-r.stderrDone
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cleanupCancel()
	cleanup := r.cleanup(cleanupCtx, "run_finished")
	r.finished = r.now()
	result, err := r.finalResult(decodeErr, processResult, cleanup)
	r.mu.Lock()
	r.result = result
	r.waitErr = err
	r.mu.Unlock()
}

func (r *run) finalResult(decodeErr error, proc processResult, cleanup agentwrap.CleanupMetadata) (agentwrap.RunResult, error) {
	status := agentwrap.StateCompleted
	var sdkErr *agentwrap.SDKError
	if decodeErr != nil {
		if errors.Is(decodeErr, context.Canceled) || errors.Is(decodeErr, context.DeadlineExceeded) {
			sdkErr = classifyContextError(decodeErr, "opencode run")
			if sdkErr.Category == agentwrap.ErrorCancellation {
				status = agentwrap.StateCancelled
			} else {
				status = agentwrap.StateFailed
			}
		} else {
			var already *agentwrap.SDKError
			if errors.As(decodeErr, &already) {
				sdkErr = already
			} else {
				sdkErr = classifyDecodeError(decodeErr)
			}
			status = agentwrap.StateFailed
		}
	} else if err := r.ctx.Err(); err != nil {
		sdkErr = classifyContextError(err, "opencode run")
		if sdkErr.Category == agentwrap.ErrorCancellation {
			status = agentwrap.StateCancelled
		} else {
			status = agentwrap.StateFailed
		}
	} else if proc.Err != nil || proc.ExitCode != 0 {
		sdkErr = classifyExitError(proc, r.stderrBuffer.String())
		status = agentwrap.StateFailed
	} else if !r.sawFinal {
		sdkErr = agentwrap.NewError(agentwrap.ErrorRuntimeExit, "opencode run", "OpenCode finished without a final structured result", nil, agentwrap.WithDebugDetail(debugDetail(r.stderrBuffer.String())))
		status = agentwrap.StateFailed
	}
	metadata := agentwrap.RunMetadata{
		Context:    r.context,
		Status:     status,
		StartedAt:  r.started,
		FinishedAt: r.finished,
		Duration:   r.finished.Sub(r.started),
		Session:    sessionMetadata(r.req, r.sessionID),
		Cleanup:    cleanup,
		Artifacts:  r.artifacts,
		Warnings:   r.warnings,
		Usage:      r.usage,
		NativeMetadata: map[string]any{
			"stderr":                 r.stderrBuffer.String(),
			"exit_code":              proc.ExitCode,
			"event_count":            r.seq,
			"event_categories":       copyStringIntMap(r.categories),
			"native_event_types":     copyStringIntMap(r.nativeTypes),
			"native_extension_count": r.categories[string(agentwrap.EventNativeExtension)],
		},
	}
	if sdkErr != nil && sdkErr.Category == agentwrap.ErrorRateLimit {
		if info := classifyRateLimitText("opencode run", r.stderrBuffer.String(), r.context); info != nil && info.info != nil {
			metadata.NativeMetadata["rate_limit_info"] = info.info
		}
	}
	if sdkErr != nil {
		metadata.Errors = []agentwrap.SDKError{*sdkErr}
	}
	if cleanup.Error != nil {
		metadata.Errors = append(metadata.Errors, *cleanup.Error)
	}
	result := agentwrap.RunResult{
		RunID:      r.id,
		SessionID:  firstSessionID(r.sessionID, r.req.SessionID),
		TurnID:     r.req.TurnID,
		Status:     status,
		Metadata:   metadata,
		Artifacts:  r.artifacts,
		Warnings:   r.warnings,
		Usage:      r.usage,
		StartedAt:  r.started,
		FinishedAt: r.finished,
		Err:        sdkErr,
	}
	if sdkErr != nil {
		return result, sdkErr
	}
	if cleanup.Error != nil {
		return result, nil
	}
	return result, nil
}

func (r *run) cleanup(ctx context.Context, reason string) agentwrap.CleanupMetadata {
	r.cleanupOnce.Do(func() {
		procCleanup := r.proc.Cancel(ctx)
		r.cleanupResult = agentwrap.CleanupMetadata{Attempted: true, Completed: procCleanup.Err == nil, Failed: procCleanup.Err != nil}
		if procCleanup.Err != nil {
			r.cleanupResult.Error = agentwrap.NewError(agentwrap.ErrorCleanup, "opencode cleanup", "OpenCode cleanup failed", procCleanup.Err, agentwrap.WithDebugDetail(procCleanup.Err.Error()))
			r.emitLifecycle(agentwrap.StateFailed, "cleanup_failed")
			return
		}
		r.emitLifecycle(agentwrap.StateCleanedUp, reason)
	})
	return r.cleanupResult
}

func (r *run) emitLifecycle(to agentwrap.LifecycleState, reason string) {
	seq, from, ok := r.transitionLifecycle(to)
	if !ok {
		return
	}
	event := agentwrap.LifecycleEvent(r.id, r.req.SessionID, r.req.TurnID, r.context, seq, r.now(), from, to, reason)
	if r.sendLocalEvent(event) {
		r.recordEventStats(event)
	}
}

func (r *run) transitionLifecycle(to agentwrap.LifecycleState) (int64, agentwrap.LifecycleState, bool) {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	from := r.lifecycle
	if from == to || from == agentwrap.StateCleanedUp || from == agentwrap.StateFailed {
		return 0, from, false
	}
	if from == agentwrap.StateCancelled && to != agentwrap.StateCleanedUp && to != agentwrap.StateFailed {
		return 0, from, false
	}
	r.seq++
	r.lifecycle = to
	return r.seq, from, true
}

func (r *run) currentLifecycle() agentwrap.LifecycleState {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	return r.lifecycle
}

func (r *run) emitSession() {
	seq := r.nextSequence()
	sessionID := firstSessionID(r.sessionID, r.req.SessionID)
	event := agentwrap.SessionEvent(r.id, sessionID, r.req.TurnID, r.context, seq, r.now(), sessionMetadata(r.req, r.sessionID))
	if r.sendLocalEvent(event) {
		r.recordEventStats(event)
	}
}

func (r *run) nextSequence() int64 {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	r.seq++
	return r.seq
}

func (r *run) updateSessionID(sessionID agentwrap.SessionID) bool {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	if sessionID == "" || r.sessionID == sessionID {
		return false
	}
	r.sessionID = sessionID
	return true
}

func observedSessionID(record nativeRecord) agentwrap.SessionID {
	return firstSessionID(
		agentwrap.SessionID(record.SessionID),
		agentwrap.SessionID(stringValue(record.Data["sessionID"])),
		agentwrap.SessionID(stringValue(record.Data["session_id"])),
	)
}

func (r *run) sendEvent(event agentwrap.Event) bool {
	defer func() {
		_ = recover()
	}()
	select {
	case <-r.ctx.Done():
		return false
	case r.events <- event:
		return true
	}
}

func (r *run) sendLocalEvent(event agentwrap.Event) bool {
	defer func() {
		_ = recover()
	}()
	select {
	case r.events <- event:
		return true
	default:
		return false
	}
}

func (r *run) recordEventStats(event agentwrap.Event) {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	if r.nativeTypes == nil {
		r.nativeTypes = make(map[string]int)
	}
	if r.categories == nil {
		r.categories = make(map[string]int)
	}
	r.categories[string(event.Category)]++
	if event.Type != "" {
		r.nativeTypes[event.Type]++
	}
}

func copyStringIntMap(src map[string]int) map[string]int {
	if len(src) == 0 {
		return map[string]int{}
	}
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func classifyStartError(err error) *agentwrap.SDKError {
	return agentwrap.NewError(agentwrap.ErrorRuntimeUnavailable, "opencode start", "OpenCode could not be started", err, agentwrap.WithDebugDetail(err.Error()))
}

func classifyDecodeError(err error) *agentwrap.SDKError {
	var d *decodeError
	if errors.As(err, &d) {
		return agentwrap.NewError(agentwrap.ErrorMalformedEvent, "opencode decode", "OpenCode emitted malformed structured output", err, agentwrap.WithDebugDetail(fmt.Sprintf("line=%d raw=%q error=%v", d.line, string(d.raw), d.err)))
	}
	return agentwrap.NewError(agentwrap.ErrorMalformedEvent, "opencode decode", "OpenCode emitted malformed structured output", err, agentwrap.WithDebugDetail(err.Error()))
}

func classifyExitError(result processResult, stderr string) *agentwrap.SDKError {
	if classified := classifyRateLimitText("opencode run", stderr, agentwrap.RuntimeContext{}); classified != nil {
		return classified.err
	}
	return agentwrap.NewError(agentwrap.ErrorRuntimeExit, "opencode run", "OpenCode exited before a successful final result", result.Err, agentwrap.WithDebugDetail(fmt.Sprintf("exit_code=%d stderr=%s", result.ExitCode, debugDetail(stderr))))
}

func classifyContextError(err error, op string) *agentwrap.SDKError {
	if errors.Is(err, context.DeadlineExceeded) {
		return agentwrap.NewError(agentwrap.ErrorTimeout, op, "OpenCode run timed out", err)
	}
	return agentwrap.NewError(agentwrap.ErrorCancellation, op, "OpenCode run was cancelled", err)
}

func debugDetail(stderr string) string {
	if strings.TrimSpace(stderr) == "" {
		return ""
	}
	return stderr
}

func validateSessionRequest(req agentwrap.RunRequest) error {
	switch req.SessionAction {
	case agentwrap.SessionActionDefault, agentwrap.SessionActionFresh, agentwrap.SessionActionContinue:
		return nil
	case agentwrap.SessionActionFork:
		return unsupportedSessionAction(agentwrap.CapabilitySessionFork, "OpenCode adapter does not support session fork")
	case agentwrap.SessionActionReplace:
		return unsupportedSessionAction(agentwrap.CapabilitySessionReplace, "OpenCode adapter does not support session replace")
	case agentwrap.SessionActionRelease:
		return unsupportedSessionAction(agentwrap.CapabilitySessionRelease, "OpenCode adapter does not support session release")
	default:
		return unsupportedSessionAction(agentwrap.CapabilitySessions, fmt.Sprintf("unsupported session action %q", req.SessionAction))
	}
}

func unsupportedSessionAction(capability agentwrap.Capability, reason string) error {
	return agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode session", reason, nil, agentwrap.WithDebugDetail(string(capability)))
}

func sessionMetadata(req agentwrap.RunRequest, observedID agentwrap.SessionID) agentwrap.SessionMetadata {
	action := req.SessionAction
	if action == agentwrap.SessionActionDefault {
		switch {
		case req.SessionID != "":
			action = agentwrap.SessionActionContinue
		case req.WantSession:
			action = agentwrap.SessionActionFresh
		}
	}
	metadata := agentwrap.SessionMetadata{
		ID:              firstSessionID(observedID, req.SessionID),
		RequestedID:     req.SessionID,
		RequestedAction: action,
		Retained:        req.WantSession || req.SessionID != "",
	}
	switch action {
	case agentwrap.SessionActionFresh:
		metadata.Relationship = agentwrap.SessionRelationshipFresh
	case agentwrap.SessionActionContinue:
		metadata.Relationship = agentwrap.SessionRelationshipBestEffort
		metadata.Continued = req.SessionID != ""
		metadata.BestEffort = true
		metadata.UnsupportedReason = "OpenCode --session continuation is passed through but not verified as durable retention"
	case agentwrap.SessionActionFork, agentwrap.SessionActionReplace, agentwrap.SessionActionRelease:
		metadata.Relationship = agentwrap.SessionRelationshipUnsupported
		metadata.UnsupportedReason = "retained-session action is unsupported by OpenCode adapter"
		metadata.Unsupported = []agentwrap.UnsupportedCapability{{Capability: agentwrap.CapabilitySessions, Reason: metadata.UnsupportedReason}}
	default:
		if req.WantSession || req.SessionID != "" {
			metadata.Relationship = agentwrap.SessionRelationshipBestEffort
			metadata.BestEffort = true
			metadata.UnsupportedReason = "full retained-session lifecycle is unsupported by OpenCode adapter"
		}
	}
	return metadata
}
