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
	}
	go handle.captureStderr()
	go handle.cancelOnContextDone()
	go handle.run()
	return handle, nil
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

	mu       sync.Mutex
	result   agentwrap.RunResult
	waitErr  error
	started  time.Time
	finished time.Time

	context      agentwrap.RuntimeContext
	seq          int64
	sawFinal     bool
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
	r.cancel()
	if err := r.proc.Cancel(ctx); err != nil {
		return agentwrap.NewError(agentwrap.ErrorCancellation, "opencode cancel", "OpenCode cancellation failed", err, agentwrap.WithDebugDetail(err.Error()))
	}
	return nil
}

func (r *run) captureStderr() {
	defer close(r.stderrDone)
	_, _ = io.Copy(r.stderrBuffer, r.proc.Stderr())
}

func (r *run) cancelOnContextDone() {
	<-r.ctx.Done()
	_ = r.proc.Cancel(context.Background())
}

func (r *run) run() {
	defer close(r.events)
	defer close(r.done)
	defer r.cancel()

	decodeErr := scanNativeRecords(r.proc.Stdout(), func(record nativeRecord) error {
		r.seq++
		projected := projectNative(projectionInput{
			runID:  r.id,
			turnID: r.req.TurnID,
			ctx:    r.context,
			seq:    r.seq,
			now:    r.now(),
			record: record,
		})
		if projected.event.SessionID == "" {
			projected.event.SessionID = r.req.SessionID
		}
		r.recordEventStats(projected.event)
		r.events <- projected.event
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
	r.finished = r.now()
	result, err := r.finalResult(decodeErr, processResult)
	r.mu.Lock()
	r.result = result
	r.waitErr = err
	r.mu.Unlock()
}

func (r *run) finalResult(decodeErr error, proc processResult) (agentwrap.RunResult, error) {
	status := agentwrap.StateCompleted
	var sdkErr *agentwrap.SDKError
	if decodeErr != nil {
		var already *agentwrap.SDKError
		if errors.As(decodeErr, &already) {
			sdkErr = already
		} else {
			sdkErr = classifyDecodeError(decodeErr)
		}
		status = agentwrap.StateFailed
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
		Session: agentwrap.SessionMetadata{
			ID:       r.req.SessionID,
			Retained: r.req.WantSession,
			Unsupported: []agentwrap.UnsupportedCapability{
				{Capability: agentwrap.CapabilitySessions, Reason: "full retained-session lifecycle is Sprint 4 scope"},
			},
		},
		Artifacts: r.artifacts,
		Warnings:  r.warnings,
		Usage:     r.usage,
		NativeMetadata: map[string]any{
			"stderr":                 r.stderrBuffer.String(),
			"exit_code":              proc.ExitCode,
			"event_count":            r.seq,
			"event_categories":       copyStringIntMap(r.categories),
			"native_event_types":     copyStringIntMap(r.nativeTypes),
			"native_extension_count": r.categories[string(agentwrap.EventNativeExtension)],
		},
	}
	if sdkErr != nil {
		metadata.Errors = []agentwrap.SDKError{*sdkErr}
	}
	result := agentwrap.RunResult{
		RunID:      r.id,
		SessionID:  r.req.SessionID,
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
	return result, nil
}

func (r *run) recordEventStats(event agentwrap.Event) {
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
