package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Antonio7098/agentwrap"
)

var runCounter atomic.Int64

// StartRun launches OpenCode in JSON event mode and returns a streaming run
// handle. The returned run owns the subprocess and event decoding state.
func (r *Runtime) StartRun(ctx context.Context, req agentwrap.RunRequest) (agentwrap.Run, error) {
	if err := validateSessionRequest(req); err != nil {
		return nil, err
	}
	if err := validateProviderModelRequest(req); err != nil {
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
	permissions, err := translatePermissions(req)
	if err != nil {
		cancel()
		return nil, err
	}
	runID := agentwrap.RunID(fmt.Sprintf("opencode-%d", runCounter.Add(1)))
	started := r.now()
	handle := &run{
		id:      runID,
		req:     req,
		ctx:     runCtx,
		cancel:  cancel,
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
		permissions:  permissions.metadata,
		stderrBuffer: newLimitBuffer(r.stderrLimit),
		stderrDone:   make(chan struct{}),
		dbQuery:      r.dbQuery,
		lifecycle:    agentwrap.StatusStarting,
	}
	handle.emitPermissionAudit("policy_initialized")
	spec, err := r.processSpec(req, permissions)
	if err != nil {
		cancel()
		return nil, err
	}
	proc, err := r.runner.Start(runCtx, spec)
	if err != nil {
		cancel()
		return nil, classifyStartError(err)
	}
	handle.proc = proc
	handle.emitLifecycle(agentwrap.StatusRunning, "process_started")
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

func (r *Runtime) processSpec(req agentwrap.RunRequest, permissions permissionTranslation) (processSpec, error) {
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
	env, err := mergeEnv(r.env, permissions.config)
	if err != nil {
		return processSpec{}, err
	}
	return processSpec{
		Executable: r.executable,
		Args:       args,
		Env:        env,
		WorkDir:    req.WorkDir,
	}, nil
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
	lifecycle     agentwrap.RunStatus
	started       time.Time
	finished      time.Time

	context            agentwrap.RuntimeContext
	eventMu            sync.Mutex
	seq                int64
	sawFinal           bool
	sawIdle            bool
	sawOutput          bool
	sessionID          agentwrap.SessionID
	artifacts          []agentwrap.ArtifactRef
	warnings           []string
	usage              agentwrap.Usage
	rateLimit          *agentwrap.RateLimitInfo
	permissions        agentwrap.PermissionMetadata
	nativeTypes        map[string]int
	categories         map[string]int
	postFinalDecodeErr string
	finishReason       string
	terminalEvidence   string
	stderrBuffer       *limitBuffer
	stderrDone         chan struct{}
	dbQuery            func(context.Context, agentwrap.SessionID, time.Time) (string, error)
	now                clock
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
	r.emitLifecycle(agentwrap.StatusCancelled, "caller_cancel")
	_ = r.cleanup(ctx, "caller_cancel")
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
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
		if projected.event.Kind() == agentwrap.EventPermission {
			r.permissions.Audit = append(r.permissions.Audit, permissionAuditFromEvent(projected.event))
		}
		r.recordEventStats(projected.event)
		if !r.sendEvent(projected.event) {
			return r.ctx.Err()
		}
		if projected.final {
			r.sawFinal = true
		}
		if projected.idle {
			r.sawIdle = true
		}
		if projected.finishReason != "" {
			r.finishReason = projected.finishReason
		}
		if projected.terminalEvidence != "" {
			r.terminalEvidence = projected.terminalEvidence
		}
		if projected.event.Kind() == agentwrap.EventMessage {
			r.sawOutput = true
		}
		if projected.usage.Native != nil || projected.usage.InputTokens != nil || projected.usage.OutputTokens != nil || projected.usage.TotalTokens != nil {
			r.usage = projected.usage
		}
		r.artifacts = append(r.artifacts, projected.artifacts...)
		r.warnings = append(r.warnings, projected.warnings...)
		if projected.rateLimit != nil {
			r.rateLimit = projected.rateLimit
		}
		if projected.fatal != nil {
			return projected.fatal
		}
		return nil
	})
	if warning := r.postFinalDecodeWarning(decodeErr); warning != "" {
		r.warnings = append(r.warnings, warning)
		r.postFinalDecodeErr = warning
		decodeErr = nil
	}
	processResult := r.proc.Wait()
	<-r.stderrDone
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cleanupCancel()
	cleanup := r.cleanup(cleanupCtx, "run_finished")
	r.finished = r.now()
	result, err := r.finalResult(decodeErr, processResult, cleanup)
	r.emitLifecycle(result.Status, lifecycleReason(result.Status))
	r.refreshResultEventStats(&result)
	r.mu.Lock()
	r.result = result
	r.waitErr = err
	r.mu.Unlock()
}

func (r *run) finalResult(decodeErr error, proc processResult, cleanup agentwrap.CleanupMetadata) (agentwrap.RunResult, error) {
	status := agentwrap.StatusCompleted
	var sdkErr *agentwrap.SDKError
	if decodeErr != nil {
		if errors.Is(decodeErr, context.Canceled) || errors.Is(decodeErr, context.DeadlineExceeded) {
			if errors.Is(decodeErr, context.DeadlineExceeded) {
				if classified := r.classifyRecentLogFailure(); classified != nil {
					sdkErr = classified.err
					if classified.info != nil {
						r.rateLimit = classified.info
					}
				} else {
					sdkErr = classifyContextError(decodeErr, "opencode run")
				}
			} else {
				sdkErr = classifyContextError(decodeErr, "opencode run")
			}
			if sdkErr.Category == agentwrap.ErrorCancellation {
				status = agentwrap.StatusCancelled
			} else {
				status = agentwrap.StatusFailed
			}
		} else {
			var already *agentwrap.SDKError
			if errors.As(decodeErr, &already) {
				sdkErr = already
			} else {
				sdkErr = classifyDecodeError(decodeErr)
			}
			status = agentwrap.StatusFailed
		}
	} else if err := r.ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			if classified := r.classifyRecentLogFailure(); classified != nil {
				sdkErr = classified.err
				if classified.info != nil {
					r.rateLimit = classified.info
				}
			} else {
				sdkErr = classifyContextError(err, "opencode run")
			}
		} else {
			sdkErr = classifyContextError(err, "opencode run")
		}
		if sdkErr.Category == agentwrap.ErrorCancellation {
			status = agentwrap.StatusCancelled
		} else {
			status = agentwrap.StatusFailed
		}
	} else if r.sawFinal {
		if proc.Err != nil || proc.ExitCode != 0 {
			if exitErr := classifyExitError(proc, r.stderrBuffer.String()); exitErr.Category != agentwrap.ErrorRuntimeExit {
				sdkErr = exitErr
				status = agentwrap.StatusFailed
			} else {
				status = agentwrap.StatusCompleted
			}
		} else {
			status = agentwrap.StatusCompleted
		}
	} else if proc.Err != nil || proc.ExitCode != 0 {
		sdkErr = classifyExitError(proc, r.stderrBuffer.String())
		status = agentwrap.StatusFailed
	} else if r.sawIdle {
		status = agentwrap.StatusCompleted
	} else if r.sawOutput {
		r.warnings = append(r.warnings, "OpenCode finished without a final structured result; treating assistant output on clean exit as completed")
		status = agentwrap.StatusCompleted
	} else if proof := r.reconcileFinalState(); proof.err != nil {
		sdkErr = proof.err
		status = agentwrap.StatusFailed
	} else if proof.completed {
		r.warnings = append(r.warnings, "OpenCode finished without a final structured result; recovered terminal finish from durable DB state")
		if proof.usage.Native != nil || proof.usage.InputTokens != nil || proof.usage.OutputTokens != nil || proof.usage.TotalTokens != nil {
			r.usage = proof.usage
		}
		status = agentwrap.StatusCompleted
	} else {
		sdkErr = agentwrap.NewError(agentwrap.ErrorRuntimeExit, "opencode run", "OpenCode finished without a final structured result", nil, agentwrap.WithDebugDetail(debugDetail(r.stderrBuffer.String())))
		status = agentwrap.StatusFailed
	}
	metadata := agentwrap.RunMetadata{
		Context:     r.context,
		Status:      status,
		StartedAt:   r.started,
		FinishedAt:  r.finished,
		Duration:    r.finished.Sub(r.started),
		Session:     sessionMetadata(r.req, r.sessionID),
		Permissions: r.permissions,
		Cleanup:     cleanup,
		Artifacts:   r.artifacts,
		Warnings:    r.warnings,
		Usage:       r.usage,
		NativeMetadata: map[string]any{
			"stderr":                 r.stderrBuffer.String(),
			"exit_code":              proc.ExitCode,
			"event_count":            r.seq,
			"event_categories":       copyStringIntMap(r.categories),
			"native_event_types":     copyStringIntMap(r.nativeTypes),
			"native_extension_count": r.categories[string(agentwrap.EventNativeExtension)],
		},
	}
	if r.postFinalDecodeErr != "" {
		metadata.NativeMetadata["post_final_decode_warning"] = r.postFinalDecodeErr
	}
	if r.finishReason != "" {
		metadata.NativeMetadata["finish_reason"] = r.finishReason
	}
	if r.terminalEvidence != "" {
		metadata.NativeMetadata["native_terminal_evidence"] = r.terminalEvidence
	}
	if r.rateLimit != nil {
		metadata.NativeMetadata["rate_limit_info"] = r.rateLimit
	} else if sdkErr != nil && sdkErr.Category == agentwrap.ErrorRateLimit {
		if info := classifyRateLimitText("opencode run", r.stderrBuffer.String(), r.context); info != nil && info.info != nil {
			metadata.NativeMetadata["rate_limit_info"] = info.info
		}
	}
	if sdkErr != nil {
		metadata.Errors = []agentwrap.SDKError{*sdkErr}
	}
	if cleanup.Error != nil {
		metadata.Errors = append(metadata.Errors, *cleanup.Error)
		warning := "OpenCode cleanup failed after run outcome was determined; preserving primary run outcome"
		r.warnings = append(r.warnings, warning)
		metadata.Warnings = r.warnings
		metadata.NativeMetadata["cleanup_warning"] = warning
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
	return result, nil
}

type dbReconcileProof struct {
	completed bool
	usage     agentwrap.Usage
	warning   string
	err       *agentwrap.SDKError
}

func (r *run) reconcileFinalState() dbReconcileProof {
	if (r.req.SessionID == "" && r.sessionID == "") || r.dbQuery == nil {
		return dbReconcileProof{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body, err := r.dbQuery(ctx, firstSessionID(r.sessionID, r.req.SessionID), r.started)
	return reconcileDBResponse(body, err)
}

func (r *Runtime) queryOpenCodeDB(ctx context.Context, sessionID agentwrap.SessionID, since time.Time) (string, error) {
	if sessionID == "" {
		return "", nil
	}
	createdAtFilter := fmt.Sprintf("session_id=%s and time_created >= %d", sqlString(string(sessionID)), since.UnixMilli())
	queries := map[string]string{
		"session":  fmt.Sprintf("select * from session where id=%s", sqlString(string(sessionID))),
		"messages": fmt.Sprintf("select * from message where %s order by time_created", createdAtFilter),
		"parts":    fmt.Sprintf("select * from part where %s order by time_created", createdAtFilter),
	}
	combined := map[string]any{}
	for key, query := range queries {
		cmd := exec.CommandContext(ctx, r.executable, "db", "--format", "json", query)
		if len(r.env) > 0 {
			cmd.Env = append(os.Environ(), r.env...)
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			return "", fmt.Errorf("opencode db %s query failed: %w: %s", key, err, strings.TrimSpace(string(out)))
		}
		var data any
		if json.Unmarshal(out, &data) != nil {
			combined[key] = string(out)
			continue
		}
		combined[key] = data
	}
	body, err := json.Marshal(combined)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func sqlString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func reconcileDBResponse(body string, err error) dbReconcileProof {
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "locked") || strings.Contains(msg, "wal_checkpoint") || strings.Contains(msg, "database") {
			return dbReconcileProof{err: agentwrap.NewError(agentwrap.ErrorRuntimeUnavailable, "opencode db", "OpenCode local DB is unavailable", err, agentwrap.WithDebugDetail(err.Error()))}
		}
		return dbReconcileProof{warning: err.Error()}
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return dbReconcileProof{}
	}
	var data any
	if json.Unmarshal([]byte(body), &data) != nil {
		return dbReconcileProof{warning: "OpenCode DB response was not JSON"}
	}
	usage := agentwrap.Usage{Native: map[string]any{"opencode_db": data}}
	if dbHasTerminalAssistant(data, &usage) {
		return dbReconcileProof{completed: true, usage: usage}
	}
	return dbReconcileProof{}
}

func dbHasTerminalAssistant(value any, usage *agentwrap.Usage) bool {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			if dbHasTerminalAssistant(item, usage) {
				return true
			}
		}
	case map[string]any:
		for _, key := range []string{"input_tokens", "inputTokens"} {
			if n, ok := int64From(v[key]); ok && usage.InputTokens == nil {
				usage.InputTokens = &n
			}
		}
		for _, key := range []string{"output_tokens", "outputTokens"} {
			if n, ok := int64From(v[key]); ok && usage.OutputTokens == nil {
				usage.OutputTokens = &n
			}
		}
		for _, key := range []string{"total_tokens", "totalTokens"} {
			if n, ok := int64From(v[key]); ok && usage.TotalTokens == nil {
				usage.TotalTokens = &n
			}
		}
		role := strings.ToLower(stringValue(v["role"]))
		finish := firstNonEmptyString(v["finish"], v["finishReason"], v["finish_reason"], v["stopReason"], v["stop_reason"])
		if role == "assistant" && finish != "" {
			return true
		}
		for _, nested := range v {
			if dbHasTerminalAssistant(nested, usage) {
				return true
			}
		}
	}
	return false
}

func firstNonEmptyString(values ...any) string {
	for _, value := range values {
		if s := strings.TrimSpace(stringValue(value)); s != "" {
			return s
		}
	}
	return ""
}

func (r *run) classifyRecentLogFailure() *rateLimitClassification {
	for _, path := range recentOpenCodeLogs(r.started) {
		content, err := os.ReadFile(path)
		if err != nil || len(content) == 0 {
			continue
		}
		text := string(content)
		if !logMayBelongToRun(text, r.context) {
			continue
		}
		if classified := classifyRateLimitText("opencode run", text, r.context); classified != nil {
			return classified
		}
	}
	return nil
}

func recentOpenCodeLogs(cutoff time.Time) []string {
	dirs := []string{}
	if dataHome := os.Getenv("XDG_DATA_HOME"); dataHome != "" {
		dirs = append(dirs, filepath.Join(dataHome, "opencode", "log"))
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		dirs = append(dirs, filepath.Join(home, ".local", "share", "opencode", "log"))
	}
	type candidate struct {
		path string
		mod  time.Time
	}
	var candidates []candidate
	seen := map[string]bool{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			if seen[path] {
				continue
			}
			info, err := entry.Info()
			if err != nil || info.ModTime().Before(cutoff) {
				continue
			}
			seen[path] = true
			candidates = append(candidates, candidate{path: path, mod: info.ModTime()})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].mod.After(candidates[j].mod) })
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.path)
	}
	return out
}

func logMayBelongToRun(text string, ctx agentwrap.RuntimeContext) bool {
	if ctx.Model == "" && ctx.Provider == "" {
		return true
	}
	if ctx.Model != "" && strings.Contains(text, string(ctx.Model)) {
		return true
	}
	if ctx.Provider != "" && strings.Contains(text, string(ctx.Provider)) {
		return true
	}
	return false
}

func (r *run) postFinalDecodeWarning(err error) string {
	if err == nil || !r.sawFinal {
		return ""
	}
	var d *decodeError
	if errors.As(err, &d) {
		return fmt.Sprintf("OpenCode emitted malformed structured output after a final result; ignoring post-final decode error at line %d: %v", d.line, d.err)
	}
	var sdkErr *agentwrap.SDKError
	if errors.As(err, &sdkErr) {
		return ""
	}
	return fmt.Sprintf("OpenCode returned an error after a final result; ignoring post-final error: %v", err)
}

func (r *run) refreshResultEventStats(result *agentwrap.RunResult) {
	if result.Metadata.NativeMetadata == nil {
		result.Metadata.NativeMetadata = map[string]any{}
	}
	result.Metadata.NativeMetadata["event_count"] = r.seq
	result.Metadata.NativeMetadata["event_categories"] = copyStringIntMap(r.categories)
	result.Metadata.NativeMetadata["native_event_types"] = copyStringIntMap(r.nativeTypes)
	result.Metadata.NativeMetadata["native_extension_count"] = r.categories[string(agentwrap.EventNativeExtension)]
}

func (r *run) cleanup(ctx context.Context, reason string) agentwrap.CleanupMetadata {
	r.cleanupOnce.Do(func() {
		procCleanup := r.proc.Cancel(ctx)
		r.cleanupResult = agentwrap.CleanupMetadata{Attempted: true, Completed: procCleanup.Err == nil, Failed: procCleanup.Err != nil}
		if procCleanup.Err != nil {
			r.cleanupResult.Error = agentwrap.NewError(agentwrap.ErrorCleanup, "opencode cleanup", "OpenCode cleanup failed", procCleanup.Err, agentwrap.WithDebugDetail(procCleanup.Err.Error()))
			return
		}
	})
	return r.cleanupResult
}

func lifecycleReason(status agentwrap.RunStatus) string {
	switch status {
	case agentwrap.StatusCompleted:
		return "run_finished"
	case agentwrap.StatusCancelled:
		return "run_cancelled"
	case agentwrap.StatusFailed:
		return "run_failed"
	default:
		return string(status)
	}
}

func (r *run) emitLifecycle(to agentwrap.RunStatus, reason string) {
	seq, from, ok := r.transitionLifecycle(to)
	if !ok {
		return
	}
	event := agentwrap.LifecycleEvent(r.id, r.req.SessionID, r.req.TurnID, r.context, seq, r.now(), from, to, reason)
	if r.sendLocalEvent(event) {
		r.recordEventStats(event)
	}
}

func (r *run) transitionLifecycle(to agentwrap.RunStatus) (int64, agentwrap.RunStatus, bool) {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	from := r.lifecycle
	if from == to || from.Terminal() {
		return 0, from, false
	}
	r.seq++
	r.lifecycle = to
	return r.seq, from, true
}

func (r *run) currentLifecycle() agentwrap.RunStatus {
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

func (r *run) emitPermissionAudit(reason string) {
	if len(r.permissions.Audit) == 0 && len(r.permissions.Support) == 0 && r.req.Permissions == "" {
		return
	}
	seq := r.nextSequence()
	event := agentwrap.Event{
		ID:        agentwrap.EventID(fmt.Sprintf("%s:%d", r.id, seq)),
		RunID:     r.id,
		SessionID: r.req.SessionID,
		Time:      r.now(),
		Type:      "permission.policy",
		Payload: agentwrap.EventPayloadWithKind(agentwrap.EventPermission, agentwrap.EventPayload{
			"turn_id":     string(r.req.TurnID),
			"context":     r.context,
			"reason":      reason,
			"mode":        string(r.permissions.Mode),
			"policy_id":   r.permissions.PolicyID,
			"policy":      r.permissions.Policy,
			"support":     r.permissions.Support,
			"unsupported": r.permissions.Unsupported,
			"audit":       r.permissions.Audit,
		}),
	}
	if r.sendLocalEvent(event) {
		r.recordEventStats(event)
	}
}

func permissionAuditFromEvent(event agentwrap.Event) agentwrap.PermissionAudit {
	audit := agentwrap.PermissionAudit{
		Source:      "opencode.event",
		Enforcement: agentwrap.PermissionEnforcementNative,
		Reason:      "native permission event observed",
	}
	if event.Payload != nil {
		if nativeType, ok := event.Payload["native_type"].(string); ok {
			audit.Metadata = map[string]string{"native_type": nativeType}
		}
	}
	return audit
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
		agentwrap.SessionID(propertiesStringValue(record.Data, "sessionID")),
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
	r.categories[string(event.Kind())]++
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

func validateProviderModelRequest(req agentwrap.RunRequest) error {
	provider := strings.TrimSpace(string(req.Provider))
	model := strings.TrimSpace(string(req.Model))
	if provider != "" && strings.Contains(provider, "/") {
		return agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode model", "provider must not contain '/'", nil, agentwrap.WithProviderModel(req.Provider, req.Model))
	}
	if model != "" && strings.Count(model, "/") > 1 {
		return agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode model", "model must be either a model id or provider/model", nil, agentwrap.WithProviderModel(req.Provider, req.Model))
	}
	return nil
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
