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

// MetadataDatabasePath selects an isolated OpenCode SQLite database for one
// logical request. The path must be absolute. Authentication and user config
// remain shared because only OPENCODE_DB is overridden.
const MetadataDatabasePath = "opencode.database_path"

// MetadataTempRoot selects a caller-owned directory beneath which agentwrap
// creates one disposable temporary directory for each OpenCode process. The
// process receives that directory through TMPDIR, TMP, and TEMP. Agentwrap
// removes it only after the process and stderr collector have exited.
const MetadataTempRoot = "opencode.temp_root"

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
		logOffsets:   openCodeLogOffsets(),
		permissions:  permissions.metadata,
		stderrBuffer: newLimitBuffer(r.stderrLimit),
		stderrDone:   make(chan struct{}),
		dbQuery:      r.dbQuery,
		rates:        r.rates,
		lifecycle:    agentwrap.StatusStarting,
	}
	handle.emitPermissionAudit("policy_initialized")
	spec, tempDir, err := r.processSpec(req, permissions)
	if err != nil {
		cancel()
		return nil, err
	}
	handle.tempDir = tempDir
	proc, err := r.runner.Start(runCtx, spec)
	if err != nil {
		_ = removeProcessTempDir(tempDir)
		cancel()
		return nil, classifyStartError(err)
	}
	handle.proc = proc
	handle.emitLifecycle(agentwrap.StatusRunning, "process_started")
	go handle.captureStderr()
	go handle.cancelOnContextDone()
	go handle.watchLogRateLimits()
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

func (r *Runtime) processSpec(req agentwrap.RunRequest, permissions permissionTranslation) (processSpec, string, error) {
	args := []string{"run", "--format", "json"}
	if req.WorkDir != "" {
		args = append(args, "--dir", req.WorkDir)
	}
	if req.Provider != "" || req.Model != "" {
		model := string(req.Model)
		if req.Provider != "" && !strings.HasPrefix(model, string(req.Provider)+"/") {
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
	env, err := mergeEnv(r.env, permissions.config)
	if err != nil {
		return processSpec{}, "", err
	}
	if r.snapshots != nil {
		env, err = withSnapshotConfig(env, *r.snapshots)
		if err != nil {
			return processSpec{}, "", err
		}
	}
	if databasePath := strings.TrimSpace(req.Metadata[MetadataDatabasePath]); databasePath != "" {
		if !filepath.IsAbs(databasePath) || filepath.Base(filepath.Clean(databasePath)) != "opencode.db" {
			return processSpec{}, "", agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode database path", "OpenCode database path must be an absolute path ending in opencode.db", nil)
		}
		env = setEnvValue(env, "OPENCODE_DB", filepath.Clean(databasePath))
	}
	tempDir, err := createProcessTempDir(req.Metadata[MetadataTempRoot])
	if err != nil {
		return processSpec{}, "", err
	}
	if tempDir != "" {
		env = setEnvValue(env, "TMPDIR", tempDir)
		env = setEnvValue(env, "TMP", tempDir)
		env = setEnvValue(env, "TEMP", tempDir)
	}
	return processSpec{
		Executable: r.executable,
		Args:       args,
		Env:        env,
		WorkDir:    req.WorkDir,
		Stdin:      req.Prompt,
	}, tempDir, nil
}

func createProcessTempDir(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", nil
	}
	root = filepath.Clean(root)
	if !filepath.IsAbs(root) {
		return "", agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode temp root", "OpenCode temp root must be an absolute path", nil)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", agentwrap.NewError(agentwrap.ErrorRuntimeUnavailable, "opencode temp root", "OpenCode temp root could not be created", err)
	}
	dir, err := os.MkdirTemp(root, "attempt-")
	if err != nil {
		return "", agentwrap.NewError(agentwrap.ErrorRuntimeUnavailable, "opencode temp directory", "OpenCode process temp directory could not be created", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return "", agentwrap.NewError(agentwrap.ErrorRuntimeUnavailable, "opencode temp directory", "OpenCode process temp directory could not be secured", err)
	}
	return dir, nil
}

func removeProcessTempDir(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	return os.RemoveAll(dir)
}

func withSnapshotConfig(env []string, enabled bool) ([]string, error) {
	const prefix = "OPENCODE_CONFIG_CONTENT="
	config := map[string]any{}
	result := make([]string, 0, len(env)+1)
	for _, item := range env {
		if !strings.HasPrefix(item, prefix) {
			result = append(result, item)
			continue
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(item, prefix)), &config); err != nil {
			return nil, agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode snapshots", "existing OPENCODE_CONFIG_CONTENT is not valid JSON", err)
		}
	}
	config["snapshot"] = enabled
	content, err := json.Marshal(config)
	if err != nil {
		return nil, agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode snapshots", "OpenCode snapshot config could not be encoded", err)
	}
	return append(result, prefix+string(content)), nil
}

func setEnvValue(env []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(env)+1)
	for _, item := range env {
		if !strings.HasPrefix(item, prefix) {
			result = append(result, item)
		}
	}
	return append(result, prefix+value)
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
	liveRateLimitKey   string
	permissions        agentwrap.PermissionMetadata
	nativeTypes        map[string]int
	categories         map[string]int
	postFinalDecodeErr string
	finishReason       string
	terminalEvidence   string
	terminalOutput     string
	stderrBuffer       *limitBuffer
	stderrDone         chan struct{}
	tempDir            string
	dbQuery            func(context.Context, agentwrap.SessionID, time.Time) (string, error)
	rates              *agentwrap.RateTableStore
	now                clock
	logOffsets         map[string]int64
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

func (r *run) watchLogRateLimits() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.done:
			return
		case <-ticker.C:
			classified := r.classifyRecentLogFailure()
			if classified == nil {
				continue
			}
			r.emitObservedRateLimit(classified)
		}
	}
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
		eventKind := projected.event.Kind()
		if eventKind == agentwrap.EventPermission {
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
		if eventKind == agentwrap.EventMessage {
			r.sawOutput = true
		}
		if eventKind == agentwrap.EventMessage || eventKind == agentwrap.EventFinalResult {
			if output := terminalOutputValue(projected.event.Payload, 0); output != "" {
				r.terminalOutput = appendTerminalOutput(r.terminalOutput, output)
			}
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
	if err := removeProcessTempDir(r.tempDir); err != nil {
		r.warnings = append(r.warnings, "remove OpenCode process temp directory: "+err.Error())
	}
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
	// OpenCode's durable message rows contain usage for providers that do not
	// emit usage.update events. Enrich every outcome, including cancellation.
	proof := r.reconcileFinalState()
	r.usage = mergeUsage(r.usage, proof.usage)
	if proof.terminalOutput != "" {
		// The durable assistant text is authoritative. Some OpenCode versions
		// omit text events from JSON output or interleave reasoning into the
		// streamed terminal buffer even though the final text is committed.
		r.terminalOutput = proof.terminalOutput
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
	if proof.cost != nil {
		// OpenCode's own per-message cost accounting is authoritative.
		metadata.EstimatedCost = proof.cost
		metadata.CostSource = agentwrap.CostSourceProviderReported
	} else if cost, source := r.priceFinalUsage(); source != "" {
		metadata.EstimatedCost = cost
		metadata.CostSource = source
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
		RunID:          r.id,
		SessionID:      firstSessionID(r.sessionID, r.req.SessionID),
		TurnID:         r.req.TurnID,
		Status:         status,
		TerminalOutput: r.terminalOutput,
		Metadata:       metadata,
		Artifacts:      r.artifacts,
		Warnings:       r.warnings,
		Usage:          r.usage,
		StartedAt:      r.started,
		FinishedAt:     r.finished,
		Err:            sdkErr,
	}
	if sdkErr != nil {
		return result, sdkErr
	}
	return result, nil
}

const maxTerminalOutputBytes = 96 << 10

func boundTerminalOutput(value string) string {
	if len(value) <= maxTerminalOutputBytes {
		return value
	}
	return value[len(value)-maxTerminalOutputBytes:]
}

func appendTerminalOutput(current, next string) string {
	if current == "" || strings.HasPrefix(next, current) {
		return boundTerminalOutput(next)
	}
	if next == current || strings.HasSuffix(current, next) {
		return current
	}
	return boundTerminalOutput(current + next)
}

func terminalOutputValue(value any, depth int) string {
	if depth > 5 {
		return ""
	}
	switch typed := value.(type) {
	case agentwrap.EventPayload:
		return terminalOutputValue(map[string]any(typed), depth)
	case map[string]any:
		for _, key := range []string{"structured_output", "output", "content", "text", "message", "part"} {
			if nested, ok := typed[key]; ok {
				if output := terminalOutputValue(nested, depth+1); output != "" {
					return output
				}
			}
		}
	case []any:
		for i := len(typed) - 1; i >= 0; i-- {
			if output := terminalOutputValue(typed[i], depth+1); output != "" {
				return output
			}
		}
	case string:
		return typed
	}
	return ""
}

type dbReconcileProof struct {
	completed      bool
	usage          agentwrap.Usage
	terminalOutput string
	warning        string
	err            *agentwrap.SDKError
	cost           *agentwrap.CostEstimate
}

// priceFinalUsage estimates cost at public API rates when the runtime did not
// report one. The returned source is empty when there was no token usage to
// price.
func (r *run) priceFinalUsage() (*agentwrap.CostEstimate, agentwrap.CostSource) {
	if r.rates == nil || !usageHasTokens(r.usage) {
		return nil, ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), agentwrap.RatesFetchTimeout)
	defer cancel()
	table, _, err := r.rates.Ensure(ctx)
	if err != nil {
		return nil, agentwrap.CostSourceUnpriced
	}
	priced := agentwrap.PriceUsage(table, string(r.context.Model), r.usage)
	switch priced.Source {
	case agentwrap.CostSourceModelPriced:
		return &agentwrap.CostEstimate{Amount: priced.Amount, Currency: priced.Currency, Estimate: true}, agentwrap.CostSourceModelPriced
	default:
		return nil, agentwrap.CostSourceUnpriced
	}
}

func usageHasTokens(usage agentwrap.Usage) bool {
	return usage.InputTokens != nil || usage.OutputTokens != nil || usage.TotalTokens != nil ||
		usage.CacheReadTokens != nil || usage.CacheWriteTokens != nil
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
	assistantTextFilter := fmt.Sprintf("p.session_id=%s and p.time_created >= %d", sqlString(string(sessionID)), since.UnixMilli())
	queries := map[string]string{
		"session":  fmt.Sprintf("select * from session where id=%s", sqlString(string(sessionID))),
		"messages": fmt.Sprintf("select * from message where %s order by time_created", createdAtFilter),
		"parts":    fmt.Sprintf("select * from part where %s order by time_created", createdAtFilter),
		"assistant_text": fmt.Sprintf(
			"select json_extract(p.data,'$.text') as text from part p join message m on m.id=p.message_id where %s and json_extract(m.data,'$.role')='assistant' and json_extract(p.data,'$.type')='text' order by p.time_created",
			assistantTextFilter,
		),
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
	metrics := dbMetrics{}
	dbCollectMetrics(data, &metrics)
	usage := agentwrap.Usage{Native: map[string]any{"source": "opencode_db"}}
	if metrics.turns > 0 {
		usage.Turns = int64Ptr(metrics.turns)
		usage.InputTokens = int64Ptr(metrics.input)
		usage.OutputTokens = int64Ptr(metrics.output)
		usage.TotalTokens = int64Ptr(metrics.total)
		usage.ReasoningTokens = int64Ptr(metrics.reasoning)
		usage.CacheReadTokens = int64Ptr(metrics.cacheRead)
		usage.CacheWriteTokens = int64Ptr(metrics.cacheWrite)
	}
	var cost *agentwrap.CostEstimate
	if metrics.costKnown {
		cost = &agentwrap.CostEstimate{Amount: metrics.cost, Currency: "USD", Estimate: false}
	}
	return dbReconcileProof{completed: metrics.turns > 0, usage: usage, terminalOutput: dbAssistantText(data), cost: cost}
}

func dbAssistantText(value any) string {
	root, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	rows, ok := root["assistant_text"].([]any)
	if !ok {
		return ""
	}
	var output string
	for _, row := range rows {
		fields, ok := row.(map[string]any)
		if !ok {
			continue
		}
		if text := stringValue(fields["text"]); text != "" {
			output = appendTerminalOutput(output, text)
		}
	}
	return output
}

type dbMetrics struct {
	turns, input, output, total, reasoning, cacheRead, cacheWrite int64
	cost                                                          float64
	costKnown                                                     bool
}

func int64Ptr(v int64) *int64 { return &v }
func dbCollectMetrics(value any, m *dbMetrics) {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			dbCollectMetrics(item, m)
		}
	case string:
		var nested any
		if strings.HasPrefix(strings.TrimSpace(v), "{") && json.Unmarshal([]byte(v), &nested) == nil {
			dbCollectMetrics(nested, m)
		}
	case map[string]any:
		if strings.EqualFold(stringValue(v["role"]), "assistant") && finishReasonFrom(v) != "" {
			m.turns++
			if tokens, ok := v["tokens"].(map[string]any); ok {
				m.input += number(tokens["input"])
				m.output += number(tokens["output"])
				m.reasoning += number(tokens["reasoning"])
				m.total += number(tokens["total"])
				if cache, ok := tokens["cache"].(map[string]any); ok {
					m.cacheRead += number(cache["read"])
					m.cacheWrite += number(cache["write"])
				}
			} else {
				m.input += firstNumber(v, "input_tokens", "inputTokens")
				m.output += firstNumber(v, "output_tokens", "outputTokens")
				m.total += firstNumber(v, "total_tokens", "totalTokens")
			}
			if c, ok := float64From(v["cost"]); ok {
				m.costKnown = true
				m.cost += c
			}
			return
		}
		for _, nested := range v {
			dbCollectMetrics(nested, m)
		}
	}
}
func number(v any) int64 { n, _ := int64From(v); return n }
func firstNumber(values map[string]any, keys ...string) int64 {
	for _, key := range keys {
		if n, ok := int64From(values[key]); ok {
			return n
		}
	}
	return 0
}
func float64From(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}
func mergeUsage(primary, fallback agentwrap.Usage) agentwrap.Usage {
	// Durable DB usage is the aggregate across every assistant turn in the run,
	// while streamed usage commonly describes only the most recent turn. Prefer
	// every populated aggregate field so callers do not under-report multi-turn
	// runs. Keep streaming values only when reconciliation could not recover a
	// field.
	if fallback.InputTokens != nil {
		primary.InputTokens = fallback.InputTokens
	}
	if fallback.OutputTokens != nil {
		primary.OutputTokens = fallback.OutputTokens
	}
	if fallback.TotalTokens != nil {
		primary.TotalTokens = fallback.TotalTokens
	}
	if fallback.ReasoningTokens != nil {
		primary.ReasoningTokens = fallback.ReasoningTokens
	}
	if fallback.CacheReadTokens != nil {
		primary.CacheReadTokens = fallback.CacheReadTokens
	}
	if fallback.CacheWriteTokens != nil {
		primary.CacheWriteTokens = fallback.CacheWriteTokens
	}
	if fallback.Turns != nil {
		primary.Turns = fallback.Turns
	}
	if fallback.Native != nil {
		primary.Native = fallback.Native
	}
	return primary
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
		// OpenCode stores tokens in a nested object: data.tokens.{input,output,total,reasoning}
		// plus data.tokens.cache.{read,write}. The flat field fallback covers older shapes.
		if tokens, ok := v["tokens"].(map[string]any); ok {
			if n, ok := int64From(tokens["input"]); ok && usage.InputTokens == nil {
				usage.InputTokens = &n
			}
			if n, ok := int64From(tokens["output"]); ok && usage.OutputTokens == nil {
				usage.OutputTokens = &n
			}
			if n, ok := int64From(tokens["total"]); ok && usage.TotalTokens == nil {
				usage.TotalTokens = &n
			}
			if n, ok := int64From(tokens["reasoning"]); ok && usage.ReasoningTokens == nil {
				usage.ReasoningTokens = &n
			}
			if cache, ok := tokens["cache"].(map[string]any); ok {
				if n, ok := int64From(cache["read"]); ok && usage.CacheReadTokens == nil {
					usage.CacheReadTokens = &n
				}
				if n, ok := int64From(cache["write"]); ok && usage.CacheWriteTokens == nil {
					usage.CacheWriteTokens = &n
				}
			}
		}
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
		content, err := readOpenCodeLogDelta(path, r.logOffsets[path])
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

const maxOpenCodeLogScanBytes = 256 * 1024

func openCodeLogOffsets() map[string]int64 {
	offsets := map[string]int64{}
	for _, path := range recentOpenCodeLogs(time.Time{}) {
		if info, err := os.Stat(path); err == nil {
			offsets[path] = info.Size()
		}
	}
	return offsets
}

func readOpenCodeLogDelta(path string, offset int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if offset < 0 || info.Size() < offset {
		offset = 0
	}
	start := offset
	if info.Size()-start > maxOpenCodeLogScanBytes {
		start = info.Size() - maxOpenCodeLogScanBytes
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(io.LimitReader(file, maxOpenCodeLogScanBytes))
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

func (r *run) emitObservedRateLimit(classified *rateLimitClassification) {
	if classified == nil || classified.info == nil {
		return
	}
	key := strings.Join([]string{
		string(classified.info.Provider),
		string(classified.info.Model),
		classified.info.UserDetail,
		classified.info.RetryAfter.String(),
	}, "\x00")
	r.eventMu.Lock()
	if key == r.liveRateLimitKey {
		r.eventMu.Unlock()
		return
	}
	r.liveRateLimitKey = key
	r.eventMu.Unlock()
	seq := r.nextSequence()
	event := agentwrap.Event{
		ID:        agentwrap.EventID(fmt.Sprintf("%s:%d", r.id, seq)),
		RunID:     r.id,
		SessionID: firstSessionID(r.sessionID, r.req.SessionID),
		Time:      r.now(),
		Type:      "opencode.log.rate_limit",
		Payload: agentwrap.EventPayloadWithKind(agentwrap.EventRateLimit, agentwrap.EventPayload{
			"turn_id":     string(r.req.TurnID),
			"context":     r.context,
			"provider":    classified.info.Provider,
			"model":       classified.info.Model,
			"retry_after": classified.info.RetryAfter.String(),
			"reset_at":    classified.info.ResetAt,
			"detail":      classified.info.UserDetail,
			"source":      "opencode.log",
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
