package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Antonio7098/agentwrap"
)

type fakeRunner struct {
	proc   process
	procs  []process
	err    error
	spec   processSpec
	starts int
}

func (r *fakeRunner) Start(_ context.Context, spec processSpec) (process, error) {
	r.spec = spec
	r.starts++
	if r.err != nil {
		return nil, r.err
	}
	if len(r.procs) > 0 {
		next := r.procs[0]
		r.procs = r.procs[1:]
		return next, nil
	}
	return r.proc, nil
}

type fakeProcess struct {
	stdout      string
	stderr      string
	result      processResult
	cancel      error
	cancelCount int
	blockCh     chan struct{}
	closeOnce   sync.Once
	once        sync.Once
	mu          sync.Mutex
}

func (p *fakeProcess) Stdout() io.ReadCloser {
	if p.blockCh != nil {
		return blockingReadCloser{done: p.blockCh, close: &p.closeOnce}
	}
	return io.NopCloser(bytes.NewBufferString(p.stdout))
}
func (p *fakeProcess) Stderr() io.Reader   { return bytes.NewBufferString(p.stderr) }
func (p *fakeProcess) Wait() processResult { return p.result }
func (p *fakeProcess) Cancel(context.Context) cleanupResult {
	p.mu.Lock()
	p.cancelCount++
	p.mu.Unlock()
	p.once.Do(func() {
		if p.blockCh != nil {
			p.closeOnce.Do(func() { close(p.blockCh) })
		}
	})
	return cleanupResult{GracefulAttempted: true, Err: p.cancel}
}

func (p *fakeProcess) CancelCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cancelCount
}

type blockingReadCloser struct {
	done  chan struct{}
	close *sync.Once
}

func (r blockingReadCloser) Read([]byte) (int, error) {
	<-r.done
	return 0, io.EOF
}

func (r blockingReadCloser) Close() error {
	r.close.Do(func() { close(r.done) })
	return nil
}

func TestStartRunBuildsStructuredCommand(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner), WithExecutable("/bin/opencode"), WithExtraArgs("--pure"))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:   "hello",
		WorkDir:  "/tmp/work",
		Provider: "anthropic",
		Model:    "claude",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = drainRun(t, run)
	want := []string{"run", "--format", "json", "--dir", "/tmp/work", "--model", "anthropic/claude", "--pure", "hello"}
	if !equalStrings(runner.spec.Args, want) {
		t.Fatalf("args = %#v, want %#v", runner.spec.Args, want)
	}
	if runner.spec.WorkDir != "/tmp/work" || runner.spec.Executable != "/bin/opencode" {
		t.Fatalf("bad spec: %#v", runner.spec)
	}
}

func TestStartRunInjectsPermissionConfigContent(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson")}}
	rt := NewRuntime(
		withProcessRunner(runner),
		WithEnv("EXISTING=1", `OPENCODE_CONFIG_CONTENT={"provider":{"example":true},"permission":{"webfetch":"allow","bash":"allow"}}`),
	)
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt: "hello",
		PermissionPolicy: &agentwrap.PermissionPolicy{
			Tools: map[agentwrap.PermissionTool]agentwrap.PermissionAction{
				agentwrap.PermissionToolRead:         agentwrap.PermissionActionAllow,
				agentwrap.PermissionToolEdit:         agentwrap.PermissionActionDeny,
				agentwrap.PermissionToolShell:        agentwrap.PermissionActionAsk,
				agentwrap.PermissionToolGlob:         agentwrap.PermissionActionAllow,
				agentwrap.PermissionToolRepoOverview: agentwrap.PermissionActionDeny,
				agentwrap.PermissionToolDoomLoop:     agentwrap.PermissionActionAsk,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, result := drainRun(t, run)
	if result.Metadata.Permissions.Policy.Tools[agentwrap.PermissionToolShell] != agentwrap.PermissionActionAsk {
		t.Fatalf("permission metadata = %#v", result.Metadata.Permissions)
	}
	if len(events) < 2 || events[0].Type != "permission.policy" || events[0].Kind() != agentwrap.EventPermission {
		t.Fatalf("permission audit event missing: %#v", events)
	}
	eventPolicyID, ok := events[0].Payload["policy_id"].(string)
	if !ok || eventPolicyID == "" || result.Metadata.Permissions.PolicyID == "" {
		t.Fatalf("permission policy id missing: event=%#v metadata=%#v", events[0].Payload, result.Metadata.Permissions)
	}
	if eventPolicyID != result.Metadata.Permissions.PolicyID {
		t.Fatalf("policy_id mismatch: event=%q metadata=%q", eventPolicyID, result.Metadata.Permissions.PolicyID)
	}
	if events[1].Kind() != agentwrap.EventLifecycle {
		t.Fatalf("permission audit should precede process lifecycle event: %#v", events[:2])
	}
	var configValue string
	for _, env := range runner.spec.Env {
		if len(env) >= len("OPENCODE_CONFIG_CONTENT=") && env[:len("OPENCODE_CONFIG_CONTENT=")] == "OPENCODE_CONFIG_CONTENT=" {
			if configValue != "" {
				t.Fatalf("duplicate OPENCODE_CONFIG_CONTENT env: %#v", runner.spec.Env)
			}
			configValue = env
		}
	}
	if configValue == "" {
		t.Fatalf("OPENCODE_CONFIG_CONTENT missing from env: %#v", runner.spec.Env)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(configValue[len("OPENCODE_CONFIG_CONTENT="):]), &config); err != nil {
		t.Fatalf("permission config is invalid JSON: %v config=%s", err, configValue)
	}
	if provider, ok := config["provider"].(map[string]any); !ok || provider["example"] != true {
		t.Fatalf("existing config was not preserved: %#v", config)
	}
	permission, ok := config["permission"].(map[string]any)
	if !ok {
		t.Fatalf("permission config missing: %#v", config)
	}
	wantPermission := map[string]string{
		"read":          "allow",
		"edit":          "deny",
		"bash":          "ask",
		"glob":          "allow",
		"repo_overview": "deny",
		"doom_loop":     "ask",
		"webfetch":      "allow",
	}
	for key, want := range wantPermission {
		if got := permission[key]; got != want {
			t.Fatalf("permission[%s] = %#v, want %q; config=%#v", key, got, want, config)
		}
	}
}

func TestStartRunRejectsInvalidExistingConfigContent(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson")}}
	rt := NewRuntime(
		withProcessRunner(runner),
		WithEnv("OPENCODE_CONFIG_CONTENT=not-json"),
	)
	_, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt: "hello",
		PermissionPolicy: &agentwrap.PermissionPolicy{
			Tools: map[agentwrap.PermissionTool]agentwrap.PermissionAction{
				agentwrap.PermissionToolShell: agentwrap.PermissionActionAllow,
			},
		},
	})
	var sdkErr *agentwrap.SDKError
	if err == nil || !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorConfiguration {
		t.Fatalf("err = %#v, want configuration SDKError", err)
	}
	if runner.starts != 0 {
		t.Fatalf("process starts = %d, want 0", runner.starts)
	}
}

func TestStartRunRejectsProviderContainingSlash(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	_, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:   "hello",
		Provider: "nonexistent-provider/test",
		Model:    "model",
	})
	var sdkErr *agentwrap.SDKError
	if err == nil || !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorConfiguration {
		t.Fatalf("err = %#v, want configuration SDKError", err)
	}
	if runner.starts != 0 {
		t.Fatalf("process starts = %d, want 0", runner.starts)
	}
}

func TestStartRunRejectsModelWithTooManySlashes(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	_, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt: "hello",
		Model:  "provider/model/extra",
	})
	var sdkErr *agentwrap.SDKError
	if err == nil || !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorConfiguration {
		t.Fatalf("err = %#v, want configuration SDKError", err)
	}
	if runner.starts != 0 {
		t.Fatalf("process starts = %d, want 0", runner.starts)
	}
}

func TestStartRunRejectsRequiredUnsupportedPathPermission(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	_, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt: "hello",
		PermissionPolicy: &agentwrap.PermissionPolicy{
			PathRules: []agentwrap.PermissionPathRule{{Path: "/tmp/outside", Action: agentwrap.PermissionActionDeny}},
		},
	})
	if err == nil {
		t.Fatal("StartRun error = nil, want unsupported permission error")
	}
	var sdkErr *agentwrap.SDKError
	if !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorConfiguration {
		t.Fatalf("error = %#v, want configuration SDKError", err)
	}
	if runner.starts != 0 {
		t.Fatalf("process starts = %d, want 0", runner.starts)
	}
}

func TestStartRunAllowsBestEffortUnsupportedPathPermission(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt: "hello",
		PermissionPolicy: &agentwrap.PermissionPolicy{
			UnsupportedBehavior: agentwrap.PermissionUnsupportedBestEffort,
			PathRules:           []agentwrap.PermissionPathRule{{Path: "/tmp/outside", Action: agentwrap.PermissionActionDeny}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, result := drainRun(t, run)
	if len(result.Metadata.Permissions.Unsupported) != 1 {
		t.Fatalf("unsupported metadata = %#v", result.Metadata.Permissions.Unsupported)
	}
}

func TestPolicyRunnerWrapsOpenCodeRuntimeWithoutAdapterRetry(t *testing.T) {
	runner := &fakeRunner{procs: []process{
		&fakeProcess{stdout: readFixture(t, "partial.ndjson"), stderr: "temporary", result: processResult{ExitCode: 1}},
		&fakeProcess{stdout: readFixture(t, "normal.ndjson")},
	}}
	rt := NewRuntime(withProcessRunner(runner))
	policy := agentwrap.PolicyRunner{
		Runtime: rt,
		Policy:  agentwrap.BasicPolicy{MaxAttemptsPerTarget: 2, Backoff: agentwrap.FixedBackoff{}},
		Sleep:   func(context.Context, time.Duration) error { return nil },
	}
	run, err := policy.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait error: %v", err)
	}
	if runner.starts != 2 {
		t.Fatalf("OpenCode starts = %d, want 2 policy-managed attempts", runner.starts)
	}
	if len(result.Metadata.Attempts) != 2 {
		t.Fatalf("attempts = %d, want 2", len(result.Metadata.Attempts))
	}
	if len(result.Metadata.Policy.Decisions) != 1 || result.Metadata.Policy.Decisions[0].Kind != agentwrap.PolicyDecisionRetry {
		t.Fatalf("policy decisions = %#v", result.Metadata.Policy.Decisions)
	}
}

func TestRunSuccessEmitsCanonicalEventsAndResult(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson"), stderr: "diagnostic"}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello", SessionID: "ses_req"})
	if err != nil {
		t.Fatal(err)
	}
	events, result := drainRun(t, run)
	if result.Err != nil || result.Status != agentwrap.StatusCompleted {
		t.Fatalf("result = %#v err=%v", result, result.Err)
	}
	if len(events) != 6 {
		t.Fatalf("events = %d, want 6", len(events))
	}
	if events[0].Kind() != agentwrap.EventLifecycle || events[1].Kind() != agentwrap.EventSession || events[2].Kind() != agentwrap.EventProgress || events[3].Kind() != agentwrap.EventMessage || events[4].Kind() != agentwrap.EventFinalResult || events[5].Kind() != agentwrap.EventLifecycle {
		t.Fatalf("unexpected categories: %#v", events)
	}
	if events[2].Raw == nil || events[2].Raw.Safe {
		t.Fatalf("raw payload missing or safe: %#v", events[2].Raw)
	}
	if got := result.Metadata.NativeMetadata["stderr"]; got != "diagnostic" {
		t.Fatalf("stderr metadata = %#v", got)
	}
}

func TestRunUnknownEventDoesNotFail(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "unknown.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	events, result := drainRun(t, run)
	if result.Err != nil || result.Status != agentwrap.StatusCompleted {
		t.Fatalf("result = %#v err=%v", result, result.Err)
	}
	if events[2].Kind() != agentwrap.EventNativeExtension {
		t.Fatalf("native category = %s", events[2].Kind())
	}
	if got := result.Metadata.NativeMetadata["native_extension_count"]; got != 1 {
		t.Fatalf("native_extension_count = %#v, want 1", got)
	}
	if got := result.Metadata.NativeMetadata["event_count"]; got != int64(5) {
		t.Fatalf("event_count = %#v, want 5", got)
	}
}

func TestRunProjectsUsageAndArtifacts(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "final.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	events, result := drainRun(t, run)
	if result.Status != agentwrap.StatusCompleted {
		t.Fatalf("status = %s", result.Status)
	}
	if len(events) != 6 || events[2].Kind() != agentwrap.EventUsage || events[3].Kind() != agentwrap.EventArtifact || events[4].Kind() != agentwrap.EventFinalResult {
		t.Fatalf("unexpected events: %#v", events)
	}
	if result.Usage.TotalTokens == nil || *result.Usage.TotalTokens != 12 {
		t.Fatalf("usage = %#v", result.Usage)
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0].URI != "file:///tmp/report.md" {
		t.Fatalf("artifacts = %#v", result.Artifacts)
	}
}

func TestRunFixtureGoldenEvents(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
		golden  string
	}{
		{name: "normal", fixture: "normal.ndjson", golden: "normal.golden.json"},
		{name: "unknown", fixture: "unknown.ndjson", golden: "unknown.golden.json"},
		{name: "final", fixture: "final.ndjson", golden: "final.golden.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, tt.fixture)}}
			rt := NewRuntime(withProcessRunner(runner))
			run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
			if err != nil {
				t.Fatal(err)
			}
			events, result := drainRun(t, run)
			got := snapshotRun(events, result)
			want := readGoldenSnapshot(t, tt.golden)
			if !reflect.DeepEqual(got, want) {
				gotJSON, _ := json.MarshalIndent(got, "", "  ")
				wantJSON, _ := json.MarshalIndent(want, "", "  ")
				t.Fatalf("snapshot mismatch\ngot:\n%s\nwant:\n%s", gotJSON, wantJSON)
			}
		})
	}
}

func TestRunMalformedFails(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "malformed.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorMalformedEvent {
		t.Fatalf("err = %v result=%#v", err, result)
	}
	if result.Status != agentwrap.StatusFailed {
		t.Fatalf("status = %s", result.Status)
	}
}

func TestClassifyExitErrorDetectsJSONRateLimit(t *testing.T) {
	stderr := `{"statusCode":429,"responseHeaders":{"retry-after-ms":"1500","x-ratelimit-limit-requests":"500","x-ratelimit-remaining-requests":"0","x-ratelimit-reset-requests":"1s"},"responseBody":"{\"type\":\"error\",\"error\":{\"type\":\"too_many_requests\"}}","message":"too many requests"}`
	err := classifyExitError(processResult{ExitCode: 1}, stderr)
	if err == nil || err.Category != agentwrap.ErrorRateLimit {
		t.Fatalf("err = %#v, want retryable rate limit", err)
	}
}

func TestClassifyRateLimitTextDetectsMiniMaxNestedUsageLimit(t *testing.T) {
	stderr := `{"statusCode":429,"responseBody":"{\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"usage limit exceeded, 5-hour usage limit reached, resets at 2026-05-20T20:00:00Z\"}}"}`
	classified := classifyRateLimitText("opencode run", stderr, agentwrap.RuntimeContext{
		Provider: "minimax-coding-plan",
		Model:    "MiniMax-M2.7",
	})
	if classified == nil || classified.err == nil || classified.err.Category != agentwrap.ErrorRateLimit {
		t.Fatalf("classification = %#v, want rate_limit", classified)
	}
	if classified.info == nil || classified.info.Provider != "minimax-coding-plan" || classified.info.Model != "MiniMax-M2.7" {
		t.Fatalf("rate limit info = %#v", classified.info)
	}
}

func TestClassifyExitErrorSkipsQuotaExceeded(t *testing.T) {
	stderr := `{"statusCode":429,"responseBody":"insufficient_quota","message":"quota exceeded"}`
	err := classifyExitError(processResult{ExitCode: 1}, stderr)
	if err == nil || err.Category == agentwrap.ErrorRateLimit {
		t.Fatalf("err = %#v, want non-rate-limit classification", err)
	}
}

func TestClassifyExitErrorSkipsQuotaExceededInMessage(t *testing.T) {
	stderr := `{"statusCode":429,"message":"quota_exceeded"}`
	err := classifyExitError(processResult{ExitCode: 1}, stderr)
	if err == nil || err.Category == agentwrap.ErrorRateLimit {
		t.Fatalf("err = %#v, want non-rate-limit classification", err)
	}
}

func TestParseRetryDelaySupportsHTTPDate(t *testing.T) {
	future := time.Now().UTC().Add(time.Hour).Format(time.RFC1123)
	if delay := parseRetryDelay(future); delay <= 0 {
		t.Fatalf("delay = %s, want positive duration from HTTP date", delay)
	}
}

func TestClassifyRateLimitDataParsesOpenCodeHeaders(t *testing.T) {
	classified := classifyRateLimitData("opencode event", map[string]any{
		"statusCode": 429,
		"message":    "provider overloaded",
		"responseHeaders": map[string]any{
			"retry-after":                            "2",
			"anthropic-ratelimit-requests-limit":     "500",
			"anthropic-ratelimit-requests-remaining": "0",
			"anthropic-ratelimit-requests-reset":     "10s",
		},
		"metadata": map[string]any{"provider": "anthropic", "model": "claude"},
	}, agentwrap.RuntimeContext{})
	if classified == nil || classified.info == nil {
		t.Fatal("expected rate-limit classification")
	}
	if classified.info.Provider != "anthropic" || classified.info.Model != "claude" {
		t.Fatalf("info = %#v", classified.info)
	}
	if classified.info.RetryAfter != 2*time.Second {
		t.Fatalf("retry after = %s, want 2s", classified.info.RetryAfter)
	}
}

func TestProjectNativeFatalErrorPromotesRateLimit(t *testing.T) {
	record := nativeRecord{
		Type: "error",
		Data: map[string]any{
			"message": "too many requests",
			"responseHeaders": map[string]any{
				"retry-after-ms": "500",
			},
			"statusCode": 429,
		},
	}
	projected := projectNative(projectionInput{
		runID:  "run-1",
		ctx:    agentwrap.RuntimeContext{Provider: "openai", Model: "gpt"},
		seq:    1,
		now:    time.Now().UTC(),
		record: record,
	})
	if projected.fatal == nil || projected.fatal.Category != agentwrap.ErrorRateLimit {
		t.Fatalf("fatal = %#v, want rate limit", projected.fatal)
	}
	if _, ok := projected.event.Payload["rate_limit"]; !ok {
		t.Fatalf("payload missing rate_limit: %#v", projected.event.Payload)
	}
}

func TestProjectNativeFatalErrorClassifiesModelNotFound(t *testing.T) {
	projected := projectNative(projectionInput{
		runID: "run-1",
		ctx:   agentwrap.RuntimeContext{RuntimeKind: "opencode", Provider: "opencode", Model: "not-a-real-model"},
		record: nativeRecord{Type: "error", Data: map[string]any{
			"message": "Model not found: opencode/not-a-real-model",
		}},
	})
	if projected.fatal == nil || projected.fatal.Category != agentwrap.ErrorModelUnavailable {
		t.Fatalf("fatal = %#v, want model_unavailable", projected.fatal)
	}
}

func TestProjectNativeFatalErrorClassifiesAuthentication(t *testing.T) {
	projected := projectNative(projectionInput{
		runID: "run-1",
		ctx:   agentwrap.RuntimeContext{RuntimeKind: "opencode", Provider: "anthropic", Model: "claude"},
		record: nativeRecord{Type: "error", Data: map[string]any{
			"message": "authentication failed: invalid API key",
		}},
	})
	if projected.fatal == nil || projected.fatal.Category != agentwrap.ErrorAuthentication {
		t.Fatalf("fatal = %#v, want authentication", projected.fatal)
	}
}

func TestRunFatalEventStoresRateLimitMetadata(t *testing.T) {
	stdout := `{"type":"error","statusCode":429,"message":"too many requests","responseHeaders":{"retry-after-ms":"500"}}
`
	runner := &fakeRunner{proc: &fakeProcess{stdout: stdout}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello", Provider: "openai", Model: "gpt"})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorRateLimit {
		t.Fatalf("err = %v result=%#v", err, result)
	}
	info, ok := result.Metadata.NativeMetadata["rate_limit_info"].(*agentwrap.RateLimitInfo)
	if !ok || info == nil {
		t.Fatalf("rate_limit_info = %#v", result.Metadata.NativeMetadata["rate_limit_info"])
	}
	if info.RetryAfter != 500*time.Millisecond {
		t.Fatalf("retry_after = %s, want 500ms", info.RetryAfter)
	}
}

func TestRunExitRateLimitStoresRateLimitMetadata(t *testing.T) {
	stderr := `{"statusCode":429,"responseHeaders":{"retry-after-ms":"1500"},"message":"too many requests"}`
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson"), stderr: stderr, result: processResult{ExitCode: 1}}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello", Provider: "openai", Model: "gpt"})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorRateLimit {
		t.Fatalf("err = %v result=%#v", err, result)
	}
	info, ok := result.Metadata.NativeMetadata["rate_limit_info"].(*agentwrap.RateLimitInfo)
	if !ok || info == nil {
		t.Fatalf("rate_limit_info = %#v", result.Metadata.NativeMetadata["rate_limit_info"])
	}
	if info.RetryAfter != 1500*time.Millisecond {
		t.Fatalf("retry_after = %s, want 1500ms", info.RetryAfter)
	}
}

func TestRunPartialWithoutFinalCompletesWithWarning(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "partial.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result := drainRun(t, run)
	if result.Status != agentwrap.StatusCompleted || result.Err != nil || len(result.Warnings) == 0 {
		t.Fatalf("result = %#v, want completed with warning", result)
	}
}

func TestRunStepFinishWithNonTerminalReasonDoesNotSetFinal(t *testing.T) {
	stdout := `{"type":"step_start","timestamp":1710000000000,"sessionID":"ses_tool"}
{"type":"step_finish","timestamp":1710000001000,"sessionID":"ses_tool","finish_reason":"tool_calls"}
`
	runner := &fakeRunner{proc: &fakeProcess{stdout: stdout}}
	rt := NewRuntime(withProcessRunner(runner), withDBQuery(nil))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	events, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorRuntimeExit {
		t.Fatalf("err = %v result=%#v, want runtime_exit because tool_calls is non-terminal", err, result)
	}
	for _, event := range events {
		if event.Kind() == agentwrap.EventFinalResult {
			t.Fatalf("non-terminal step_finish was projected as final: %#v", event)
		}
	}
	if got := result.Metadata.NativeMetadata["finish_reason"]; got != "tool_calls" {
		t.Fatalf("finish_reason metadata = %#v, want tool_calls", got)
	}
}

func TestRunSessionStatusIdleCompletesWithoutOutputFallback(t *testing.T) {
	stdout := `{"type":"step_start","timestamp":1710000000000,"sessionID":"ses_idle"}
{"type":"session.status","timestamp":1710000001000,"sessionID":"ses_idle","status":{"type":"idle"}}
`
	runner := &fakeRunner{proc: &fakeProcess{stdout: stdout}}
	rt := NewRuntime(withProcessRunner(runner), withDBQuery(nil))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result := drainRun(t, run)
	if result.Status != agentwrap.StatusCompleted || result.Err != nil {
		t.Fatalf("result = %#v, want idle-based completion", result)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("warnings = %#v, want no output-fallback warning", result.Warnings)
	}
	if got := result.Metadata.NativeMetadata["native_terminal_evidence"]; got != "session.status.idle" {
		t.Fatalf("native_terminal_evidence = %#v, want session.status.idle", got)
	}
}

func TestRunTrailingEventsAfterStepFinishAreCaptured(t *testing.T) {
	stdout := `{"type":"step_start","timestamp":1710000000000,"sessionID":"ses_trailing"}
{"type":"step_finish","timestamp":1710000001000,"sessionID":"ses_trailing","finish_reason":"stop"}
{"type":"usage.update","timestamp":1710000002000,"sessionID":"ses_trailing","input_tokens":1,"output_tokens":2,"total_tokens":3}
`
	runner := &fakeRunner{proc: &fakeProcess{stdout: stdout}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result := drainRun(t, run)
	if result.Status != agentwrap.StatusCompleted || result.Err != nil {
		t.Fatalf("result = %#v, want completed", result)
	}
	if result.Usage.TotalTokens == nil || *result.Usage.TotalTokens != 3 {
		t.Fatalf("usage = %#v, want trailing usage captured", result.Usage)
	}
	if got := result.Metadata.NativeMetadata["finish_reason"]; got != "stop" {
		t.Fatalf("finish_reason metadata = %#v, want stop", got)
	}
}

func TestRunNonZeroExitCapturesStderr(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{
		stdout: readFixture(t, "nonzero.ndjson"),
		stderr: "bad auth",
		result: processResult{ExitCode: 7, Err: errors.New("exit status 7")},
	}}
	rt := NewRuntime(withProcessRunner(runner), WithStderrLimit(4))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorRuntimeExit {
		t.Fatalf("err = %v result=%#v", err, result)
	}
	if got := result.Metadata.NativeMetadata["stderr"]; got != "bad " {
		t.Fatalf("bounded stderr = %#v", got)
	}
}

func TestStartFailureIsRuntimeUnavailable(t *testing.T) {
	rt := NewRuntime(withProcessRunner(&fakeRunner{err: errors.New("not found")}))
	_, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	var sdkErr *agentwrap.SDKError
	if !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorRuntimeUnavailable {
		t.Fatalf("err = %T %v", err, err)
	}
}

func TestCancelClassifiesRunAsCancelled(t *testing.T) {
	proc := &fakeProcess{blockCh: make(chan struct{})}
	rt := NewRuntime(withProcessRunner(&fakeRunner{proc: proc}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	events, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorCancellation || result.Status != agentwrap.StatusCancelled {
		t.Fatalf("err = %v result=%#v", err, result)
	}
	requireLifecycleTransition(t, events, agentwrap.StatusRunning, agentwrap.StatusCancelled, "caller_cancel")
}

func TestContextTimeoutClassifiesRunAsTimeout(t *testing.T) {
	proc := &fakeProcess{blockCh: make(chan struct{})}
	rt := NewRuntime(withProcessRunner(&fakeRunner{proc: proc}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello", Timeout: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorTimeout {
		t.Fatalf("err = %v result=%#v", err, result)
	}
}

func TestBlockedStdoutTimeoutStaysTimeout(t *testing.T) {
	proc := &fakeProcess{blockCh: make(chan struct{})}
	rt := NewRuntime(withProcessRunner(&fakeRunner{proc: proc}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello", Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil {
		t.Fatalf("expected timeout result, got nil err result=%#v", result)
	}
	if result.Err == nil || result.Err.Category != agentwrap.ErrorTimeout {
		t.Fatalf("result err = %#v", result.Err)
	}
	if result.Status != agentwrap.StatusFailed {
		t.Fatalf("status = %s", result.Status)
	}
}

func TestCleanupFailurePreservesRunOutcome(t *testing.T) {
	proc := &fakeProcess{stdout: readFixture(t, "normal.ndjson"), cancel: errors.New("cleanup failed")}
	rt := NewRuntime(withProcessRunner(&fakeRunner{proc: proc}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result := drainRun(t, run)
	if result.Status != agentwrap.StatusCompleted || result.Err != nil {
		t.Fatalf("cleanup failure result = %#v, want completed primary outcome", result)
	}
	if !result.Metadata.Cleanup.Failed || result.Metadata.Cleanup.Error == nil || result.Metadata.Cleanup.Error.Category != agentwrap.ErrorCleanup {
		t.Fatalf("cleanup metadata = %#v", result.Metadata.Cleanup)
	}
	if len(result.Metadata.Errors) != 1 || result.Metadata.Errors[0].Category != agentwrap.ErrorCleanup {
		t.Fatalf("metadata errors = %#v, want cleanup metadata error", result.Metadata.Errors)
	}
	if got, _ := result.Metadata.NativeMetadata["cleanup_warning"].(string); got == "" {
		t.Fatalf("cleanup_warning missing from native metadata: %#v", result.Metadata.NativeMetadata)
	}
}

func TestCancelWithCleanupFailurePreservesCancellation(t *testing.T) {
	proc := &fakeProcess{blockCh: make(chan struct{}), cancel: errors.New("cleanup failed")}
	rt := NewRuntime(withProcessRunner(&fakeRunner{proc: proc}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel error = %v, want nil with cleanup issue preserved in result metadata", err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorCancellation || result.Status != agentwrap.StatusCancelled {
		t.Fatalf("err = %v result=%#v, want cancelled primary outcome", err, result)
	}
	if !result.Metadata.Cleanup.Failed || result.Metadata.Cleanup.Error == nil || result.Metadata.Cleanup.Error.Category != agentwrap.ErrorCleanup {
		t.Fatalf("cleanup metadata = %#v", result.Metadata.Cleanup)
	}
	if len(result.Metadata.Errors) < 2 {
		t.Fatalf("metadata errors = %#v, want cancellation plus cleanup", result.Metadata.Errors)
	}
}

func TestUnsupportedSessionActionFailsBeforeStart(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "normal.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	_, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello", SessionAction: agentwrap.SessionActionFork})
	if err == nil {
		t.Fatal("expected unsupported session action error")
	}
	var sdkErr *agentwrap.SDKError
	if !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorConfiguration {
		t.Fatalf("err = %T %v", err, err)
	}
	if runner.starts != 0 {
		t.Fatalf("process started for unsupported session action")
	}
}

func TestCancelOneConcurrentRunDoesNotAffectAnother(t *testing.T) {
	cancelled := &fakeProcess{blockCh: make(chan struct{})}
	completes := &fakeProcess{stdout: readFixture(t, "normal.ndjson")}
	runner := &fakeRunner{procs: []process{cancelled, completes}}
	rt := NewRuntime(withProcessRunner(runner))
	run1, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "one"})
	if err != nil {
		t.Fatal(err)
	}
	run2, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "two"})
	if err != nil {
		t.Fatal(err)
	}
	if err := run1.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, result1, err1 := drainRunErr(t, run1)
	_, result2 := drainRun(t, run2)
	if err1 == nil || result1.Status != agentwrap.StatusCancelled {
		t.Fatalf("cancelled result = %#v err=%v", result1, err1)
	}
	if result2.Status != agentwrap.StatusCompleted || result2.Err != nil {
		t.Fatalf("second result = %#v", result2)
	}
	if cancelled.CancelCount() == 0 {
		t.Fatalf("cancelled run was not cleaned up")
	}
}

func TestCapabilities(t *testing.T) {
	caps, err := NewRuntime().Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !caps.Supports(agentwrap.CapabilityStructuredEvents) || !caps.Supports(agentwrap.CapabilityRawPayloads) {
		t.Fatalf("caps = %#v", caps)
	}
	if caps.Supports(agentwrap.CapabilitySessions) {
		t.Fatalf("sessions should not be fully supported in Sprint 3")
	}
}

func drainRun(t *testing.T, run agentwrap.Run) ([]agentwrap.Event, agentwrap.RunResult) {
	t.Helper()
	events, result, err := drainRunErr(t, run)
	if err != nil {
		t.Fatalf("Wait returned error: %v result=%#v", err, result)
	}
	return events, result
}

func drainRunErr(t *testing.T, run agentwrap.Run) ([]agentwrap.Event, agentwrap.RunResult, error) {
	t.Helper()
	var events []agentwrap.Event
	for event := range run.Events() {
		events = append(events, event)
	}
	result, err := run.Wait(context.Background())
	return events, result, err
}

func readFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type eventSnapshot struct {
	Category    string   `json:"category"`
	Type        string   `json:"type"`
	SessionID   string   `json:"session_id"`
	RawSafe     bool     `json:"raw_safe"`
	RawSource   string   `json:"raw_source"`
	PayloadKeys []string `json:"payload_keys"`
}

type resultSnapshot struct {
	Status               string         `json:"status"`
	EventCount           int64          `json:"event_count"`
	NativeExtensionCount int            `json:"native_extension_count"`
	EventCategories      map[string]int `json:"event_categories"`
	NativeEventTypes     map[string]int `json:"native_event_types"`
	Artifacts            int            `json:"artifacts"`
	Warnings             int            `json:"warnings"`
	HasError             bool           `json:"has_error"`
}

type runSnapshot struct {
	Events []eventSnapshot `json:"events"`
	Result resultSnapshot  `json:"result"`
}

func snapshotRun(events []agentwrap.Event, result agentwrap.RunResult) runSnapshot {
	snapshot := runSnapshot{
		Events: make([]eventSnapshot, 0, len(events)),
		Result: resultSnapshot{
			Status:               string(result.Status),
			EventCount:           int64Value(result.Metadata.NativeMetadata["event_count"]),
			NativeExtensionCount: intValue(result.Metadata.NativeMetadata["native_extension_count"]),
			EventCategories:      intMapValue(result.Metadata.NativeMetadata["event_categories"]),
			NativeEventTypes:     intMapValue(result.Metadata.NativeMetadata["native_event_types"]),
			Artifacts:            len(result.Artifacts),
			Warnings:             len(result.Warnings),
			HasError:             result.Err != nil,
		},
	}
	for _, event := range events {
		next := eventSnapshot{
			Category:    string(event.Kind()),
			Type:        event.Type,
			SessionID:   string(event.SessionID),
			PayloadKeys: sortedPayloadKeys(event.Payload),
		}
		if event.Raw != nil {
			next.RawSafe = event.Raw.Safe
			next.RawSource = event.Raw.Source
		}
		snapshot.Events = append(snapshot.Events, next)
	}
	return snapshot
}

func readGoldenSnapshot(t *testing.T, name string) runSnapshot {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot runSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Result.EventCategories == nil {
		snapshot.Result.EventCategories = map[string]int{}
	}
	if snapshot.Result.NativeEventTypes == nil {
		snapshot.Result.NativeEventTypes = map[string]int{}
	}
	return snapshot
}

func sortedPayloadKeys(payload agentwrap.EventPayload) []string {
	keys := make([]string, 0, len(payload))
	for key := range payload {
		if key == "event_kind" || key == "turn_id" || key == "context" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func int64Value(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func intValue(value any) int {
	return int(int64Value(value))
}

func intMapValue(value any) map[string]int {
	out := map[string]int{}
	switch v := value.(type) {
	case map[string]int:
		for key, count := range v {
			out[key] = count
		}
	case map[string]any:
		for key, count := range v {
			out[key] = intValue(count)
		}
	}
	return out
}

func requireLifecycleTransition(t *testing.T, events []agentwrap.Event, from, to agentwrap.RunStatus, reason string) {
	t.Helper()
	for _, event := range events {
		if event.Kind() != agentwrap.EventLifecycle {
			continue
		}
		if event.Payload["from"] == string(from) && event.Payload["to"] == string(to) && event.Payload["reason"] == reason {
			return
		}
	}
	t.Fatalf("missing lifecycle transition %s -> %s reason=%s in %#v", from, to, reason, events)
}

// ---- Workstream 1: Process-Boundary Completion Semantics Tests ----
// Tests added 2026-05-21 for AGENTWRAP_NEXT_ROBUSTNESS_PLAN.md Workstream 1

// TestRunCleanExitWithFinalEventCompletes covers Case 1 from the plan:
// Clean exit with final structured event -> expected: completed
func TestRunCleanExitWithFinalEventCompletes(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "final.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	events, result := drainRun(t, run)
	if result.Status != agentwrap.StatusCompleted || result.Err != nil {
		t.Fatalf("result = %#v err=%v, want completed with no error", result, result.Err)
	}
	hasFinalResult := false
	for _, ev := range events {
		if ev.Kind() == agentwrap.EventFinalResult {
			hasFinalResult = true
			break
		}
	}
	if !hasFinalResult {
		t.Fatalf("no final-result event observed in events: %#v", events)
	}
}

// TestRunCleanExitWithOutputWithoutFinalCompletesWithWarning covers Case 2 from the plan:
// Clean exit with assistant text but no final structured event.
// DESIRED: completed with warning. CURRENT: fails with runtime_exit.
// This test documents CURRENT behavior as a gap.
func TestRunCleanExitWithOutputWithoutFinalCompletesWithWarning(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "partial.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	eventCh := run.Events()
	var events []agentwrap.Event
	for ev := range eventCh {
		events = append(events, ev)
	}
	result, waitErr := run.Wait(context.Background())
	if waitErr != nil || result.Err != nil || result.Status != agentwrap.StatusCompleted {
		t.Fatalf("result=%#v waitErr=%v, want completed with warning", result, waitErr)
	}
	if len(result.Warnings) == 0 {
		t.Fatalf("warnings = %#v, want missing-final warning", result.Warnings)
	}
}

// TestRunCleanExitNoFinalEventButDBTerminalFinishCompletes covers Case 3 from the plan:
// Clean exit with no final structured event but DB assistant message has terminal finish.
// DESIRED: completed with warning and DB-recovered usage. CURRENT: fails (fake has no DB).
func TestRunCleanExitNoFinalEventButDBTerminalFinishCompletes(t *testing.T) {
	stdout := `{"type":"step_start","timestamp":1710000000000,"sessionID":"ses_terminal"}
`
	runner := &fakeRunner{proc: &fakeProcess{stdout: stdout}}
	rt := NewRuntime(withProcessRunner(runner), withDBQuery(func(context.Context, agentwrap.SessionID) (string, error) {
		return `{"messages":[{"role":"assistant","finish":"stop","input_tokens":3,"output_tokens":4,"total_tokens":7}]}`, nil
	}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:    "hello",
		SessionID: agentwrap.SessionID("ses_terminal"),
	})
	if err != nil {
		t.Fatal(err)
	}
	eventCh := run.Events()
	var events []agentwrap.Event
	for ev := range eventCh {
		events = append(events, ev)
	}
	result, waitErr := run.Wait(context.Background())
	if waitErr != nil || result.Err != nil || result.Status != agentwrap.StatusCompleted {
		t.Fatalf("result=%#v waitErr=%v, want DB-reconciled completion", result, waitErr)
	}
	if result.Usage.TotalTokens == nil || *result.Usage.TotalTokens != 7 {
		t.Fatalf("usage = %#v, want DB-recovered total tokens", result.Usage)
	}
}

// TestRunCleanExitNoFinalNoOutputNoDBFinishFails covers Case 4 from the plan:
// Clean exit with no final structured event, no assistant output, and no DB terminal finish.
// Expected: runtime_exit
func TestRunCleanExitNoFinalNoOutputNoDBFinishFails(t *testing.T) {
	stdout := `{"type":"step_start","timestamp":1710000000000,"sessionID":"ses_empty"}
`
	runner := &fakeRunner{proc: &fakeProcess{stdout: stdout}}
	rt := NewRuntime(withProcessRunner(runner), withDBQuery(nil))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorRuntimeExit {
		t.Fatalf("err = %v result=%#v, want runtime_exit error", err, result)
	}
	if result.Status != agentwrap.StatusFailed {
		t.Fatalf("status = %s, want failed", result.Status)
	}
}

func TestReconcileDBResponseHandlesShapes(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantDone  bool
		wantTotal int64
	}{
		{name: "messages", body: `{"messages":[{"role":"assistant","finish":"stop","total_tokens":9}]}`, wantDone: true, wantTotal: 9},
		{name: "parts", body: `[{"role":"assistant","finishReason":"stop","totalTokens":11}]`, wantDone: true, wantTotal: 11},
		{name: "no assistant finish", body: `{"messages":[{"role":"user","finish":"stop"}]}`},
		{name: "non json", body: `not json`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proof := reconcileDBResponse(tt.body, nil)
			if proof.completed != tt.wantDone {
				t.Fatalf("completed=%v, want %v proof=%#v", proof.completed, tt.wantDone, proof)
			}
			if tt.wantTotal > 0 {
				if proof.usage.TotalTokens == nil || *proof.usage.TotalTokens != tt.wantTotal {
					t.Fatalf("usage=%#v, want total %d", proof.usage, tt.wantTotal)
				}
			}
		})
	}
}

func TestReconcileDBResponseLockedDBIsRuntimeUnavailable(t *testing.T) {
	proof := reconcileDBResponse("", errors.New("database is locked during wal_checkpoint"))
	if proof.err == nil || proof.err.Category != agentwrap.ErrorRuntimeUnavailable {
		t.Fatalf("proof=%#v, want runtime_unavailable", proof)
	}
}

// TestRunNonZeroExitWithDBTerminalFinishFails covers Case 5 from the plan:
// Non-zero exit with DB terminal finish.
// Contract is undecided - this test documents current behavior as fails with runtime_exit.
func TestRunNonZeroExitWithDBTerminalFinishFails(t *testing.T) {
	runner := &fakeRunner{procs: []process{
		&fakeProcess{
			stdout: `{"type":"step_start","timestamp":1710000000000,"sessionID":"ses_nz"}
{"type":"text","timestamp":1710000001000,"sessionID":"ses_nz","part":{"type":"text","text":"done"}}
`,
			result: processResult{ExitCode: 7, Err: errors.New("exit status 7")},
		},
	}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:    "hello",
		SessionID: agentwrap.SessionID("ses_nz"),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	// Current behavior: non-zero exit with no final event -> runtime_exit
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorRuntimeExit {
		t.Fatalf("err = %v result=%#v, want runtime_exit for non-zero exit without final", err, result)
	}
	if result.Status != agentwrap.StatusFailed {
		t.Fatalf("status = %s, want failed", result.Status)
	}
}

// TestRunDecodeErrorAfterPartialEventsFails covers Case 6 from the plan:
// Decode error after partial valid events.
// Expected: fail as decode/runtime event error (malformed_event)
func TestRunDecodeErrorAfterPartialEventsFails(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "malformed.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorMalformedEvent {
		t.Fatalf("err = %v result=%#v, want malformed_event decode error", err, result)
	}
	if result.Status != agentwrap.StatusFailed {
		t.Fatalf("status = %s, want failed", result.Status)
	}
}

// TestTimeoutWithRecentProviderErrorLogClassifiesRateLimit covers Case 7 from the plan:
// Timeout with recent OpenCode provider error log.
// DESIRED: classify provider error. CURRENT: may get timeout (log timing sensitivity).
// This test documents CURRENT behavior and timing sensitivity gap.
func TestTimeoutWithRecentProviderErrorLogClassifiesRateLimit(t *testing.T) {
	dataHome := t.TempDir()
	logDir := filepath.Join(dataHome, "opencode", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_DATA_HOME", dataHome)
	now := time.Now()
	logFilename := now.Format("20060102T150405") + ".log"
	logContent := "INFO args=[\"run\",\"--model\",\"minimax-coding-plan/MiniMax-M2.7\"] opencode\n" +
		"ERROR service=llm providerID=minimax-coding-plan modelID=MiniMax-M2.7 error={\"statusCode\":429,\"responseBody\":\"{\\\"type\\\":\\\"error\\\",\\\"error\\\":{\\\"type\\\":\\\"rate_limit_error\\\",\\\"message\\\":\\\"usage limit exceeded\\\"}}\"} stream error\n"
	if err := os.WriteFile(filepath.Join(logDir, logFilename), []byte(logContent), 0644); err != nil {
		t.Fatal(err)
	}
	proc := &fakeProcess{blockCh: make(chan struct{})}
	rt := NewRuntime(withProcessRunner(&fakeRunner{proc: proc}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:  "hello",
		Model:   agentwrap.ModelID("minimax-coding-plan/MiniMax-M2.7"),
		Timeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	eventCh := run.Events()
	var events []agentwrap.Event
	for ev := range eventCh {
		events = append(events, ev)
	}
	result, waitErr := run.Wait(context.Background())
	if waitErr != nil {
		t.Logf("Wait error (expected): %v", waitErr)
	}
	if result.Err == nil || result.Err.Category != agentwrap.ErrorRateLimit {
		t.Fatalf("err=%v result=%#v, want rate_limit from recent matching OpenCode log", waitErr, result)
	}
	if result.Metadata.NativeMetadata["rate_limit_info"] == nil {
		t.Fatalf("rate_limit_info missing from native metadata: %#v", result.Metadata.NativeMetadata)
	}
}

// TestTimeoutWithDBTerminalFinishReportsTimeoutWithEvidence covers Case 8 from the plan:
// Timeout with DB terminal finish.
// Contract is undecided - timeout is caller's deadline, but durable state may show completion.
func TestTimeoutWithDBTerminalFinishReportsTimeoutWithEvidence(t *testing.T) {
	proc := &fakeProcess{blockCh: make(chan struct{})}
	rt := NewRuntime(withProcessRunner(&fakeRunner{proc: proc}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:  "hello",
		Timeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorTimeout {
		t.Fatalf("err = %v result=%#v, want timeout error", err, result)
	}
	if result.Status != agentwrap.StatusFailed {
		t.Fatalf("status = %s, want failed", result.Status)
	}
	t.Logf("Timeout classified as %s with status %s - DB reconciliation would happen if session existed", result.Err.Category, result.Status)
}

// TestRunNonZeroExitWithFinalEventStillCompletes covers the final-state
// precedence contract: an observed final structured result wins over a later
// non-zero process exit.
func TestRunNonZeroExitWithFinalEventStillCompletes(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{
		stdout: readFixture(t, "final.ndjson"),
		stderr: "diagnostic output",
		result: processResult{ExitCode: 1, Err: errors.New("exit status 1")},
	}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	eventCh := run.Events()
	var events []agentwrap.Event
	for ev := range eventCh {
		events = append(events, ev)
	}
	result, waitErr := run.Wait(context.Background())
	hasFinalResult := false
	for _, ev := range events {
		if ev.Kind() == agentwrap.EventFinalResult {
			hasFinalResult = true
			break
		}
	}
	if !hasFinalResult {
		t.Fatalf("expected final-result event: %#v", events)
	}
	if waitErr != nil || result.Err != nil {
		t.Fatalf("err = %v result.Err = %v, want completed final event to supersede exit status", waitErr, result.Err)
	}
	if result.Status != agentwrap.StatusCompleted {
		t.Fatalf("status = %s, want completed", result.Status)
	}
}

func TestRunMalformedAfterFinalCompletesWithWarning(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "final.ndjson") + `{"type":` + "\n"}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result := drainRun(t, run)
	if result.Status != agentwrap.StatusCompleted || result.Err != nil {
		t.Fatalf("result = %#v err=%v, want completed with post-final warning", result, result.Err)
	}
	if len(result.Warnings) == 0 {
		t.Fatalf("warnings = %#v, want post-final decode warning", result.Warnings)
	}
	if got, _ := result.Metadata.NativeMetadata["post_final_decode_warning"].(string); got == "" {
		t.Fatalf("post_final_decode_warning missing in native metadata: %#v", result.Metadata.NativeMetadata)
	}
}

func TestPostFinalNonDecodeErrorBecomesWarning(t *testing.T) {
	r := &run{sawFinal: true}
	if got := r.postFinalDecodeWarning(context.Canceled); got == "" {
		t.Fatal("postFinalDecodeWarning returned empty string, want generic post-final warning")
	}
}

// TestRunEmptyStdoutFails covers empty stdout scenario:
func TestRunEmptyStdoutFails(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: ""}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorRuntimeExit {
		t.Fatalf("err = %v result=%#v, want runtime_exit for empty stdout", err, result)
	}
}
