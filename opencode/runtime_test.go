package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/antonioborgerees/agentwrap"
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

func TestPolicyRunnerWrapsOpenCodeRuntimeWithoutAdapterRetry(t *testing.T) {
	runner := &fakeRunner{procs: []process{
		&fakeProcess{stdout: readFixture(t, "normal.ndjson"), stderr: "temporary", result: processResult{ExitCode: 1}},
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

func TestRunPartialWithoutFinalFails(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: readFixture(t, "partial.ndjson")}}
	rt := NewRuntime(withProcessRunner(runner))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := drainRunErr(t, run)
	if err == nil || result.Err == nil || result.Err.Category != agentwrap.ErrorRuntimeExit {
		t.Fatalf("err = %v result=%#v", err, result)
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
	requireLifecycleTransition(t, events, agentwrap.StatusCancelled, agentwrap.StatusCompleted, "caller_cancel")
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

func TestCleanupFailureDoesNotOverwritePrimarySuccess(t *testing.T) {
	proc := &fakeProcess{stdout: readFixture(t, "normal.ndjson"), cancel: errors.New("cleanup failed")}
	rt := NewRuntime(withProcessRunner(&fakeRunner{proc: proc}))
	run, err := rt.StartRun(context.Background(), agentwrap.RunRequest{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, result := drainRun(t, run)
	if result.Status != agentwrap.StatusCompleted || result.Err != nil {
		t.Fatalf("primary result changed: %#v", result)
	}
	if !result.Metadata.Cleanup.Failed || result.Metadata.Cleanup.Error == nil || result.Metadata.Cleanup.Error.Category != agentwrap.ErrorCleanup {
		t.Fatalf("cleanup metadata = %#v", result.Metadata.Cleanup)
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
