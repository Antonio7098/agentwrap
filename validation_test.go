package agentwrap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestValidateRunBuiltIns(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "report.md"), "# Summary\n\nDone\n\n## Details\n\nComplete\n")
	mustWriteFile(t, filepath.Join(dir, "template.md"), "# Summary\n\n## Details\n")
	mustWriteFile(t, filepath.Join(dir, "data.json"), `{"name":"agentwrap","ok":true}`)
	if err := os.Mkdir(filepath.Join(dir, "artifacts"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	result := RunResult{
		Artifacts: []ArtifactRef{{ID: "report", URI: "report.md"}},
		Metadata:  RunMetadata{NativeMetadata: map[string]any{"model": "gpt"}},
	}
	spec := ValidationSpec{Expectations: []ValidationExpectation{
		{ID: "file", Kind: ExpectationFile, Path: "report.md"},
		{ID: "dir", Kind: ExpectationDirectory, Path: "artifacts"},
		{ID: "artifact", Kind: ExpectationArtifact, ArtifactID: "report"},
		{ID: "markdown", Kind: ExpectationMarkdownTemplate, Path: "report.md", TemplatePath: "template.md"},
		{ID: "json", Kind: ExpectationJSON, Path: "data.json", RequiredFields: []string{"name", "ok"}},
		{ID: "metadata", Kind: ExpectationMetadata, MetadataKey: "model"},
	}}

	validation := ValidateRun(context.Background(), RunRequest{WorkDir: dir}, result, spec)
	if !validation.Passed {
		t.Fatalf("validation failed: %#v", validation.Failures)
	}
	if validation.PassedCount != 6 {
		t.Fatalf("passed count = %d, want 6", validation.PassedCount)
	}
}

func TestValidateRunReportsMarkdownAndJSONFailures(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "report.md"), "# Summary\n\nTODO\n")
	mustWriteFile(t, filepath.Join(dir, "template.md"), "# Summary\n\n## Required\n")
	mustWriteFile(t, filepath.Join(dir, "data.json"), `{"name":`)

	spec := ValidationSpec{Expectations: []ValidationExpectation{
		{ID: "markdown", Kind: ExpectationMarkdownTemplate, Path: "report.md", TemplatePath: "template.md", RepairHint: "add required sections"},
		{ID: "json", Kind: ExpectationJSON, Path: "data.json", RequiredFields: []string{"name"}},
	}}
	validation := ValidateRun(context.Background(), RunRequest{WorkDir: dir}, RunResult{}, spec)
	if validation.Passed {
		t.Fatal("validation passed, want failures")
	}
	if validation.FailedCount != 2 {
		t.Fatalf("failed count = %d, want 2", validation.FailedCount)
	}
	if !strings.Contains(validation.Failures[0].Observed, "Required") {
		t.Fatalf("markdown observed = %q, want missing heading detail", validation.Failures[0].Observed)
	}
	if validation.Failures[1].Observed != "invalid JSON" {
		t.Fatalf("json observed = %q, want invalid JSON", validation.Failures[1].Observed)
	}
}

func TestValidateRunCallerDefinedValidator(t *testing.T) {
	spec := ValidationSpec{Validators: []Validator{ValidatorFunc(func(context.Context, ValidationContext) ValidationCheck {
		return ValidationCheck{ExpectationID: "custom", Kind: ExpectationCustom, Passed: false, Expected: "ok", Observed: "bad"}
	})}}
	validation := ValidateRun(context.Background(), RunRequest{}, RunResult{}, spec)
	if validation.Passed || len(validation.Failures) != 1 || validation.Failures[0].ExpectationID != "custom" {
		t.Fatalf("validation = %#v, want custom failure", validation)
	}
}

func TestValidatingRuntimeFailsSuccessfulRuntimeOnMissingOutput(t *testing.T) {
	inner := &validationScriptRuntime{results: []validationScriptResult{{}}}
	runner := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{Expectations: []ValidationExpectation{{ID: "missing", Kind: ExpectationFile, Path: "missing.txt"}}}}
	run, err := runner.StartRun(context.Background(), RunRequest{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err == nil {
		t.Fatal("Wait error = nil, want validation error")
	}
	if result.Err == nil || result.Err.Category != ErrorValidation {
		t.Fatalf("error = %#v, want validation", result.Err)
	}
	if !hasCategory(collectValidationEvents(run), EventValidation) {
		t.Fatalf("missing validation event")
	}
}

func TestValidatingRuntimeRepairsThenSucceeds(t *testing.T) {
	dir := t.TempDir()
	inner := &validationScriptRuntime{onStart: func(start int, req RunRequest) {
		if start == 2 {
			mustWriteFile(t, filepath.Join(dir, "out.txt"), "fixed")
		}
	}, results: []validationScriptResult{{}, {session: SessionMetadata{Relationship: SessionRelationshipSame}}}}
	runner := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{
		Expectations: []ValidationExpectation{{ID: "out", Kind: ExpectationFile, Path: "out.txt"}},
		Repair:       RepairConfig{MaxAttempts: 1},
	}}
	run, err := runner.StartRun(context.Background(), RunRequest{WorkDir: dir, PermissionPolicy: &PermissionPolicy{Default: PermissionActionDeny}})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.Status != StatusCompleted || len(result.Metadata.Repair.Attempts) != 1 {
		t.Fatalf("result = %#v, want repaired completion", result)
	}
	if inner.requests[1].SessionAction != SessionActionContinue {
		t.Fatalf("repair session action = %q, want continue", inner.requests[1].SessionAction)
	}
	if inner.requests[1].PermissionPolicy == nil || inner.requests[1].PermissionPolicy.Summary().ID == "" {
		t.Fatalf("repair request did not inherit permission policy")
	}
}

func TestValidatingRuntimeRepairExhaustion(t *testing.T) {
	inner := &validationScriptRuntime{results: []validationScriptResult{{}, {}}}
	runner := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{
		Expectations: []ValidationExpectation{{ID: "out", Kind: ExpectationFile, Path: "missing.txt"}},
		Repair:       RepairConfig{MaxAttempts: 1},
	}}
	run, err := runner.StartRun(context.Background(), RunRequest{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err == nil {
		t.Fatal("Wait error = nil, want repair exhausted")
	}
	if result.Err == nil || result.Err.Category != ErrorRepairExhausted {
		t.Fatalf("error = %#v, want repair exhausted", result.Err)
	}
	if !result.Metadata.Repair.Exhausted {
		t.Fatalf("repair metadata = %#v, want exhausted", result.Metadata.Repair)
	}
}

func TestValidatingRuntimePermissionDeniedDuringRepair(t *testing.T) {
	permissionErr := NewError(ErrorPermission, "repair", "permission denied", nil)
	inner := &validationScriptRuntime{results: []validationScriptResult{{}, {err: permissionErr}}}
	runner := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{
		Expectations: []ValidationExpectation{{ID: "out", Kind: ExpectationFile, Path: "missing.txt"}},
		Repair:       RepairConfig{MaxAttempts: 1},
	}}
	run, err := runner.StartRun(context.Background(), RunRequest{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err == nil {
		t.Fatal("Wait error = nil, want permission error")
	}
	if result.Err == nil || result.Err.Category != ErrorPermission {
		t.Fatalf("error = %#v, want permission", result.Err)
	}
	if !result.Metadata.Repair.PermissionDenied {
		t.Fatalf("repair metadata = %#v, want permission denied", result.Metadata.Repair)
	}
}

func TestValidatingRuntimeCancellationDuringValidation(t *testing.T) {
	cancelOnValidate := ValidatorFunc(func(ctx context.Context, _ ValidationContext) ValidationCheck {
		if cancel, ok := ctx.Value(cancelContextKey{}).(context.CancelFunc); ok {
			cancel()
		}
		return ValidationCheck{ExpectationID: "custom", Kind: ExpectationCustom, Passed: true}
	})
	inner := &validationScriptRuntime{results: []validationScriptResult{{}}}
	runner := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{Validators: []Validator{cancelOnValidate}}}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, cancelContextKey{}, context.CancelFunc(cancel))
	run, err := runner.StartRun(ctx, RunRequest{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err == nil {
		t.Fatal("Wait error = nil, want cancellation")
	}
	if result.Err == nil || result.Err.Category != ErrorCancellation {
		t.Fatalf("error = %#v, want cancellation", result.Err)
	}
	if len(result.Metadata.Repair.Attempts) != 0 {
		t.Fatalf("repair attempts = %#v, want none after validation cancellation", result.Metadata.Repair.Attempts)
	}
}

func TestPolicyContextReceivesValidationResult(t *testing.T) {
	inner := &validationScriptRuntime{results: []validationScriptResult{{}}}
	validating := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{Expectations: []ValidationExpectation{{ID: "out", Kind: ExpectationFile, Path: "missing.txt"}}}}
	policy := captureValidationPolicy{}
	runner := PolicyRunner{Runtime: validating, Policy: &policy}
	run, err := runner.StartRun(context.Background(), RunRequest{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	_, _ = run.Wait(context.Background())
	if policy.validation == nil || policy.validation.Passed {
		t.Fatalf("policy validation = %#v, want failed validation result", policy.validation)
	}
}

func TestValidatingRuntimeUnsupportedSameSessionRepair(t *testing.T) {
	inner := &validationScriptRuntime{results: []validationScriptResult{
		{},
		{session: SessionMetadata{RequestedAction: SessionActionContinue, Relationship: SessionRelationshipUnsupported, UnsupportedReason: "continue unsupported"}},
	}}
	runner := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{
		Expectations: []ValidationExpectation{{ID: "out", Kind: ExpectationFile, Path: "missing.txt"}},
		Repair:       RepairConfig{MaxAttempts: 1},
	}}
	run, err := runner.StartRun(context.Background(), RunRequest{WorkDir: t.TempDir(), SessionID: "s1"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err == nil {
		t.Fatal("Wait error = nil, want unsupported repair error")
	}
	if result.Err == nil || result.Err.Category != ErrorRepairExhausted {
		t.Fatalf("error = %#v, want repair exhausted", result.Err)
	}
	if !result.Metadata.Repair.UnsupportedSameSession {
		t.Fatalf("repair metadata = %#v, want unsupported same session", result.Metadata.Repair)
	}
	if result.Metadata.Repair.Attempts[0].Status != StatusFailed || result.Metadata.Repair.Attempts[0].ErrorCategory != ErrorRepairExhausted {
		t.Fatalf("repair attempt = %#v, want failed repair-exhausted summary", result.Metadata.Repair.Attempts[0])
	}
}

func TestValidatingRuntimeFreshSessionFallbackAfterUnsupportedSameSessionRepair(t *testing.T) {
	dir := t.TempDir()
	inner := &validationScriptRuntime{onStart: func(start int, req RunRequest) {
		if start == 3 {
			mustWriteFile(t, filepath.Join(dir, "out.txt"), "fixed")
		}
	}, results: []validationScriptResult{
		{},
		{session: SessionMetadata{RequestedAction: SessionActionContinue, Relationship: SessionRelationshipUnsupported, UnsupportedReason: "continue unsupported"}},
		{session: SessionMetadata{RequestedAction: SessionActionFresh, Relationship: SessionRelationshipFresh}},
	}}
	runner := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{
		Expectations: []ValidationExpectation{{ID: "out", Kind: ExpectationFile, Path: "out.txt"}},
		Repair:       RepairConfig{MaxAttempts: 1, AllowFreshSessionFallback: true},
	}}
	run, err := runner.StartRun(context.Background(), RunRequest{WorkDir: dir, SessionID: "s1"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", result.Status)
	}
	if len(inner.requests) != 3 || inner.requests[2].SessionAction != SessionActionFresh {
		t.Fatalf("requests = %#v, want fresh fallback repair", inner.requests)
	}
	if !result.Metadata.Repair.Attempts[0].UnsupportedSession {
		t.Fatalf("repair attempt = %#v, want unsupported-session fallback recorded", result.Metadata.Repair.Attempts[0])
	}
	if !result.Metadata.Repair.Attempts[0].Validation.Passed {
		t.Fatalf("repair validation = %#v, want passed validation recorded", result.Metadata.Repair.Attempts[0].Validation)
	}
}

type captureValidationPolicy struct {
	validation *ValidationResult
}

func (p *captureValidationPolicy) Decide(ctx context.Context, policyCtx PolicyContext) (PolicyDecision, error) {
	p.validation = policyCtx.Validation
	return PolicyDecision{Kind: PolicyDecisionStop, Err: policyCtx.Err}, nil
}

func TestValidatingRuntimeCancellationDuringRepair(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	inner := &validationScriptRuntime{results: []validationScriptResult{
		{},
		{wait: func(context.Context) {
			close(started)
			<-release
		}},
	}}
	runner := ValidatingRuntime{Runtime: inner, Spec: ValidationSpec{
		Expectations: []ValidationExpectation{{ID: "out", Kind: ExpectationFile, Path: "missing.txt"}},
		Repair:       RepairConfig{MaxAttempts: 1},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	run, err := runner.StartRun(ctx, RunRequest{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	<-started
	cancel()
	close(release)
	result, err := run.Wait(context.Background())
	if err == nil {
		t.Fatal("Wait error = nil, want cancellation")
	}
	if result.Err == nil || result.Err.Category != ErrorCancellation {
		t.Fatalf("error = %#v, want cancellation", result.Err)
	}
}

type validationScriptRuntime struct {
	mu       sync.Mutex
	results  []validationScriptResult
	requests []RunRequest
	onStart  func(int, RunRequest)
}

func (r *validationScriptRuntime) StartRun(ctx context.Context, req RunRequest) (Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req)
	start := len(r.requests)
	if r.onStart != nil {
		r.onStart(start, req)
	}
	result := validationScriptResult{}
	if start-1 < len(r.results) {
		result = r.results[start-1]
	}
	return &validationScriptRun{
		id:       RunID("validation-script-" + string(rune('0'+start))),
		req:      req,
		startCtx: ctx,
		result:   result,
		events:   make(chan Event),
		done:     make(chan struct{}),
	}, nil
}

func (r *validationScriptRuntime) Capabilities(context.Context) (Capabilities, error) {
	return Capabilities{RuntimeKind: "validation-script", Features: map[Capability]CapabilitySupport{}}, nil
}

type validationScriptResult struct {
	err     *SDKError
	session SessionMetadata
	wait    func(context.Context)
}

type validationScriptRun struct {
	id       RunID
	req      RunRequest
	startCtx context.Context
	result   validationScriptResult
	events   chan Event
	done     chan struct{}
	once     sync.Once
}

func (r *validationScriptRun) ID() RunID { return r.id }

func (r *validationScriptRun) Events() <-chan Event {
	r.start()
	return r.events
}

func (r *validationScriptRun) Wait(ctx context.Context) (RunResult, error) {
	r.start()
	select {
	case <-r.done:
	case <-ctx.Done():
		sdkErr := NewError(ErrorCancellation, "validation script", "cancelled", ctx.Err())
		return RunResult{RunID: r.id, Status: StatusCancelled, Err: sdkErr}, sdkErr
	}
	if err := ctx.Err(); err != nil {
		sdkErr := NewError(ErrorCancellation, "validation script", "cancelled", err)
		return RunResult{RunID: r.id, Status: StatusCancelled, Err: sdkErr}, sdkErr
	}
	status := StatusCompleted
	if r.result.err != nil {
		status = StatusFailed
	}
	session := r.result.session
	if session.Relationship == "" {
		session = SessionMetadata{ID: r.req.SessionID, RequestedID: r.req.SessionID, RequestedAction: r.req.SessionAction, Relationship: SessionRelationshipSame}
	}
	now := time.Now().UTC()
	result := RunResult{
		RunID:      r.id,
		SessionID:  session.ID,
		Status:     status,
		StartedAt:  now,
		FinishedAt: now,
		Err:        r.result.err,
		Metadata:   RunMetadata{Status: status, Session: session, StartedAt: now, FinishedAt: now},
	}
	if r.result.err != nil {
		return result, r.result.err
	}
	return result, nil
}

func (r *validationScriptRun) Cancel(context.Context) error { return nil }

func (r *validationScriptRun) start() {
	r.once.Do(func() {
		go func() {
			defer close(r.events)
			defer close(r.done)
			if r.result.wait != nil {
				r.result.wait(r.startCtx)
			}
		}()
	})
}

type cancelContextKey struct{}

func collectValidationEvents(run Run) []Event {
	events := []Event{}
	for event := range run.Events() {
		events = append(events, event)
	}
	return events
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
