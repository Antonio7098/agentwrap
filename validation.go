package agentwrap

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var validationRunCounter atomic.Int64

// ExpectationKind identifies a built-in or caller-defined validation shape.
type ExpectationKind string

const (
	ExpectationFile             ExpectationKind = "file"
	ExpectationDirectory        ExpectationKind = "directory"
	ExpectationArtifact         ExpectationKind = "artifact"
	ExpectationMarkdownTemplate ExpectationKind = "markdown_template"
	ExpectationJSON             ExpectationKind = "json"
	ExpectationMetadata         ExpectationKind = "metadata"
	ExpectationCustom           ExpectationKind = "custom"
)

// ExpectationSeverity describes whether a failed expectation should fail the
// logical run. Sprint 8 treats empty severity as required.
type ExpectationSeverity string

const (
	ExpectationRequired ExpectationSeverity = "required"
	ExpectationOptional ExpectationSeverity = "optional"
)

// ValidationSpec configures output validation for a run.
type ValidationSpec struct {
	Expectations []ValidationExpectation
	Validators   []Validator
	Repair       RepairConfig
}

// ValidationExpectation describes one runtime-neutral output expectation.
type ValidationExpectation struct {
	ID             string
	Kind           ExpectationKind
	Severity       ExpectationSeverity
	Path           string
	ArtifactID     ArtifactID
	ArtifactURI    string
	TemplatePath   string
	MetadataKey    string
	RequiredFields []string
	RepairHint     string
}

// Validator is implemented by caller-defined validation checks.
type Validator interface {
	Validate(context.Context, ValidationContext) ValidationCheck
}

// ValidatorFunc adapts a function into a Validator.
type ValidatorFunc func(context.Context, ValidationContext) ValidationCheck

// Validate implements Validator.
func (f ValidatorFunc) Validate(ctx context.Context, vctx ValidationContext) ValidationCheck {
	return f(ctx, vctx)
}

// ValidationContext contains safe facts needed by validators.
type ValidationContext struct {
	Request RunRequest
	Result  RunResult
	WorkDir string
}

// ValidationResult records the aggregate outcome of a validation pass.
type ValidationResult struct {
	Passed       bool
	Skipped      bool
	PassedCount  int
	FailedCount  int
	SkippedCount int
	Checks       []ValidationCheck
	Failures     []ValidationFailure
	Errors       []SDKError
	Native       map[string]any
}

// ValidationCheck records one expectation outcome.
type ValidationCheck struct {
	ExpectationID string
	Kind          ExpectationKind
	Severity      ExpectationSeverity
	Passed        bool
	Skipped       bool
	Expected      string
	Observed      string
	Detail        string
	RepairHint    string
}

// ValidationFailure is the repair-friendly failure subset.
type ValidationFailure struct {
	ExpectationID string
	Kind          ExpectationKind
	Severity      ExpectationSeverity
	Expected      string
	Observed      string
	Detail        string
	RepairHint    string
}

// RepairConfig configures bounded repair after failed validation.
type RepairConfig struct {
	MaxAttempts               int
	SessionAction             SessionAction
	AllowFreshSessionFallback bool
	ShouldRepair              func(RepairContext) bool
	BuildPrompt               func(RepairContext) string
	OverrideRequest           func(RepairContext, RunRequest) RunRequest
}

// RepairContext contains safe repair prompt inputs.
type RepairContext struct {
	OriginalRequest RunRequest
	CurrentResult   RunResult
	Validation      ValidationResult
	Attempt         int
	MaxAttempts     int
}

// ValidatingRuntime wraps a Runtime with output validation and bounded repair.
type ValidatingRuntime struct {
	Runtime Runtime
	Spec    ValidationSpec
	Now     func() time.Time
}

// StartRun starts the wrapped runtime and validates the successful result.
func (r ValidatingRuntime) StartRun(ctx context.Context, req RunRequest) (Run, error) {
	if r.Runtime == nil {
		return nil, NewError(ErrorConfiguration, "validation runner", "runtime is required", nil)
	}
	spec := r.Spec
	if req.Validation != nil {
		spec = *req.Validation
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := &validationRun{
		id:       RunID(fmt.Sprintf("validation-%d", validationRunCounter.Add(1))),
		runtime:  r.Runtime,
		spec:     spec,
		original: req,
		ctx:      runCtx,
		cancel:   cancel,
		now:      r.Now,
		events:   make(chan Event, 64),
		done:     make(chan struct{}),
		started:  time.Now().UTC(),
	}
	if run.now == nil {
		run.now = func() time.Time { return time.Now().UTC() }
	}
	run.started = run.now()
	go run.execute()
	return run, nil
}

// Capabilities reports wrapped runtime capabilities with validation support.
func (r ValidatingRuntime) Capabilities(ctx context.Context) (Capabilities, error) {
	if r.Runtime == nil {
		return Capabilities{}, NewError(ErrorConfiguration, "validation runner capabilities", "runtime is required", nil)
	}
	caps, err := r.Runtime.Capabilities(ctx)
	if err != nil {
		return caps, err
	}
	if caps.Features == nil {
		caps.Features = map[Capability]CapabilitySupport{}
	}
	caps.Features[CapabilityValidationEvents] = CapabilitySupport{Supported: true, Detail: "runtime-neutral validation wrapper"}
	return caps, nil
}

type validationRun struct {
	id       RunID
	runtime  Runtime
	spec     ValidationSpec
	original RunRequest
	ctx      context.Context
	cancel   context.CancelFunc
	now      func() time.Time
	started  time.Time

	events chan Event
	done   chan struct{}

	mu      sync.Mutex
	current Run
	result  RunResult
	waitErr error
	seq     int64
}

func (r *validationRun) ID() RunID { return r.id }

func (r *validationRun) Events() <-chan Event { return r.events }

func (r *validationRun) Wait(ctx context.Context) (RunResult, error) {
	select {
	case <-ctx.Done():
		return RunResult{}, contextValidationError(ctx.Err())
	case <-r.done:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.waitErr != nil {
		return r.result, r.waitErr
	}
	return r.result, nil
}

func (r *validationRun) Cancel(ctx context.Context) error {
	r.cancel()
	r.mu.Lock()
	current := r.current
	r.mu.Unlock()
	if current != nil {
		return current.Cancel(ctx)
	}
	return nil
}

func (r *validationRun) execute() {
	defer close(r.done)
	defer close(r.events)
	defer r.cancel()

	req := r.original
	req.Validation = nil
	result, err := r.startAndWait(req)
	if err != nil || result.Err != nil {
		r.finish(result, firstSDKError(result.Err, err))
		return
	}
	history := []ValidationResult{}
	repairSummaries := []RepairAttemptSummary{}
	for attempt := 0; ; attempt++ {
		validation := r.validate(req, result)
		history = append(history, validation)
		result = withValidationMetadata(result, r.spec, history, repairSummaries)
		if validation.Passed {
			result.Status = StatusCompleted
			result.Err = nil
			r.finish(result, nil)
			return
		}
		repairCtx := RepairContext{OriginalRequest: r.original, CurrentResult: result, Validation: validation, Attempt: attempt + 1, MaxAttempts: r.spec.Repair.MaxAttempts}
		if !r.shouldRepair(repairCtx) {
			err := validationError(validation)
			if r.spec.Repair.MaxAttempts > 0 && len(repairSummaries) >= r.spec.Repair.MaxAttempts {
				err = NewError(ErrorRepairExhausted, "validation repair", "validation repair attempts exhausted", nil, WithMetadata(map[string]string{
					"max_attempts": fmt.Sprintf("%d", r.spec.Repair.MaxAttempts),
				}))
			}
			result.Status = StatusFailed
			result.Err = err
			r.finish(withValidationMetadata(result, r.spec, history, repairSummaries), err)
			return
		}
		repairResult, summary, repairErr := r.runRepair(req, repairCtx)
		repairSummaries = append(repairSummaries, summary)
		result = repairResult
		if repairErr != nil {
			result.Status = statusForError(repairErr)
			result.Err = repairErr
			r.finish(withValidationMetadata(result, r.spec, history, repairSummaries), repairErr)
			return
		}
	}
}

func (r *validationRun) startAndWait(req RunRequest) (RunResult, error) {
	started := r.now()
	run, err := r.runtime.StartRun(r.ctx, req)
	if err != nil {
		sdkErr := ensureSDKError(err, "validation start")
		return RunResult{RunID: r.id, Status: StatusFailed, StartedAt: started, FinishedAt: r.now(), Err: sdkErr}, sdkErr
	}
	r.setCurrent(run)
	eventsDone := make(chan struct{})
	go func() {
		defer close(eventsDone)
		r.forwardEvents(run.Events())
	}()
	result, waitErr := run.Wait(r.ctx)
	<-eventsDone
	r.clearCurrent(run)
	if waitErr != nil && result.Err == nil {
		result.Err = ensureSDKError(waitErr, "validation wait")
	}
	if result.Err != nil {
		return result, result.Err
	}
	return result, nil
}

func (r *validationRun) validate(req RunRequest, result RunResult) ValidationResult {
	r.sendLifecycle(result, StatusCompleted, StatusValidating, "validation started")
	r.sendEvent(Event{RunID: r.id, SessionID: result.SessionID, Type: "validation.started", Payload: EventPayloadWithKind(EventValidation, EventPayload{"phase": "started"})})
	validation := ValidateRun(r.ctx, req, result, r.spec)
	r.sendEvent(Event{RunID: r.id, SessionID: result.SessionID, Type: "validation.completed", Payload: EventPayloadWithKind(EventValidation, EventPayload{
		"passed":        validation.Passed,
		"passed_count":  validation.PassedCount,
		"failed_count":  validation.FailedCount,
		"skipped_count": validation.SkippedCount,
		"failures":      safeFailurePayload(validation.Failures),
	})})
	r.sendLifecycle(result, StatusValidating, StatusCompleted, "validation completed")
	return validation
}

func (r *validationRun) runRepair(req RunRequest, repairCtx RepairContext) (RunResult, RepairAttemptSummary, *SDKError) {
	repairReq := repairRequest(req, r.spec.Repair, repairCtx)
	r.sendLifecycle(repairCtx.CurrentResult, StatusValidating, StatusRepairing, "repair started")
	r.sendEvent(Event{RunID: r.id, SessionID: repairCtx.CurrentResult.SessionID, Type: "repair.started", Payload: EventPayloadWithKind(EventValidation, EventPayload{"attempt": repairCtx.Attempt})})
	started := r.now()
	result, err := r.startAndWait(repairReq)
	summary := RepairAttemptSummary{
		Attempt:            repairCtx.Attempt,
		RunID:              result.RunID,
		ParentRunID:        r.id,
		Request:            attemptRequest(repairReq),
		Status:             result.Status,
		StartedAt:          firstTime(result.StartedAt, started),
		FinishedAt:         firstTime(result.FinishedAt, r.now()),
		Session:            result.Metadata.Session,
		Permissions:        result.Metadata.Permissions,
		Error:              result.Err,
		PermissionPolicyID: permissionPolicyID(repairReq.PermissionPolicy),
	}
	summary.Duration = summary.FinishedAt.Sub(summary.StartedAt)
	if result.Err != nil {
		summary.ErrorCategory = result.Err.Category
	}
	if result.Metadata.Session.Relationship == SessionRelationshipUnsupported && repairReq.SessionAction == SessionActionContinue {
		summary.UnsupportedSession = true
		unsupportedErr := NewError(ErrorRepairExhausted, "validation repair", "same-session repair is unsupported", nil, WithDebugDetail(result.Metadata.Session.UnsupportedReason))
		err = unsupportedErr
		result.Err = unsupportedErr
	}
	if err != nil {
		sdkErr := firstSDKError(result.Err, err)
		r.sendEvent(Event{RunID: r.id, SessionID: result.SessionID, Type: "repair.failed", Payload: EventPayloadWithKind(EventValidation, EventPayload{"attempt": repairCtx.Attempt, "error_category": string(sdkErr.Category)})})
		return result, summary, sdkErr
	}
	r.sendEvent(Event{RunID: r.id, SessionID: result.SessionID, Type: "repair.completed", Payload: EventPayloadWithKind(EventValidation, EventPayload{"attempt": repairCtx.Attempt})})
	return result, summary, nil
}

func (r *validationRun) shouldRepair(ctx RepairContext) bool {
	if r.spec.Repair.MaxAttempts <= 0 || ctx.Attempt > r.spec.Repair.MaxAttempts {
		return false
	}
	if r.spec.Repair.ShouldRepair != nil {
		return r.spec.Repair.ShouldRepair(ctx)
	}
	return true
}

func (r *validationRun) setCurrent(run Run) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = run
}

func (r *validationRun) clearCurrent(run Run) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current == run {
		r.current = nil
	}
}

func (r *validationRun) forwardEvents(events <-chan Event) {
	for event := range events {
		r.sendEvent(event)
	}
}

func (r *validationRun) sendLifecycle(result RunResult, from, to RunStatus, reason string) {
	r.sendEvent(LifecycleEvent(r.id, result.SessionID, result.TurnID, result.Metadata.Context, r.seq+1, r.now(), from, to, reason))
}

func (r *validationRun) sendEvent(event Event) {
	r.seq++
	if event.ID == "" {
		event.ID = EventID(fmt.Sprintf("%s-event-%d", r.id, r.seq))
	}
	if event.RunID == "" || event.RunID != r.id {
		event.Payload = cloneEventPayload(event.Payload)
		event.Payload["inner_run_id"] = string(event.RunID)
		event.RunID = r.id
	}
	if event.Time.IsZero() {
		event.Time = r.now()
	}
	select {
	case <-r.ctx.Done():
	case r.events <- event:
	default:
	}
}

func (r *validationRun) finish(result RunResult, err *SDKError) {
	if result.RunID == "" {
		result.RunID = r.id
	}
	result.Metadata.ParentRunID = r.id
	result.Metadata.Status = result.Status
	if err != nil {
		result.Err = err
		result.Status = statusForError(err)
		result.Metadata.Status = result.Status
		result.Metadata.Errors = appendSDKError(result.Metadata.Errors, err)
	}
	r.mu.Lock()
	r.result = result
	if err != nil {
		r.waitErr = err
	} else {
		r.waitErr = nil
	}
	r.mu.Unlock()
}

// ValidateRun evaluates the configured validators against a completed result.
func ValidateRun(ctx context.Context, req RunRequest, result RunResult, spec ValidationSpec) ValidationResult {
	vctx := ValidationContext{Request: req, Result: result, WorkDir: req.WorkDir}
	checks := []ValidationCheck{}
	for _, expectation := range spec.Expectations {
		checks = append(checks, validateExpectation(ctx, vctx, expectation))
	}
	for _, validator := range spec.Validators {
		if validator == nil {
			continue
		}
		checks = append(checks, normalizeCheck(validator.Validate(ctx, vctx)))
	}
	return aggregateValidation(checks)
}

func validateExpectation(ctx context.Context, vctx ValidationContext, exp ValidationExpectation) ValidationCheck {
	if err := ctx.Err(); err != nil {
		return failedCheck(exp, "context active", err.Error(), "validation cancelled")
	}
	switch exp.Kind {
	case ExpectationFile:
		return validatePath(exp, vctx.WorkDir, false)
	case ExpectationDirectory:
		return validatePath(exp, vctx.WorkDir, true)
	case ExpectationArtifact:
		return validateArtifact(exp, vctx.Result)
	case ExpectationMarkdownTemplate:
		return validateMarkdownTemplate(exp, vctx.WorkDir)
	case ExpectationJSON:
		return validateJSON(exp, vctx.WorkDir)
	case ExpectationMetadata:
		return validateMetadata(exp, vctx.Result)
	default:
		return failedCheck(exp, "supported expectation kind", string(exp.Kind), "unknown validation expectation kind")
	}
}

func validatePath(exp ValidationExpectation, workDir string, wantDir bool) ValidationCheck {
	path := resolvePath(workDir, exp.Path)
	info, err := os.Stat(path)
	if err != nil {
		return failedCheck(exp, path, "missing", "path was not found")
	}
	if wantDir && !info.IsDir() {
		return failedCheck(exp, "directory", "file", "path exists but is not a directory")
	}
	if !wantDir && info.IsDir() {
		return failedCheck(exp, "file", "directory", "path exists but is a directory")
	}
	return passedCheck(exp, path, "present")
}

func validateArtifact(exp ValidationExpectation, result RunResult) ValidationCheck {
	artifacts := append([]ArtifactRef{}, result.Artifacts...)
	artifacts = append(artifacts, result.Metadata.Artifacts...)
	for _, artifact := range artifacts {
		if exp.ArtifactID != "" && artifact.ID == exp.ArtifactID {
			return passedCheck(exp, string(exp.ArtifactID), "referenced")
		}
		if exp.ArtifactURI != "" && artifact.URI == exp.ArtifactURI {
			return passedCheck(exp, exp.ArtifactURI, "referenced")
		}
	}
	expected := "artifact reference"
	if exp.ArtifactID != "" {
		expected = string(exp.ArtifactID)
	} else if exp.ArtifactURI != "" {
		expected = exp.ArtifactURI
	}
	return failedCheck(exp, expected, "missing", "artifact reference was not present")
}

func validateMarkdownTemplate(exp ValidationExpectation, workDir string) ValidationCheck {
	templatePath := resolvePath(workDir, exp.TemplatePath)
	artifactPath := resolvePath(workDir, exp.Path)
	templateBytes, err := os.ReadFile(templatePath)
	if err != nil {
		return failedCheck(exp, templatePath, "unreadable", "template file could not be read")
	}
	artifactBytes, err := os.ReadFile(artifactPath)
	if err != nil {
		return failedCheck(exp, artifactPath, "unreadable", "markdown artifact could not be read")
	}
	expectedHeadings := markdownHeadings(string(templateBytes))
	observedHeadings := markdownHeadings(string(artifactBytes))
	if len(expectedHeadings) == 0 {
		return failedCheck(exp, "template headings", "none", "template has no Markdown headings")
	}
	if missing := missingInOrder(expectedHeadings, observedHeadings); len(missing) > 0 {
		return failedCheck(exp, strings.Join(expectedHeadings, " > "), "missing or out of order: "+strings.Join(missing, ", "), "required Markdown headings are missing or out of order")
	}
	if unresolved := unresolvedTemplateSlots(string(artifactBytes)); len(unresolved) > 0 {
		return failedCheck(exp, "resolved template placeholders", strings.Join(unresolved, ", "), "markdown artifact contains unresolved template placeholders")
	}
	return passedCheck(exp, templatePath, "markdown template matched")
}

func validateJSON(exp ValidationExpectation, workDir string) ValidationCheck {
	path := resolvePath(workDir, exp.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		return failedCheck(exp, path, "unreadable", "JSON file could not be read")
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return failedCheck(exp, "well-formed JSON", "invalid JSON", err.Error())
	}
	if len(exp.RequiredFields) > 0 {
		obj, ok := value.(map[string]any)
		if !ok {
			return failedCheck(exp, "JSON object", fmt.Sprintf("%T", value), "required fields need a JSON object")
		}
		missing := []string{}
		for _, field := range exp.RequiredFields {
			if _, ok := obj[field]; !ok {
				missing = append(missing, field)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return failedCheck(exp, "required fields: "+strings.Join(exp.RequiredFields, ", "), "missing: "+strings.Join(missing, ", "), "JSON object is missing required fields")
		}
	}
	return passedCheck(exp, path, "valid JSON")
}

func validateMetadata(exp ValidationExpectation, result RunResult) ValidationCheck {
	if exp.MetadataKey == "" {
		return failedCheck(exp, "metadata key", "empty", "metadata expectation requires a key")
	}
	if result.Metadata.NativeMetadata != nil {
		if _, ok := result.Metadata.NativeMetadata[exp.MetadataKey]; ok {
			return passedCheck(exp, exp.MetadataKey, "present")
		}
	}
	return failedCheck(exp, exp.MetadataKey, "missing", "metadata key was not present")
}

func aggregateValidation(checks []ValidationCheck) ValidationResult {
	result := ValidationResult{Passed: true, Checks: checks}
	for _, check := range checks {
		check = normalizeCheck(check)
		if check.Skipped {
			result.SkippedCount++
			continue
		}
		if check.Passed {
			result.PassedCount++
			continue
		}
		if severityRequired(check.Severity) {
			result.Passed = false
			result.FailedCount++
			result.Failures = append(result.Failures, ValidationFailure{
				ExpectationID: check.ExpectationID,
				Kind:          check.Kind,
				Severity:      check.Severity,
				Expected:      check.Expected,
				Observed:      check.Observed,
				Detail:        check.Detail,
				RepairHint:    check.RepairHint,
			})
		} else {
			result.SkippedCount++
		}
	}
	if len(checks) == 0 {
		result.Skipped = true
		result.SkippedCount = 1
	}
	if !result.Passed {
		result.Errors = []SDKError{*validationError(result)}
	}
	return result
}

func repairRequest(req RunRequest, config RepairConfig, repairCtx RepairContext) RunRequest {
	next := req
	next.Validation = nil
	if config.SessionAction != "" {
		next.SessionAction = config.SessionAction
	} else {
		next.SessionAction = SessionActionContinue
	}
	if next.SessionID == "" {
		next.SessionID = repairCtx.CurrentResult.SessionID
	}
	next.Prompt = defaultRepairPrompt(repairCtx)
	if config.BuildPrompt != nil {
		next.Prompt = config.BuildPrompt(repairCtx)
	}
	if config.OverrideRequest != nil {
		next = config.OverrideRequest(repairCtx, next)
	}
	return next
}

func defaultRepairPrompt(ctx RepairContext) string {
	var b strings.Builder
	b.WriteString("Repair the previous run output so it satisfies these validation failures. Preserve the existing task intent and update durable artifacts instead of relying on terminal output.\n")
	for _, failure := range ctx.Validation.Failures {
		b.WriteString("\n- ")
		b.WriteString(failure.ExpectationID)
		b.WriteString(": expected ")
		b.WriteString(failure.Expected)
		b.WriteString("; observed ")
		b.WriteString(failure.Observed)
		if failure.RepairHint != "" {
			b.WriteString("; hint ")
			b.WriteString(failure.RepairHint)
		}
	}
	return b.String()
}

func withValidationMetadata(result RunResult, spec ValidationSpec, history []ValidationResult, repairs []RepairAttemptSummary) RunResult {
	result.Metadata.Validation = ValidationMetadata{
		Configured: len(spec.Expectations) > 0 || len(spec.Validators) > 0,
		History:    append([]ValidationResult(nil), history...),
	}
	if len(history) > 0 {
		result.Metadata.Validation.Final = history[len(history)-1]
	}
	result.Metadata.Repair = RepairMetadata{
		Configured:  spec.Repair.MaxAttempts > 0,
		Attempted:   len(repairs) > 0,
		MaxAttempts: spec.Repair.MaxAttempts,
		Attempts:    append([]RepairAttemptSummary(nil), repairs...),
	}
	for _, repair := range repairs {
		if result.Metadata.Repair.PermissionPolicyID == "" {
			result.Metadata.Repair.PermissionPolicyID = repair.PermissionPolicyID
		}
		if repair.ErrorCategory == ErrorPermission {
			result.Metadata.Repair.PermissionDenied = true
		}
		if repair.UnsupportedSession {
			result.Metadata.Repair.UnsupportedSameSession = true
		}
	}
	if spec.Repair.MaxAttempts > 0 && len(repairs) >= spec.Repair.MaxAttempts && len(history) > 0 && !history[len(history)-1].Passed {
		result.Metadata.Repair.Exhausted = true
		result.Metadata.Repair.ExhaustedReason = "max repair attempts exhausted"
	}
	return result
}

func validationError(result ValidationResult) *SDKError {
	return NewError(ErrorValidation, "validation", "required output validation failed", nil, WithMetadata(map[string]string{
		"failed_count": fmt.Sprintf("%d", result.FailedCount),
	}))
}

func contextValidationError(err error) *SDKError {
	if err == nil {
		return nil
	}
	if err == context.Canceled {
		return NewError(ErrorCancellation, "validation", "validation run was cancelled", err)
	}
	if err == context.DeadlineExceeded {
		return NewError(ErrorTimeout, "validation", "validation run timed out", err)
	}
	return NewError(ErrorUnknown, "validation", err.Error(), err)
}

func firstSDKError(primary *SDKError, err error) *SDKError {
	if primary != nil {
		return primary
	}
	if err == nil {
		return nil
	}
	return ensureSDKError(err, "validation")
}

func statusForError(err *SDKError) RunStatus {
	if err != nil && err.Category == ErrorCancellation {
		return StatusCancelled
	}
	if err != nil {
		return StatusFailed
	}
	return StatusCompleted
}

func appendSDKError(errors []SDKError, err *SDKError) []SDKError {
	if err == nil {
		return errors
	}
	for _, existing := range errors {
		if existing.Category == err.Category && existing.Operation == err.Operation && existing.UserDetail == err.UserDetail {
			return errors
		}
	}
	return append(errors, *err)
}

func failedCheck(exp ValidationExpectation, expected, observed, detail string) ValidationCheck {
	return ValidationCheck{ExpectationID: expectationID(exp), Kind: exp.Kind, Severity: severity(exp.Severity), Passed: false, Expected: safeDetail(expected), Observed: safeDetail(observed), Detail: safeDetail(detail), RepairHint: safeDetail(exp.RepairHint)}
}

func passedCheck(exp ValidationExpectation, expected, observed string) ValidationCheck {
	return ValidationCheck{ExpectationID: expectationID(exp), Kind: exp.Kind, Severity: severity(exp.Severity), Passed: true, Expected: safeDetail(expected), Observed: safeDetail(observed), RepairHint: safeDetail(exp.RepairHint)}
}

func normalizeCheck(check ValidationCheck) ValidationCheck {
	if check.Severity == "" {
		check.Severity = ExpectationRequired
	}
	if check.Kind == "" {
		check.Kind = ExpectationCustom
	}
	if check.ExpectationID == "" {
		check.ExpectationID = string(check.Kind)
	}
	check.Expected = safeDetail(check.Expected)
	check.Observed = safeDetail(check.Observed)
	check.Detail = safeDetail(check.Detail)
	check.RepairHint = safeDetail(check.RepairHint)
	return check
}

func expectationID(exp ValidationExpectation) string {
	if exp.ID != "" {
		return exp.ID
	}
	if exp.Path != "" {
		return string(exp.Kind) + ":" + exp.Path
	}
	if exp.ArtifactID != "" {
		return string(exp.Kind) + ":" + string(exp.ArtifactID)
	}
	if exp.MetadataKey != "" {
		return string(exp.Kind) + ":" + exp.MetadataKey
	}
	return string(exp.Kind)
}

func severity(value ExpectationSeverity) ExpectationSeverity {
	if value == "" {
		return ExpectationRequired
	}
	return value
}

func severityRequired(value ExpectationSeverity) bool {
	return value == "" || value == ExpectationRequired
}

func resolvePath(workDir, path string) string {
	if path == "" || filepath.IsAbs(path) || workDir == "" {
		return path
	}
	return filepath.Join(workDir, path)
}

func markdownHeadings(content string) []string {
	headings := []string{}
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		i := 0
		for i < len(trimmed) && trimmed[i] == '#' {
			i++
		}
		if i == 0 || i >= len(trimmed) || trimmed[i] != ' ' {
			continue
		}
		headings = append(headings, strings.TrimSpace(trimmed[i+1:]))
	}
	return headings
}

func missingInOrder(expected, observed []string) []string {
	missing := []string{}
	pos := 0
	for _, heading := range expected {
		found := false
		for pos < len(observed) {
			if observed[pos] == heading {
				found = true
				pos++
				break
			}
			pos++
		}
		if !found {
			missing = append(missing, heading)
		}
	}
	return missing
}

func unresolvedTemplateSlots(content string) []string {
	tokens := []string{}
	for _, marker := range []string{"TODO", "TBD", "{{", "}}", "<!-- REQUIRED"} {
		if strings.Contains(content, marker) {
			tokens = append(tokens, marker)
		}
	}
	return tokens
}

func safeDetail(value string) string {
	value = strings.TrimSpace(value)
	const limit = 240
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

func safeFailurePayload(failures []ValidationFailure) []map[string]string {
	payload := make([]map[string]string, 0, len(failures))
	for _, failure := range failures {
		payload = append(payload, map[string]string{
			"expectation_id": failure.ExpectationID,
			"kind":           string(failure.Kind),
			"expected":       failure.Expected,
			"observed":       failure.Observed,
			"detail":         failure.Detail,
			"repair_hint":    failure.RepairHint,
		})
	}
	return payload
}

func cloneEventPayload(payload EventPayload) EventPayload {
	cloned := EventPayload{}
	for k, v := range payload {
		cloned[k] = v
	}
	return cloned
}

func permissionPolicyID(policy *PermissionPolicy) string {
	if policy == nil {
		return ""
	}
	return policy.Summary().ID
}
