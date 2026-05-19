package agentwrap

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var policyRunCounter atomic.Int64

// ResiliencePolicy decides what should happen after a runtime attempt ends.
type ResiliencePolicy interface {
	Decide(context.Context, PolicyContext) (PolicyDecision, error)
}

// PolicyContext is the immutable input passed to a resilience policy.
type PolicyContext struct {
	OriginalRequest RunRequest
	CurrentRequest  RunRequest
	Attempt         int
	AttemptOnTarget int
	TargetIndex     int
	Target          FallbackAlternative
	Alternatives    []FallbackAlternative
	PriorAttempts   []AttemptSummary
	CurrentResult   RunResult
	Err             *SDKError
	RateLimit       *RateLimitInfo
	Validation      *ValidationResult
	StartedAt       time.Time
	Elapsed         time.Duration
	Metadata        map[string]string
}

// ValidationResult is intentionally minimal until output validation is
// implemented. It gives policies a stable field without defining validators.
type ValidationResult struct {
	Passed bool
	Errors []SDKError
	Native map[string]any
}

// PolicyDecisionKind identifies the next action selected by a policy.
type PolicyDecisionKind string

const (
	PolicyDecisionStop     PolicyDecisionKind = "stop"
	PolicyDecisionRetry    PolicyDecisionKind = "retry"
	PolicyDecisionWait     PolicyDecisionKind = "wait"
	PolicyDecisionFallback PolicyDecisionKind = "fallback"
)

// PolicyDecision is the explicit action returned by a resilience policy.
type PolicyDecision struct {
	Kind          PolicyDecisionKind
	Reason        string
	Detail        string
	Delay         time.Duration
	TargetIndex   int
	Target        *FallbackAlternative
	Request       *RunRequest
	SessionAction SessionAction
	RateLimit     *RateLimitInfo
	Metadata      map[string]any
	Err           *SDKError
}

// RateLimitInfo describes a runtime or provider rate-limit signal.
type RateLimitInfo struct {
	Provider       ProviderID
	Model          ModelID
	Scope          string
	LimitName      string
	RetryAfter     time.Duration
	ResetAt        time.Time
	Remaining      *int64
	Limit          *int64
	Source         string
	UserDetail     string
	NativeMetadata map[string]any
}

// FallbackAlternative is a runtime and request override that policy execution
// may switch to after a failed attempt.
type FallbackAlternative struct {
	Name     string
	Runtime  Runtime
	Request  RunRequest
	Context  RuntimeContext
	Metadata map[string]any
}

// BackoffPolicy computes a delay for the next policy attempt.
type BackoffPolicy interface {
	Delay(PolicyContext) time.Duration
}

// FixedBackoff returns one fixed delay for every retry.
type FixedBackoff struct {
	DelayValue time.Duration
}

// Delay returns the configured fixed delay.
func (b FixedBackoff) Delay(PolicyContext) time.Duration {
	return b.DelayValue
}

// ExponentialBackoff computes Initial * Factor^(attempt-on-target-1), capped.
type ExponentialBackoff struct {
	Initial time.Duration
	Factor  float64
	Max     time.Duration
}

// Delay returns a bounded exponential delay.
func (b ExponentialBackoff) Delay(ctx PolicyContext) time.Duration {
	if b.Initial <= 0 {
		return 0
	}
	factor := b.Factor
	if factor < 1 {
		factor = 1
	}
	delay := float64(b.Initial)
	for i := 1; i < ctx.AttemptOnTarget; i++ {
		delay *= factor
	}
	result := time.Duration(delay)
	if b.Max > 0 && result > b.Max {
		return b.Max
	}
	return result
}

// BasicPolicy is a conservative bounded policy helper. It classifies failure
// facts at decision time and falls back only to configured alternatives.
type BasicPolicy struct {
	MaxAttemptsPerTarget int
	MaxElapsed           time.Duration
	Backoff              BackoffPolicy
	RetryRateLimits      bool
	Fallbacks            []FallbackAlternative
	ShouldRetry          func(PolicyContext) bool
	ShouldFallback       func(PolicyContext) bool
}

// Decide implements ResiliencePolicy.
func (p BasicPolicy) Decide(ctx context.Context, policyCtx PolicyContext) (PolicyDecision, error) {
	if err := ctx.Err(); err != nil {
		return PolicyDecision{Kind: PolicyDecisionStop, Err: contextPolicyError(err)}, nil
	}
	if policyCtx.Err == nil {
		return PolicyDecision{Kind: PolicyDecisionStop, Reason: "attempt completed"}, nil
	}
	if p.MaxElapsed > 0 && policyCtx.Elapsed >= p.MaxElapsed {
		return PolicyDecision{Kind: PolicyDecisionStop, Reason: "max elapsed time exhausted", Err: policyCtx.Err}, nil
	}
	if policyCtx.Err.Category == ErrorRateLimit && !p.RetryRateLimits {
		return p.fallbackOrStop(policyCtx, "rate limit retry disabled")
	}
	if policyCtx.Err.Category == ErrorRuntimeExit && p.hasFallback(policyCtx) {
		return p.fallbackOrStop(policyCtx, "runtime exit")
	}
	if p.shouldRetry(policyCtx) && p.canRetry(policyCtx) {
		delay := p.delay(policyCtx)
		reason := "retryable failure"
		if policyCtx.Err.Category == ErrorRateLimit {
			reason = "rate limit"
		}
		return PolicyDecision{
			Kind:          PolicyDecisionRetry,
			Reason:        reason,
			Detail:        policyCtx.Err.UserDetail,
			Delay:         delay,
			TargetIndex:   policyCtx.TargetIndex,
			SessionAction: SessionActionDefault,
			RateLimit:     policyCtx.RateLimit,
		}, nil
	}
	return p.fallbackOrStop(policyCtx, "retry exhausted")
}

func (p BasicPolicy) canRetry(ctx PolicyContext) bool {
	max := p.MaxAttemptsPerTarget
	if max <= 0 {
		return false
	}
	return ctx.AttemptOnTarget < max
}

func (p BasicPolicy) delay(ctx PolicyContext) time.Duration {
	if ctx.RateLimit != nil {
		if ctx.RateLimit.RetryAfter > 0 {
			return ctx.RateLimit.RetryAfter
		}
		if !ctx.RateLimit.ResetAt.IsZero() {
			if until := time.Until(ctx.RateLimit.ResetAt); until > 0 {
				return until
			}
		}
	}
	if p.Backoff == nil {
		return 0
	}
	return p.Backoff.Delay(ctx)
}

func (p BasicPolicy) fallbackOrStop(ctx PolicyContext, reason string) (PolicyDecision, error) {
	if ctx.Err != nil && p.shouldFallback(ctx) {
		fallbacks := p.Fallbacks
		if len(fallbacks) == 0 {
			fallbacks = ctx.Alternatives
		}
		next := ctx.TargetIndex + 1
		if next < len(fallbacks)+1 {
			target := fallbacks[next-1]
			return PolicyDecision{
				Kind:          PolicyDecisionFallback,
				Reason:        reason,
				Detail:        ctx.Err.UserDetail,
				TargetIndex:   next,
				Target:        &target,
				SessionAction: target.Request.SessionAction,
				RateLimit:     ctx.RateLimit,
			}, nil
		}
	}
	return PolicyDecision{Kind: PolicyDecisionStop, Reason: reason, Detail: safeErrDetail(ctx.Err), Err: ctx.Err}, nil
}

func (p BasicPolicy) hasFallback(ctx PolicyContext) bool {
	fallbacks := p.Fallbacks
	if len(fallbacks) == 0 {
		fallbacks = ctx.Alternatives
	}
	return ctx.TargetIndex+1 < len(fallbacks)+1
}

func (p BasicPolicy) shouldRetry(ctx PolicyContext) bool {
	if p.ShouldRetry != nil {
		return p.ShouldRetry(ctx)
	}
	return defaultRetryable(ctx)
}

func (p BasicPolicy) shouldFallback(ctx PolicyContext) bool {
	if p.ShouldFallback != nil {
		return p.ShouldFallback(ctx)
	}
	return defaultFallbackable(ctx)
}

func defaultRetryable(ctx PolicyContext) bool {
	err := ctx.Err
	if err == nil {
		return false
	}
	switch err.Category {
	case ErrorTimeout, ErrorRuntimeExit, ErrorRuntimeUnavailable, ErrorProviderUnavailable, ErrorModelUnavailable, ErrorMalformedEvent:
		return true
	case ErrorRateLimit:
		return ctx.RateLimit != nil || err.RetryAfter > 0 || err.StatusCode == 429 || err.StatusCode == 503
	default:
		return false
	}
}

func defaultFallbackable(ctx PolicyContext) bool {
	err := ctx.Err
	if err == nil {
		return false
	}
	switch err.Category {
	case ErrorAuthentication, ErrorPermission, ErrorConfiguration, ErrorCancellation:
		return false
	case ErrorRateLimit, ErrorTimeout, ErrorRuntimeExit, ErrorRuntimeUnavailable, ErrorProviderUnavailable, ErrorModelUnavailable, ErrorMalformedEvent, ErrorValidation, ErrorUnknown:
		return true
	default:
		return false
	}
}

// PolicyRunner executes a runtime run through a resilience policy.
type PolicyRunner struct {
	Runtime      Runtime
	Alternatives []FallbackAlternative
	Policy       ResiliencePolicy
	Now          func() time.Time
	Sleep        func(context.Context, time.Duration) error
}

// StartRun starts policy execution and returns a logical run handle.
func (r PolicyRunner) StartRun(ctx context.Context, req RunRequest) (Run, error) {
	if r.Runtime == nil {
		return nil, NewError(ErrorConfiguration, "policy runner", "primary runtime is required", nil)
	}
	policy := r.Policy
	if policy == nil {
		policy = BasicPolicy{}
	}
	now := r.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	sleep := r.Sleep
	if sleep == nil {
		sleep = defaultPolicySleep
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := &policyRun{
		id:           RunID(fmt.Sprintf("policy-%d", policyRunCounter.Add(1))),
		original:     req,
		ctx:          runCtx,
		cancel:       cancel,
		primary:      r.Runtime,
		alternatives: append([]FallbackAlternative(nil), r.Alternatives...),
		policy:       policy,
		now:          now,
		sleep:        sleep,
		events:       make(chan Event, 64),
		done:         make(chan struct{}),
		started:      now(),
		targetCounts: map[int]int{},
	}
	go run.execute()
	return run, nil
}

// Capabilities reports the primary runtime capabilities.
func (r PolicyRunner) Capabilities(ctx context.Context) (Capabilities, error) {
	if r.Runtime == nil {
		return Capabilities{}, NewError(ErrorConfiguration, "policy runner capabilities", "primary runtime is required", nil)
	}
	return r.Runtime.Capabilities(ctx)
}

type policyRun struct {
	id       RunID
	original RunRequest
	ctx      context.Context
	cancel   context.CancelFunc

	primary      Runtime
	alternatives []FallbackAlternative
	policy       ResiliencePolicy
	now          func() time.Time
	sleep        func(context.Context, time.Duration) error

	events chan Event
	done   chan struct{}

	mu            sync.Mutex
	result        RunResult
	waitErr       error
	current       Run
	started       time.Time
	finished      time.Time
	seq           int64
	attempts      []AttemptSummary
	decisions     []PolicyDecisionRecord
	eventMu       sync.Mutex
	droppedEvents []PolicyDroppedEvent
	targetCounts  map[int]int
}

func (r *policyRun) ID() RunID { return r.id }

func (r *policyRun) Events() <-chan Event { return r.events }

func (r *policyRun) Wait(ctx context.Context) (RunResult, error) {
	select {
	case <-ctx.Done():
		return RunResult{}, contextPolicyError(ctx.Err())
	case <-r.done:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.result, r.waitErr
}

func (r *policyRun) Cancel(ctx context.Context) error {
	r.cancel()
	r.mu.Lock()
	current := r.current
	r.mu.Unlock()
	if current != nil {
		return current.Cancel(ctx)
	}
	return nil
}

func (r *policyRun) execute() {
	defer close(r.done)
	defer close(r.events)
	defer r.cancel()

	targetIndex := 0
	req := r.original
	var last RunResult
	var lastErr *SDKError
	for {
		select {
		case <-r.ctx.Done():
			r.finishCancelled(last)
			return
		default:
		}
		r.targetCounts[targetIndex]++
		attempt := len(r.attempts) + 1
		attemptOnTarget := r.targetCounts[targetIndex]
		runtime := r.runtimeFor(targetIndex)
		started := r.now()
		run, startErr := runtime.StartRun(r.ctx, req)
		if startErr != nil {
			last = RunResult{RunID: "", Status: StatusFailed, StartedAt: started, FinishedAt: r.now(), Err: ensureSDKError(startErr, "policy attempt start")}
		} else {
			r.setCurrent(run)
			eventsDone := make(chan struct{})
			go func() {
				defer close(eventsDone)
				r.forwardEvents(run.Events(), attempt, targetIndex)
			}()
			result, waitErr := run.Wait(r.ctx)
			<-eventsDone
			r.clearCurrent(run)
			last = result
			if waitErr != nil && last.Err == nil {
				last.Err = ensureSDKError(waitErr, "policy attempt wait")
			}
		}
		lastErr = last.Err
		summary := r.summarizeAttempt(attempt, attemptOnTarget, targetIndex, req, started, last)
		r.attempts = append(r.attempts, summary)
		if lastErr == nil {
			r.finish(last, nil, false, "")
			return
		}
		policyCtx := PolicyContext{
			OriginalRequest: r.original,
			CurrentRequest:  req,
			Attempt:         attempt,
			AttemptOnTarget: attemptOnTarget,
			TargetIndex:     targetIndex,
			Target:          r.targetFor(targetIndex),
			Alternatives:    append([]FallbackAlternative(nil), r.alternatives...),
			PriorAttempts:   append([]AttemptSummary(nil), r.attempts...),
			CurrentResult:   last,
			Err:             lastErr,
			RateLimit:       rateLimitFromResult(last),
			StartedAt:       r.started,
			Elapsed:         r.now().Sub(r.started),
			Metadata:        cloneStringMap(r.original.Metadata),
		}
		decision, err := r.policy.Decide(r.ctx, policyCtx)
		if err != nil {
			lastErr = ensureSDKError(err, "policy decide")
			last.Err = lastErr
			r.finish(last, lastErr, true, "policy decision failed")
			return
		}
		decision = normalizeDecision(decision, policyCtx)
		r.recordDecision(attempt, targetIndex, decision, policyCtx)
		if decision.Kind == PolicyDecisionStop {
			if decision.Err != nil {
				last.Err = decision.Err
			}
			r.finish(last, last.Err, true, decision.Reason)
			return
		}
		if decision.Delay > 0 {
			if err := r.sleep(r.ctx, decision.Delay); err != nil {
				last.Err = contextPolicyError(err)
				r.finish(last, last.Err, true, "policy wait cancelled")
				return
			}
		}
		switch decision.Kind {
		case PolicyDecisionRetry, PolicyDecisionWait:
			req = requestForDecision(req, decision)
		case PolicyDecisionFallback:
			r.applyDecisionTarget(decision)
			targetIndex = decision.TargetIndex
			req = fallbackRequest(r.original, decision, r.targetFor(targetIndex))
		default:
			last.Err = NewError(ErrorConfiguration, "policy decision", "unsupported policy decision", nil, WithDebugDetail(string(decision.Kind)))
			r.finish(last, last.Err, true, "unsupported policy decision")
			return
		}
	}
}

func (r *policyRun) applyDecisionTarget(decision PolicyDecision) {
	if decision.Target == nil || decision.TargetIndex <= 0 {
		return
	}
	idx := decision.TargetIndex - 1
	for len(r.alternatives) <= idx {
		r.alternatives = append(r.alternatives, FallbackAlternative{})
	}
	r.alternatives[idx] = *decision.Target
}

func (r *policyRun) runtimeFor(index int) Runtime {
	if index == 0 {
		return r.primary
	}
	if idx := index - 1; idx >= 0 && idx < len(r.alternatives) && r.alternatives[idx].Runtime != nil {
		return r.alternatives[idx].Runtime
	}
	return r.primary
}

func (r *policyRun) targetFor(index int) FallbackAlternative {
	if index == 0 {
		return FallbackAlternative{Runtime: r.primary, Request: r.original}
	}
	if idx := index - 1; idx >= 0 && idx < len(r.alternatives) {
		return r.alternatives[idx]
	}
	return FallbackAlternative{Runtime: r.primary, Request: r.original}
}

func (r *policyRun) setCurrent(run Run) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = run
}

func (r *policyRun) clearCurrent(run Run) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current == run {
		r.current = nil
	}
}

func (r *policyRun) forwardEvents(events <-chan Event, attempt, targetIndex int) {
	for event := range events {
		if event.Payload == nil {
			event.Payload = EventPayload{}
		}
		event.Payload["policy_run_id"] = r.id
		event.Payload["attempt"] = attempt
		event.Payload["target_index"] = targetIndex
		r.sendEvent(event)
	}
}

func (r *policyRun) sendEvent(event Event) {
	r.eventMu.Lock()
	r.seq++
	if event.ID == "" {
		event.ID = EventID(fmt.Sprintf("%s-event-%d", r.id, r.seq))
	}
	if event.RunID == "" {
		event.RunID = r.id
	}
	if event.Time.IsZero() {
		event.Time = r.now()
	}
	r.eventMu.Unlock()
	select {
	case <-r.ctx.Done():
	case r.events <- event:
	default:
		r.recordDroppedEvent(event)
	}
}

func (r *policyRun) recordDroppedEvent(event Event) {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	r.droppedEvents = append(r.droppedEvents, PolicyDroppedEvent{
		Kind:  event.Kind(),
		Type:  event.Type,
		RunID: event.RunID,
		At:    r.now(),
	})
}

func (r *policyRun) summarizeAttempt(attempt, attemptOnTarget, targetIndex int, req RunRequest, started time.Time, result RunResult) AttemptSummary {
	finished := result.FinishedAt
	if finished.IsZero() {
		finished = r.now()
	}
	status := result.Status
	if status == "" {
		if result.Err != nil {
			status = StatusFailed
		} else {
			status = StatusCompleted
		}
	}
	summary := AttemptSummary{
		Attempt:         attempt,
		AttemptOnTarget: attemptOnTarget,
		TargetIndex:     targetIndex,
		RunID:           result.RunID,
		ParentRunID:     r.id,
		Context:         result.Metadata.Context,
		Request:         attemptRequest(req),
		Status:          status,
		StartedAt:       firstTime(result.StartedAt, started),
		FinishedAt:      finished,
		Duration:        finished.Sub(firstTime(result.StartedAt, started)),
		Session:         result.Metadata.Session,
		Error:           result.Err,
		RateLimit:       rateLimitFromResult(result),
		NativeMetadata:  cloneAnyMap(result.Metadata.NativeMetadata),
	}
	if summary.Context.RuntimeKind == "" {
		summary.Context = RuntimeContext{Provider: req.Provider, Model: req.Model}
	}
	if result.Err != nil {
		summary.ErrorCategory = result.Err.Category
	}
	return summary
}

func (r *policyRun) recordDecision(attempt, targetIndex int, decision PolicyDecision, ctx PolicyContext) {
	record := PolicyDecisionRecord{
		Attempt:     attempt,
		TargetIndex: targetIndex,
		Kind:        decision.Kind,
		Reason:      decision.Reason,
		Detail:      decision.Detail,
		Delay:       decision.Delay,
		Context:     ctx.CurrentResult.Metadata.Context,
		RateLimit:   decision.RateLimit,
		Metadata:    cloneAnyMap(decision.Metadata),
	}
	r.decisions = append(r.decisions, record)
	if decision.Kind == PolicyDecisionStop {
		return
	}
	category := EventRetry
	eventType := "retry"
	if decision.Kind == PolicyDecisionFallback {
		category = EventFallback
		eventType = "fallback"
	}
	if decision.RateLimit != nil {
		r.sendEvent(Event{
			RunID: r.id,
			Type:  "rate_limit",
			Payload: EventPayloadWithKind(EventRateLimit, EventPayload{
				"attempt":      attempt,
				"target_index": targetIndex,
				"provider":     decision.RateLimit.Provider,
				"model":        decision.RateLimit.Model,
				"retry_after":  decision.RateLimit.RetryAfter.String(),
				"reset_at":     decision.RateLimit.ResetAt,
				"detail":       decision.RateLimit.UserDetail,
			}),
		})
	}
	r.sendEvent(Event{
		RunID: r.id,
		Type:  eventType,
		Payload: EventPayloadWithKind(category, EventPayload{
			"attempt":        attempt,
			"target_index":   targetIndex,
			"decision":       decision.Kind,
			"reason":         decision.Reason,
			"detail":         decision.Detail,
			"delay":          decision.Delay.String(),
			"session_action": decision.SessionAction,
		}),
	})
}

func (r *policyRun) finish(result RunResult, err *SDKError, exhausted bool, reason string) {
	r.finished = r.now()
	if result.RunID == "" {
		result.RunID = r.id
	}
	if result.StartedAt.IsZero() {
		result.StartedAt = r.started
	}
	if result.FinishedAt.IsZero() {
		result.FinishedAt = r.finished
	}
	if result.Status == "" {
		if err != nil {
			result.Status = StatusFailed
		} else {
			result.Status = StatusCompleted
		}
	}
	if err != nil {
		result.Err = err
	}
	result.Metadata.ParentRunID = r.id
	result.Metadata.Attempt = len(r.attempts)
	result.Metadata.Attempts = append([]AttemptSummary(nil), r.attempts...)
	result.Metadata.Policy = PolicyMetadata{
		LogicalRunID:     r.id,
		FinalAttempt:     len(r.attempts),
		FinalTargetIndex: finalTargetIndex(r.attempts),
		Exhausted:        exhausted,
		ExhaustedReason:  reason,
		Decisions:        append([]PolicyDecisionRecord(nil), r.decisions...),
		DroppedEvents:    r.snapshotDroppedEvents(),
	}
	if result.Metadata.StartedAt.IsZero() {
		result.Metadata.StartedAt = result.StartedAt
	}
	if result.Metadata.FinishedAt.IsZero() {
		result.Metadata.FinishedAt = result.FinishedAt
	}
	if result.Metadata.Duration == 0 && !result.Metadata.StartedAt.IsZero() && !result.Metadata.FinishedAt.IsZero() {
		result.Metadata.Duration = result.Metadata.FinishedAt.Sub(result.Metadata.StartedAt)
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

func (r *policyRun) snapshotDroppedEvents() []PolicyDroppedEvent {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	return append([]PolicyDroppedEvent(nil), r.droppedEvents...)
}

func (r *policyRun) finishCancelled(last RunResult) {
	err := contextPolicyError(r.ctx.Err())
	last.Status = StatusCancelled
	last.Err = err
	r.finish(last, err, true, "policy run cancelled")
}

func normalizeDecision(decision PolicyDecision, ctx PolicyContext) PolicyDecision {
	if decision.Kind == "" {
		decision.Kind = PolicyDecisionStop
	}
	if decision.RateLimit == nil {
		decision.RateLimit = ctx.RateLimit
	}
	if decision.SessionAction == "" {
		decision.SessionAction = SessionActionDefault
	}
	return decision
}

func requestForDecision(current RunRequest, decision PolicyDecision) RunRequest {
	next := current
	if decision.Request != nil {
		next = *decision.Request
	}
	if decision.SessionAction != "" {
		next.SessionAction = decision.SessionAction
	}
	return next
}

func fallbackRequest(original RunRequest, decision PolicyDecision, target FallbackAlternative) RunRequest {
	next := original
	if target.Request.Prompt != "" {
		next.Prompt = target.Request.Prompt
	}
	overlayRunRequest(&next, target.Request)
	if decision.Request != nil {
		overlayRunRequest(&next, *decision.Request)
	}
	if decision.SessionAction != "" {
		next.SessionAction = decision.SessionAction
	}
	return next
}

func overlayRunRequest(dst *RunRequest, src RunRequest) {
	if src.WorkDir != "" {
		dst.WorkDir = src.WorkDir
	}
	if src.SessionID != "" {
		dst.SessionID = src.SessionID
	}
	if src.TurnID != "" {
		dst.TurnID = src.TurnID
	}
	if src.Provider != "" {
		dst.Provider = src.Provider
	}
	if src.Model != "" {
		dst.Model = src.Model
	}
	if src.Permissions != "" {
		dst.Permissions = src.Permissions
	}
	if src.Sandbox != "" {
		dst.Sandbox = src.Sandbox
	}
	if src.Timeout != 0 {
		dst.Timeout = src.Timeout
	}
	if src.Metadata != nil {
		dst.Metadata = cloneStringMap(src.Metadata)
	}
	if src.WantSession {
		dst.WantSession = true
	}
	if src.SessionAction != "" {
		dst.SessionAction = src.SessionAction
	}
	if src.RequireCaps != nil {
		dst.RequireCaps = append([]Capability(nil), src.RequireCaps...)
	}
	if src.RequireHealth != nil {
		dst.RequireHealth = append([]HealthCheckID(nil), src.RequireHealth...)
	}
}

func attemptRequest(req RunRequest) AttemptRequest {
	return AttemptRequest{
		WorkDir:       req.WorkDir,
		SessionID:     req.SessionID,
		Provider:      req.Provider,
		Model:         req.Model,
		Permissions:   req.Permissions,
		Sandbox:       req.Sandbox,
		Timeout:       req.Timeout,
		WantSession:   req.WantSession,
		SessionAction: req.SessionAction,
		RequireCaps:   append([]Capability(nil), req.RequireCaps...),
		RequireHealth: append([]HealthCheckID(nil), req.RequireHealth...),
		MetadataKeys:  sortedKeys(req.Metadata),
	}
}

func rateLimitFromResult(result RunResult) *RateLimitInfo {
	if info, ok := result.Metadata.NativeMetadata["rate_limit_info"].(*RateLimitInfo); ok && info != nil {
		return info
	}
	if info, ok := result.Metadata.NativeMetadata["rate_limit_info"].(RateLimitInfo); ok {
		copied := info
		return &copied
	}
	if result.Err != nil && result.Err.Category == ErrorRateLimit {
		info := &RateLimitInfo{
			Provider:   result.Metadata.Context.Provider,
			Model:      result.Metadata.Context.Model,
			Source:     "sdk_error",
			UserDetail: result.Err.UserDetail,
		}
		if info.Provider == "" {
			info.Provider = result.Metadata.Context.Provider
		}
		return info
	}
	for _, err := range result.Metadata.Errors {
		if err.Category == ErrorRateLimit {
			return &RateLimitInfo{
				Provider:   result.Metadata.Context.Provider,
				Model:      result.Metadata.Context.Model,
				Source:     "metadata_error",
				UserDetail: err.UserDetail,
			}
		}
	}
	return nil
}

func ensureSDKError(err error, operation string) *SDKError {
	if err == nil {
		return nil
	}
	var sdkErr *SDKError
	if ErrorAs(err, &sdkErr) {
		return sdkErr
	}
	return NewError(ErrorUnknown, operation, "runtime attempt failed", err, WithDebugDetail(err.Error()))
}

func contextPolicyError(err error) *SDKError {
	if err == nil {
		return nil
	}
	category := ErrorCancellation
	if err == context.DeadlineExceeded {
		category = ErrorTimeout
	}
	return NewError(category, "policy runner", "policy run stopped", err)
}

func defaultPolicySleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func firstTime(primary, fallback time.Time) time.Time {
	if !primary.IsZero() {
		return primary
	}
	return fallback
}

func finalTargetIndex(attempts []AttemptSummary) int {
	if len(attempts) == 0 {
		return 0
	}
	return attempts[len(attempts)-1].TargetIndex
}

func safeErrDetail(err *SDKError) string {
	if err == nil {
		return ""
	}
	if err.UserDetail != "" {
		return err.UserDetail
	}
	return err.DebugDetail
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func sortedKeys(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
