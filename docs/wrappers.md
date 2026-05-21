# Wrappers

Wrappers implement `Runtime` and can wrap any other `Runtime`.

## PolicyRunner

`PolicyRunner` executes a runtime through a `ResiliencePolicy`. It keeps retry and fallback behavior outside adapters.

Policy input is `PolicyContext`, which includes:

- original and current request
- attempt number
- attempt number on the current target
- target index
- configured fallback alternatives
- prior attempts
- current result and classified error
- rate-limit information
- validation result when present
- elapsed time
- metadata

Policy output is `PolicyDecision`:

- `stop`
- `retry`
- `wait`
- `fallback`

Every attempt is recorded in `RunMetadata.Attempts`. Decisions are recorded in `RunMetadata.Policy.Decisions`. Policy execution emits canonical `rate_limit`, `retry`, and `fallback` events where applicable.

### BasicPolicy

`BasicPolicy` is conservative:

- It retries only retryable classified failures by default.
- It requires explicit `MaxAttemptsPerTarget`.
- It does not retry rate limits unless `RetryRateLimits` is true.
- It uses `RateLimitInfo.RetryAfter` or reset time before generic backoff.
- It falls back only to configured alternatives.

Default retryable categories include timeout, runtime exit, runtime/provider/model unavailable, malformed events, and qualifying rate limits.

Default fallbackable categories include rate limit, timeout, runtime exit, runtime/provider/model unavailable, malformed event, validation, and unknown. Authentication, permission, configuration, and cancellation do not fallback by default.

## ValidatingRuntime

`ValidatingRuntime` starts a wrapped runtime and validates successful results. A logical run succeeds only when the runtime succeeds and required validation passes.

Validation can be configured on the wrapper or per request through `RunRequest.Validation`.

Built-in expectation kinds:

- `file`
- `directory`
- `artifact`
- `markdown_template`
- `json`
- `metadata`
- `custom`

Expectation severity:

- `required`
- `optional`

Custom validators implement:

```go
type Validator interface {
    Validate(context.Context, ValidationContext) ValidationCheck
}
```

`ValidatorFunc` adapts a function to the interface.

### Repair

`RepairConfig` enables bounded validation repair:

- `MaxAttempts`
- `SessionAction`
- `AllowFreshSessionFallback`
- `ShouldRepair`
- `BuildPrompt`
- `OverrideRequest`

Repair prompts are built from safe expected/observed failure facts and artifact references. By default, repair attempts continue the original session posture and inherit workdir, provider/model, sandbox, permissions, and permission policy unless explicitly overridden.

Exhausted repair returns `ErrorRepairExhausted`. Permission denial during repair remains `ErrorPermission`.

## ObservingRuntime

`ObservingRuntime` adds active/completed run records, ordered event records, optional sink fan-out, and optional store persistence.

It exposes `RunInspector` methods when a store is configured:

- `ListActiveRuns`
- `GetCompletedRun`
- `ListRunEvents`

### Event Sinks

`NamedEventSink` configures an `EventSink` with:

- `Name`
- `Sink`
- `Required`

Required sink failures are returned from `Wait` when the primary runtime outcome succeeded. Best-effort sink failures are recorded in the run record as warnings and do not replace the primary outcome.

### RunStore

`RunStore` is backend-neutral:

- `UpsertRun`
- `AppendEvent`
- `ListActiveRuns`
- `GetCompletedRun`
- `ListRunEvents`

`MemoryRunStore` is a deterministic in-memory reference implementation for tests and local inspection. It is not a production durability recommendation.

### Persistence Policy

`PersistencePolicy.PersistUnsafeRawPayloads` controls whether unsafe raw native payload bytes may be retained. The default is false.

