# agentwrap

`agentwrap` is a Go SDK skeleton for supervising agentic coding runtimes from product workflows.

The current SDK surface establishes the core public runtime contract and the
first OpenCode adapter:

- public SDK package at the module root
- runtime-neutral run/session/turn/event/artifact/metadata/capability types
- classified SDK errors
- SDK health/preflight contracts and source-aware effective config summaries
- runtime-neutral resilience policy execution for bounded retry, backoff,
  fallback, rate-limit handling, and per-attempt metadata
- lifecycle/session events plus cleanup metadata that does not overwrite the
  primary run result
- OpenCode structured-output adapter with timeout/cancellation cleanup and
  best-effort `--session` continuation metadata
- OpenCode health probes for executable availability, structured-output help,
  workdir/config/path/provider/model checks where detectable, and required
  preflight blocking before process launch
- private structured-event fixtures and fake lifecycle tests under `internal/testkit`
- private fake runtime contract proof under `internal/testkit`

## Resilience Policies

`PolicyRunner` wraps existing `Runtime` implementations instead of moving retry
logic into adapters. A policy receives a `PolicyContext` with the classified
error, attempt number, runtime/provider/model context, prior attempts, session
metadata, and rate-limit information where available. It returns an explicit
`PolicyDecision`: stop, retry, wait, or fallback.

The built-in `BasicPolicy` is conservative:

- retries only classified retryable failures
- does not retry unknown or unrecoverable failures by default
- requires explicit bounds through `MaxAttemptsPerTarget`
- uses `RateLimitInfo.RetryAfter` or reset metadata before generic backoff
- falls back only to configured alternatives

Every attempt is retained in `RunMetadata.Attempts`, and policy decisions are
recorded in `RunMetadata.Policy.Decisions`. Policy execution emits canonical
`rate_limit`, `retry`, and `fallback` events with one logical correlation ID.

## Development

Requires Go 1.22 or newer.

```sh
go test ./...
```

Format code before committing:

```sh
gofmt -w .
```

## Scope Guardrails

- Real OpenCode invocation remains opt-in through the gated smoke test.
- Health checks do not start billable agent work; uncertain provider/model/auth
  readiness is reported as unknown or degraded instead of guessed as ready.
- Validation/repair and persistence are not implemented yet.
- Policy execution is bounded and explicit; there is no global circuit breaker
  or provider-wide throttling layer yet.
- No UltraPlan workflow logic in this SDK.
- CLI-oriented study material is treated as internal engineering evidence only, not product direction.
