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
- runtime-neutral output validation for files, directories, artifact
  references, Markdown template files, JSON shape checks, metadata fields, and
  caller-defined validators
- runtime-neutral observability records, ordered event records, optional event
  sinks, optional run stores, active/completed inspection APIs, and a
  deterministic in-memory reference store
- bounded validation repair attempts that inherit the original session,
  sandbox, runtime/model, and permission posture by default
- initialization-time permission policies with OpenCode native config
  translation and permission audit metadata
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

## Output Validation And Repair

`ValidatingRuntime` wraps any `Runtime` and runs configured validators after a
successful runtime result. A configured run is successful only when the runtime
finishes and required validators pass. Validation results are recorded in
`RunMetadata.Validation` and emitted as canonical validation events.

Built-in expectations cover durable output checks: file presence, directory
presence, artifact references, Markdown artifact compliance against a template
file, JSON well-formedness plus minimal required fields, and metadata fields.
Callers can add `ValidatorFunc` checks for product-specific rules without
placing those rules in the runtime adapter.

Repair is bounded with `RepairConfig.MaxAttempts`. Repair prompts are built
from safe expected/observed failure facts and artifact references, not raw large
content. Repair requests default to `SessionActionContinue` and inherit the
original workdir, provider/model, sandbox, permission mode, and
`PermissionPolicy` unless the caller overrides the repair request. Permission
denial during repair remains an `ErrorPermission`; exhausted repair returns
`ErrorRepairExhausted`.

## Permission Policies

Callers can attach a structured `PermissionPolicy` to `RunRequest` at run
initialization. The policy uses SDK tool classes such as `PermissionToolRead`,
`PermissionToolEdit`, and `PermissionToolShell` with actions `allow`, `deny`,
or `ask`.

The OpenCode adapter translates supported policy fields into per-process
`OPENCODE_CONFIG_CONTENT` permission config. Unsupported required features,
such as path-level rules in the current subprocess adapter, fail before process
start. Callers can opt into `PermissionUnsupportedBestEffort` when they want the
unsupported feature recorded but not treated as blocking.

Permission policy summaries, support classification, and audit records are
reported in `RunMetadata.Permissions`. The adapter also emits a canonical
permission event before process launch with a stable permission policy ID. Live
OpenCode REST/SSE approval posting is not part of the current subprocess
adapter; the native config path is the supported Sprint 7 behavior.

## Observability And Persistence Hooks

`ObservingRuntime` wraps any `Runtime` without changing adapter code. It drains
canonical events, forwards them to the caller, assigns per-run sequence numbers,
updates active run projections, and merges final `RunResult` metadata into a
completed `RunRecord`.

Callers can attach best-effort or required `EventSink` implementations and an
optional `RunStore`. Required sink failures are returned from `Wait` when the
primary runtime outcome succeeded; best-effort failures are recorded in the run
record without replacing the primary outcome. `MemoryRunStore` is a deterministic
reference implementation for tests and product-local inspection, not a durable
backend choice.

Run records preserve existing metadata types for attempts, retry/fallback
policy, sessions, permission audit, validation, repair, cleanup, artifacts,
usage, estimated cost, throughput, warnings, errors, and native metadata. Usage
token values keep nil/unknown semantics; unknown values are not converted to
zero. Artifact records receive producer metadata for source run, runtime,
provider, and model when available.

Unsafe native raw payload bytes are not persisted by default. Event records keep
safe canonical payload fields plus raw presence/safety/source/encoding and an
omission reason when unsafe bytes were dropped.

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
- Durable persistence backend selection is caller-owned; the SDK ships only the
  `RunStore` interface and in-memory reference store.
- Live permission approval transport is deferred until the SDK has an OpenCode
  server-mode adapter.
- Policy execution is bounded and explicit; there is no global circuit breaker
  or provider-wide throttling layer yet.
- No UltraPlan workflow logic in this SDK.
- CLI-oriented study material is treated as internal engineering evidence only, not product direction.
