# Core API

The core SDK contract lives in package `agentwrap`.

## Runtime

```go
type Runtime interface {
    StartRun(context.Context, RunRequest) (Run, error)
    Capabilities(context.Context) (Capabilities, error)
}
```

`StartRun` validates and starts runtime work. A successful return means the caller owns a `Run` handle. It does not mean the agentic task completed.

`Capabilities` reports contract-level features such as structured events, sessions, cancellation, artifacts, usage, permissions, and validation events.

## Run

```go
type Run interface {
    ID() RunID
    Events() <-chan Event
    Wait(context.Context) (RunResult, error)
    Cancel(context.Context) error
}
```

Callers should drain `Events()` while waiting if they need streaming progress or if the runtime may emit enough events to fill buffers. `Wait` returns both a `RunResult` and an error. When an SDK failure is terminal, both `RunResult.Err` and the returned error usually point at the classified failure.

`Cancel` is best-effort. The final result is still delivered through `Wait`.

## RunRequest

`RunRequest` is intentionally minimal and runtime-neutral:

- `Prompt`: caller prompt sent to the runtime.
- `WorkDir`: working directory for runtime execution.
- `SessionID`, `TurnID`: caller identity for retained-session and turn tracking.
- `Provider`, `Model`: provider/model hints or requirements.
- `Permissions`: open permission mode string for runtime-level modes.
- `PermissionPolicy`: structured SDK permission policy.
- `Sandbox`: open sandbox mode string.
- `Timeout`: maximum runtime duration when supported by the adapter.
- `Metadata`: caller-owned string metadata.
- `WantSession`, `SessionAction`: retained-session intent.
- `RequireCaps`: required capabilities, for adapters/wrappers that enforce them.
- `RequireHealth`: required preflight checks before launch.
- `Validation`: per-run validation override.

The SDK records safe request facts in metadata but avoids storing large or sensitive prompt content in attempt summaries.

## RunResult

`RunResult` is the final caller-visible outcome:

- `RunID`, `SessionID`, `TurnID`: identity.
- `Status`: `starting`, `running`, `validating`, `repairing`, `completed`, `failed`, or `cancelled`.
- `Metadata`: dashboard and audit metadata.
- `Artifacts`: durable output references.
- `Warnings`: user-safe warnings.
- `Usage`: best-effort token usage.
- `StartedAt`, `FinishedAt`: runtime timing.
- `Err`: classified SDK failure when the run did not complete successfully.

Terminal statuses are `completed`, `failed`, and `cancelled`.

## Capabilities

The SDK defines open capability identifiers. Current constants include:

- `sessions`
- `session_continue`
- `session_fork`
- `session_replace`
- `session_release`
- `cancellation`
- `structured_events`
- `raw_payloads`
- `artifacts`
- `permissions`
- `usage`
- `validation_events`

Use `Capabilities.Supports(capability)` for explicit support checks.

## Sessions

Session behavior is expressed through `SessionAction`:

- `fresh`
- `continue`
- `fork`
- `replace`
- `release`

Adapters report the resolved relationship in `SessionMetadata`:

- `fresh`
- `same`
- `forked`
- `replaced`
- `released`
- `unsupported`
- `best_effort`

Best-effort means the adapter passed session intent to the runtime but cannot prove durable retained-session semantics.

## Artifacts

`ArtifactRef` references durable runtime output without embedding large content:

- `ID`
- `URI`
- `Kind`
- `Description`
- `Metadata`

Wrappers may add producer metadata such as source run, runtime kind, runtime name, provider, and model.

