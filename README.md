# agentwrap

`agentwrap` is a Go SDK skeleton for supervising agentic coding runtimes from product workflows.

The current SDK surface establishes the core public runtime contract and the
first OpenCode adapter:

- public SDK package at the module root
- runtime-neutral run/session/turn/event/artifact/metadata/capability types
- classified SDK errors
- SDK health/preflight contracts and source-aware effective config summaries
- lifecycle/session events plus cleanup metadata that does not overwrite the
  primary run result
- OpenCode structured-output adapter with timeout/cancellation cleanup and
  best-effort `--session` continuation metadata
- OpenCode health probes for executable availability, structured-output help,
  workdir/config/path/provider/model checks where detectable, and required
  preflight blocking before process launch
- private structured-event fixtures and fake lifecycle tests under `internal/testkit`
- private fake runtime contract proof under `internal/testkit`

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
- No retry/fallback, validation/repair, or persistence implementation yet.
- No UltraPlan workflow logic in this SDK.
- CLI-oriented study material is treated as internal engineering evidence only, not product direction.
