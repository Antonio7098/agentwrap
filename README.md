# agentwrap

`agentwrap` is a Go SDK skeleton for supervising agentic coding runtimes from product workflows.

The current SDK surface establishes the core public runtime contract and the
first OpenCode adapter:

- public SDK package at the module root
- runtime-neutral run/session/turn/event/artifact/metadata/capability types
- classified SDK errors
- lifecycle/session events plus cleanup metadata that does not overwrite the
  primary run result
- OpenCode structured-output adapter with timeout/cancellation cleanup and
  best-effort `--session` continuation metadata
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
- No retry/fallback, validation/repair, persistence, or health/config implementation yet.
- No UltraPlan workflow logic in this SDK.
- CLI-oriented study material is treated as internal engineering evidence only, not product direction.
