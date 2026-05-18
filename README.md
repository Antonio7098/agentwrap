# agentwrap

`agentwrap` is a Go SDK skeleton for supervising agentic coding runtimes from product workflows.

Sprint 2 establishes the core public runtime contract:

- public SDK package at the module root
- runtime-neutral run/session/turn/event/artifact/metadata/capability types
- classified SDK errors
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

- No real OpenCode invocation in Sprint 2.
- No retry/fallback, validation/repair, persistence, or health/config implementation yet.
- No UltraPlan workflow logic in this SDK.
- CLI-oriented study material is treated as internal engineering evidence only, not product direction.
