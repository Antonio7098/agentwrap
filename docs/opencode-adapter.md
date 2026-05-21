# OpenCode Adapter

The `opencode` package implements `agentwrap.Runtime` for the OpenCode CLI.

## Construction

```go
runtime := opencode.NewRuntime(
    opencode.WithExecutable("opencode"),
    opencode.WithExtraArgs("--some-opencode-flag"),
    opencode.WithEnv("KEY=value"),
    opencode.WithStderrLimit(16*1024),
)
```

Options:

- `WithExecutable(path)`: OpenCode executable path. Default is `opencode`.
- `WithExtraArgs(args...)`: appended after default structured-output flags and before the prompt.
- `WithEnv(env...)`: appended to subprocess environment.
- `WithStderrLimit(limit)`: bounds retained stderr diagnostics. Default is 16 KiB.

## Process Invocation

`StartRun` launches:

```text
opencode run --format json [--dir WORKDIR] [--model PROVIDER/MODEL] [--session SESSION] [extra args...] PROMPT
```

Provider/model behavior:

- If both provider and model are set and the model string does not already contain `/`, the adapter sends `provider/model`.
- If only model is set, the adapter sends that model string.

`WorkDir` is passed as both OpenCode `--dir` and the subprocess working directory.

`Timeout` creates a context deadline for the subprocess run.

## Structured Output

OpenCode stdout is decoded as newline-delimited JSON. Each nonblank line must be a JSON object with a string `type` field.

The adapter maps native types to event projections:

- `step_start` -> `progress`
- `step_finish` -> `final_result`
- `text`, `reasoning` -> `message`
- `tool_use` -> `tool`
- `error` -> `fatal_error`
- native types containing `permission` -> `permission`
- native types containing `question` or waiting status -> `blocking`
- native types containing `usage`, `token`, or `cost` -> `usage`
- native types containing `artifact` or `file` -> `artifact`
- native types containing `warn` -> `warning`
- native types containing `session` -> `session`
- otherwise -> `native_extension`

Raw native JSON records are attached to events as unsafe `RawPayload` values with source `opencode.stdout` and encoding `json`.

## Final Result Semantics

The adapter treats a native `step_finish` event as the final structured result marker.

Important cases:

- Malformed output before a final result fails the run with `ErrorMalformedEvent`.
- Malformed output after a final result is converted into a warning and does not fail the completed run.
- Nonzero process exit after a final result may still be considered completed when classified as a normal runtime exit.
- Process exit before a final structured result fails with `ErrorRuntimeExit` unless a more specific classification applies.
- Context deadline produces `ErrorTimeout`.
- Context cancellation produces `ErrorCancellation`.

## Sessions

Supported session request actions:

- default
- `fresh`
- `continue`

Unsupported actions are rejected before launch:

- `fork`
- `replace`
- `release`

Passing `SessionID` maps to OpenCode `--session` and is reported as best-effort continuation. The adapter does not claim full retained-session lifecycle support.

## Permissions

`PermissionPolicy` is translated into OpenCode config through `OPENCODE_CONFIG_CONTENT`.

Supported SDK tool mappings include:

- `read` -> `read`
- `edit` -> `edit`
- `shell` -> `bash`
- `glob` -> `glob`
- `search` -> `grep`
- `list` -> `list`
- `task` -> `task`
- `todo` -> `todowrite`
- `question` -> `question`
- `web_fetch` -> `webfetch`
- `web_search` -> `websearch`
- `repo_clone` -> `repo_clone`
- `repo_overview` -> `repo_overview`
- `external_directory` -> `external_directory`
- `doom_loop` -> `doom_loop`
- `language_server` -> `lsp`
- `skill` -> `skill`

Default policy actions are expanded across known native tools. Tool-specific actions override individual tools.

Path-level rules are not enforceable by the current subprocess adapter. They fail before launch unless `PermissionUnsupportedBestEffort` is configured.

If existing `OPENCODE_CONFIG_CONTENT` is provided through `WithEnv`, it must be valid JSON and its `permission` value, when present, must be an object.

## Health Checks

The adapter implements `HealthChecker`.

Default checks include:

- runtime availability through `opencode --version`
- structured output support through `opencode run --help`
- working directory validation
- config probe through `opencode debug config`
- paths probe through `opencode debug paths`
- provider probe through `opencode providers list`
- authentication readiness probe
- model probe through `opencode models PROVIDER --verbose`

Authentication readiness may be `unknown` when it cannot be proven without starting work.

`RunRequest.RequireHealth` blocks process launch unless the requested checks pass required-health evaluation.

## Capabilities

The adapter reports support for:

- structured events
- raw payloads
- cancellation
- artifacts when native events include them
- usage when native events include token data
- permissions through native config and event projection
- best-effort session continuation

It reports no support for full retained-session lifecycle, fork, replace, release, or native validation events.

