# Muse Code adapter contract spike

Date: 2026-08-07

This spike characterizes Muse Code 0.1.0 (`0.1.0-R708.1`) against AgentWrap's existing `Runtime` and `Run` contracts. It does not add a production `muse.Runtime`; the code under `internal/spikes/musecontract` is a decoder/projector proof used to test the important semantic boundary before process supervision is implemented.

## Outcome

The existing AgentWrap public contract can represent Muse without changes.

Muse has a first-class headless command and a versioned JSONL stream:

```text
muse exec --json [OPTIONS] [PROMPT]
```

The adapter should consume stdout directly for live events. Muse's durable session log is a separate reconciliation and recovery surface, not the primary live stream.

## Observed installation

- Official installer: `https://dev.meta.ai/install.sh`
- Platform: Linux x86-64
- CLI: `Muse Code 0.1.0 (0.1.0-R708.1)`
- Binary SHA-256: `50937b6470cd0edf28eb683c352a5e7af3bcb1b015cd9a3b21dbf79d22af8182`
- Launcher SHA-256: `21c66e550a71cac2e4af081cc33d10bec81993d0043ec492761fc449e6c440f6`

The installer and CLI were kept under a repository-local `.sandbox/` directory, which is gitignored.

## Observed CLI contract

| Concern | Evidence | Adapter consequence |
|---|---|---|
| Headless execution | `muse exec` | Launch this subcommand, never the TUI. |
| Structured output | `muse exec --json` emits one schema-versioned JSON object per stdout line | Claim `structured_events` and `raw_payloads`. |
| Diagnostics | Workspace diagnostics are written to stderr while stdout remains valid JSONL | Decode stdout only; retain bounded stderr separately. |
| Native identity | The envelope carries session stream ID, sequence, causation ID, and linked run/task stream IDs | Preserve all native IDs; keep AgentWrap's adapter-owned `RunID`. |
| Completion | A strong `run.terminal.completed` record precedes clean exit | Require `run.terminal.*` as terminal evidence; do not infer success from exit 0 alone. |
| Child tasks | `task.lifecycle.failed` records were observed before `run.terminal.completed` and exit 0 | Never promote task failure to run failure. |
| Sessions | `--session-id <UUID>` appended another turn to the existing durable session | Candidate mapping for `SessionContinue`; verify Meta-provider behavior before claiming it. |
| Durable history | Session JSONL includes run/task events and child-session log paths | Use for reconciliation, resume evidence, and post-run enrichment. |
| Export | `muse export` emits `export_schema_version: 1` and diagnostics for gaps, duplicates, and unknown payloads | Prefer export for offline support bundles, not live streaming. |
| Trace inspection | `muse trace inspect --format json` projects one or all run streams offline | Useful diagnostic tool; not required by the adapter runtime. |
| Provider/model | Providers are `meta` and deterministic `echo`; `--model` and Meta reasoning effort are explicit | Use `echo` for contract tests. Accept only truthful provider/model combinations. |
| Authentication | Meta account login uses OAuth device flow; `META_API_KEY` and stdin API-key storage are also supported | Never accept a password. Classify the observed missing-credential message as `authentication`. |
| Permissions | Headless mode exposes approval, sandbox, workspace-write, shell, and network flags | Implement a conservative translation table; do not claim arbitrary AgentWrap policy support. |

## Native envelope

Observed stdout records use this stable outer shape:

```json
{
  "schema_version": 1,
  "id": "...",
  "stream": {"kind": "session", "id": "..."},
  "sequence": 1,
  "recorded_at": 1780531400000000,
  "record_type": "event",
  "durability": "durable",
  "causation_id": "...",
  "payload_type": "run.lifecycle.started",
  "payload_schema_version": 1,
  "payload": {}
}
```

`recorded_at` is microseconds since Unix epoch. Event payloads are intentionally open. Unknown `payload_type` values must be retained as `native_extension`, and raw records remain unsafe by default.

## Initial projection rules

| Muse payload | AgentWrap kind | Terminal? |
|---|---|---|
| `runtime.command.*`, `run.lifecycle.*` | `lifecycle` | No |
| `run.output.delta`, committed assistant output | `message` | No |
| `task.stream.*`, `task.lifecycle.*` | `progress` | No, including task failure |
| approval/permission records | `permission` | No |
| question/user-input records | `blocking` | No |
| usage/token/cost records | `usage` | No |
| tool records | `tool` | No |
| file/edit/artifact records | `artifact` | No |
| `session.*` | `session` | No |
| `run.terminal.completed` | `final_result` | Yes |
| other `run.terminal.*` | `fatal_error` initially | Yes |
| unknown payload | `native_extension` | No |

Cancellation may deserve `lifecycle` rather than `fatal_error`; settle that using a captured real cancellation record before production implementation.

## Process construction

The minimum process shape is:

```text
muse exec --json
  --workspace <workdir>
  [--provider meta]
  [--model <model>]
  [--session-id <uuid>]
  [permission/sandbox flags]
  <prompt>
```

Pass the prompt as the final argv element or via `--prompt-file`; do not use shell interpolation. Keep configuration/data directories injectable for test isolation. `--no-session-log` is appropriate for fixture tests but not for production runs that advertise recovery.

One beta CLI parsing limitation was observed: `--approval-mode` is documented as a root/TUI option but is rejected when placed after `muse exec`, while placing root options before `exec` caused the invocation to be parsed as TUI input. The initial adapter should use only flags accepted by `muse exec --help`, with fixture tests covering exact argv construction.

## Truthful initial capabilities

| Capability | Initial value | Basis |
|---|---:|---|
| `structured_events` | Supported | Observed JSONL stdout. |
| `raw_payloads` | Supported | Full native line can be retained. |
| `cancellation` | Unknown | Needs signal/process-tree characterization against a real Meta run. |
| `artifacts` | Unknown | Native vocabulary exists, but no real file-edit fixture captured. |
| `usage` | Unknown | Fields exist in runtime schemas, but no Meta usage record captured. |
| `permissions` | Unknown | CLI controls exist; exact non-interactive enforcement needs real tool-call tests. |
| `sessions` | Unsupported initially | Full create/continue/fork/replace/release lifecycle is not implemented. |
| `session_continue` | Unknown | Echo-provider append worked; Meta-provider semantics remain unverified. |

## Authentication and remaining live matrix

The sandbox could download and execute Muse, but outbound access to `auth.meta.com` was blocked before the OAuth device flow returned a user code. No supplied password was used or stored.

Before production implementation, capture these Meta-provider fixtures:

1. Simple successful response and usage.
2. Workspace read, shell call, and file edit.
3. Approval allow/deny and non-interactive blocking behavior.
4. SIGINT, SIGTERM, timeout, and forced process-tree termination.
5. Invalid/expired authentication.
6. Invalid model.
7. Rate limit and retry-after metadata.
8. Continued session and crash recovery.
9. Subagent lifecycle with and without worktree isolation.

## Recommended production slice

Add `muse/` as a sibling of `opencode/` with its own options, process runner, decoder, projector, runtime, health checks, fixtures, and gated integration tests. Do not extract a shared CLI adapter first. After Muse passes the same behavioral invariants as OpenCode, extract only proven duplicate process utilities and add an adapter conformance suite.

No public AgentWrap contract change is justified by the observed Muse behavior.
