# Changelog

All notable changes to `agentwrap` are documented in this file.

This changelog is intentionally detailed. The changes here were driven by real smoke testing against a live OpenCode binary, and the important part is not just the code that changed but the failure modes that motivated it.

## Unreleased

### OpenCode local runtime and DB failures are now classified explicitly

We added explicit handling for OpenCode local state failures in `opencode/runtime.go`.

What changed:

- `PRAGMA wal_checkpoint(PASSIVE)` / `wal_checkpoint` failures are recognized as local OpenCode DB problems.
- `database is locked` is recognized as a local OpenCode runtime-state failure.
- Generic SQLite database errors are grouped into the same local-failure path.
- These cases now return `runtime_unavailable` instead of a generic `runtime_exit`.
- Native metadata now includes `local_failure_info` so callers can inspect the category, user detail, and reason.

Why this matters:

OpenCode is an external black box. When it fails inside its own local database layer, the wrapper should not pretend that the failure is just a normal process exit. The adapter needs to surface the real shape of the failure so retry, fallback, and reporting logic can make a correct decision.

Evidence from smoke testing:

- A real `smoke-text` run previously surfaced a checkpoint failure shape from OpenCode.
- The wrapper now classifies that as a local runtime availability problem and preserves the reason in metadata.

### Timeout classification now consults recent OpenCode logs for more than rate limits

We extended timeout and context-deadline diagnosis so it can read recent OpenCode logs before falling back to a generic timeout classification.

What changed:

- Recent-log classification now looks for both provider rate-limit text and local OpenCode DB/runtime failure text.
- If a recent log matches a rate-limit shape, the wrapper reports `rate_limit`.
- If a recent log matches a local DB/runtime shape, the wrapper reports `runtime_unavailable`.
- Only if no relevant recent log evidence exists does the wrapper fall back to a generic timeout.

Why this matters:

Without this, a timeout can hide the real cause. The process might have already emitted the useful failure in logs, but the wrapper would still report only `timeout`. The new behavior keeps the terminal category closer to the actual root cause.

### Rate-limit detection was widened to cover MiniMax usage-limit text

We updated rate-limit classification to include additional text shapes.

What changed:

- `rate_limit_error` is now recognized.
- `usage limit exceeded` is now recognized.
- Existing rate-limit text matching remains in place.

Why this matters:

The real provider error text did not always use the same phrasing as existing tests. The wrapper needs to accept the provider's real-world wording, not just the phrasing we expected in advance.

Evidence from smoke testing:

- A real MiniMax smoke run completed successfully when provider usage was available again.
- The rate-limit text shape is still covered by unit tests so the known provider message is not lost even when the live provider is not currently rate-limiting.

### Clean exits with incomplete stdout are no longer always treated as failures

We made the final-result path more conservative about turning a clean process exit into a hard failure.

What changed:

- If OpenCode exits cleanly after emitting assistant output but without a final structured result, the run now completes with a warning instead of being forced into failure.
- If a final structured stdout event is missing, the adapter can reconcile against OpenCode DB state.
- The reconciliation path checks persisted session rows, message rows, and usage tokens to recover the final state when possible.

Why this matters:

OpenCode does not always emit a perfect terminal event stream. Sometimes the durable DB state contains the real completion signal even when stdout is incomplete. The wrapper should preserve successful work instead of manufacturing a failure from a missing event.

Evidence from smoke testing:

- Real smoke runs showed cases where OpenCode exited cleanly but did not emit the final structured result.
- The wrapper now records a warning and preserves the recovered state rather than dropping the run on the floor.

### Cancellation handling was tightened

We made cancellation classification more explicit.

What changed:

- A run that has already transitioned to the cancelled lifecycle is now reported as `cancellation` even if the underlying process reports a killed/non-zero exit.
- Cleanup after context cancellation now gets a longer grace window.

Why this matters:

Subprocess exit codes are not always the right semantic truth. If the wrapper intentionally cancelled the run, the user should see `cancellation`, not a misleading process-exit category.

### Validation and repair metadata now records phase information

We added more precise repair reporting in `validation.go` and `metadata.go`.

What changed:

- `RepairMetadata` now includes `Phase`.
- `RepairMetadata` now includes `FailurePhase`.
- Repair attempt summaries now include their own `Phase`.
- Initial run failures before repair now record `initial_run`.
- Repair attempt failures record `repair_attempt`.
- Repair exhaustion records `exhausted` and `after_repair_exhaustion`.

Why this matters:

Validation and repair are multi-step flows. A single failed result is not enough to understand where the failure happened. The new phase fields make it possible to distinguish:

- failure before repair started
- failure during a repair attempt
- failure after repair budget exhaustion

This is especially useful when the smoke harness is used to investigate whether a failure belongs to the initial run, the repair prompt, or the validation contract itself.

### Unit coverage was expanded around the new failure shapes

We added or extended tests in `opencode/runtime_test.go` and `validation_test.go`.

New coverage includes:

- OpenCode DB checkpoint failure classification
- timeout classification from recent OpenCode rate-limit logs
- timeout classification from recent OpenCode DB failure logs
- MiniMax `usage limit exceeded` / `rate_limit_error` classification
- clean exit with output but no final structured event
- no final output and no terminal event
- cancellation after process exit
- repair exhaustion metadata
- permission-denied repair metadata
- initial-run failure metadata

Why this matters:

The wrapper is only as reliable as the narrow failure cases it can reproduce deterministically. These tests lock down the exact shapes we observed in real smoke runs so future changes do not weaken the adapter in the same places again.

### Smoke harness behavior was tightened to support robust wrapper testing

The smoke harness in `agentwrap-smoke` was adjusted alongside the adapter work.

What changed:

- The `validate-repair` scenario was made deterministic.
- The repair scenario now explicitly starts from a failure state and repairs to a known file outcome.
- `smoke-all` now fails if an expected category is missing or mismatched.
- `results.json` now records whether `events.jsonl` is present.
- `saveOpenCodeDB` always writes a DB snapshot status file, even when the DB snapshot cannot be captured.

Why this matters:

The point of the smoke harness is not to prove the happy path once. It is to keep finding edge cases until the wrapper's failure reporting is explicit. That requires strict expected-failure assertions and honest evidence capture.

Evidence from smoke testing:

- `validate-repair` now passes with exactly one repair attempt and the expected file content.
- `validate-repair-exhaust` now fails with `repair_exhausted` and records the exhaustion phase.
- The full real `smoke-all` run passed all scenarios after the harness was tightened.

### Documentation now describes the robustness process explicitly

The smoke harness documentation was updated to describe how we should work on this system.

What changed outside this repository:

- `agentwrap-smoke/README.md` now explains that wrapping OpenCode means dealing with an external process boundary.
- The README now lays out the investigation loop: wrapper-visible evidence first, OpenCode DB/logs as the tie-breaker, smallest regression next, then real smoke rerun.
- `agentwrap-smoke/AGENTWRAP_REPORTING.md` now records the exact smoke evidence and the remaining ambiguities.
- `agentwrap-smoke/AGENTWRAP_REAL_OPENCODE_TEST_PLAN.md` now serves as the backlog for the next robustness work.

Why this matters:

The wrapper is a systems-integration problem, not a pure API shim. The docs need to reflect that so future changes continue to preserve evidence and avoid hiding failures.

### Real smoke test results that motivated these changes

Verified in `agentwrap-smoke` using a real OpenCode binary:

- `validate-repair` completed successfully after one repair attempt.
- `validate-repair-exhaust` failed with `repair_exhausted` after one repair attempt.
- `smoke-all` passed all 20 scenarios.
- `cancel` was reported as `cancellation`.
- `timeout` was reported as `timeout`.
- fallback scenarios completed or failed as expected.
- `session-fork` remained an explicit configuration failure.

These runs are the basis for the adapter changes above. The changelog is intentionally explicit about them because the whole point of the work is to make the wrapper's behavior traceable back to observed evidence.

