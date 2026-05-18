// Package opencode adapts the OpenCode CLI to the runtime-neutral agentwrap
// contract.
//
// The adapter launches `opencode run --format json` and treats stdout as a
// structured JSON-lines event stream. OpenCode command details and native event
// shapes stay in this package; callers consume canonical agentwrap events,
// final results, and classified SDK errors. Raw native payloads are preserved
// for diagnostics and are marked unsafe by default.
package opencode
