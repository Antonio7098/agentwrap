package opencode

import (
	"context"
	"io"
	"time"

	"github.com/antonioborgerees/agentwrap"
)

const (
	defaultExecutable  = "opencode"
	defaultStderrLimit = 16 * 1024
)

// Option configures an OpenCode runtime adapter.
type Option func(*Runtime)

// WithExecutable sets the OpenCode executable path.
func WithExecutable(path string) Option {
	return func(r *Runtime) {
		if path != "" {
			r.executable = path
		}
	}
}

// WithExtraArgs appends adapter-local OpenCode CLI arguments after the default
// structured-output flags and before the prompt.
func WithExtraArgs(args ...string) Option {
	return func(r *Runtime) {
		r.extraArgs = append([]string(nil), args...)
	}
}

// WithEnv appends environment values for the OpenCode subprocess.
func WithEnv(env ...string) Option {
	return func(r *Runtime) {
		r.env = append([]string(nil), env...)
	}
}

// WithStderrLimit bounds retained stderr diagnostics in bytes.
func WithStderrLimit(limit int) Option {
	return func(r *Runtime) {
		if limit > 0 {
			r.stderrLimit = limit
		}
	}
}

func withProcessRunner(runner processRunner) Option {
	return func(r *Runtime) {
		if runner != nil {
			r.runner = runner
		}
	}
}

type clock func() time.Time

func withClock(now clock) Option {
	return func(r *Runtime) {
		if now != nil {
			r.now = now
		}
	}
}

// Runtime implements agentwrap.Runtime for the OpenCode CLI.
type Runtime struct {
	executable  string
	extraArgs   []string
	env         []string
	stderrLimit int
	runner      processRunner
	now         clock
}

var _ agentwrap.Runtime = (*Runtime)(nil)

// NewRuntime constructs an OpenCode runtime adapter.
func NewRuntime(options ...Option) *Runtime {
	r := &Runtime{
		executable:  defaultExecutable,
		stderrLimit: defaultStderrLimit,
		runner:      execProcessRunner{},
		now:         time.Now,
	}
	for _, opt := range options {
		opt(r)
	}
	return r
}

// Capabilities reports the Sprint 3 OpenCode adapter surface. Retained session
// depth, validation, retry, fallback, and health/config checks are later sprint
// concerns.
func (r *Runtime) Capabilities(context.Context) (agentwrap.Capabilities, error) {
	return agentwrap.Capabilities{
		RuntimeKind: agentwrap.RuntimeKind("opencode"),
		Features: map[agentwrap.Capability]agentwrap.CapabilitySupport{
			agentwrap.CapabilityStructuredEvents: {Supported: true, Detail: "uses opencode run --format json"},
			agentwrap.CapabilityRawPayloads:      {Supported: true, Detail: "preserves native JSON lines as unsafe raw payloads"},
			agentwrap.CapabilityCancellation:     {Supported: true, Detail: "best-effort subprocess cancellation"},
			agentwrap.CapabilityArtifacts:        {Supported: true, Detail: "artifact references are projected when native events include them"},
			agentwrap.CapabilityUsage:            {Supported: true, Detail: "usage is projected when native events include token data"},
			agentwrap.CapabilityPermissions:      {Supported: true, Detail: "permission requests are surfaced and non-interactive OpenCode auto-rejects by default"},
			agentwrap.CapabilitySessions:         {Supported: false, Detail: "full retained-session lifecycle is not supported"},
			agentwrap.CapabilitySessionContinue:  {Supported: true, Detail: "passes requested session id to opencode --session as best effort"},
			agentwrap.CapabilitySessionFork:      {Supported: false, Detail: "forking retained sessions is not supported"},
			agentwrap.CapabilitySessionReplace:   {Supported: false, Detail: "replacing retained sessions is not supported"},
			agentwrap.CapabilitySessionRelease:   {Supported: false, Detail: "releasing retained sessions is not supported"},
			agentwrap.CapabilityValidationEvents: {Supported: false, Detail: "output validation is Sprint 7 scope"},
		},
		Unsupported: []agentwrap.UnsupportedCapability{
			{Capability: agentwrap.CapabilitySessions, Reason: "full retained-session lifecycle is not supported"},
			{Capability: agentwrap.CapabilitySessionFork, Reason: "OpenCode adapter does not support session fork"},
			{Capability: agentwrap.CapabilitySessionReplace, Reason: "OpenCode adapter does not support session replace"},
			{Capability: agentwrap.CapabilitySessionRelease, Reason: "OpenCode adapter does not support session release"},
			{Capability: agentwrap.CapabilityValidationEvents, Reason: "validation and repair are owned by a later sprint"},
		},
	}, nil
}

type processRunner interface {
	Start(context.Context, processSpec) (process, error)
}

type process interface {
	Stdout() io.ReadCloser
	Stderr() io.Reader
	Wait() processResult
	Cancel(context.Context) cleanupResult
}

type processSpec struct {
	Executable string
	Args       []string
	Env        []string
	WorkDir    string
}

type processResult struct {
	ExitCode int
	Err      error
}

type cleanupResult struct {
	GracefulAttempted bool
	ForceAttempted    bool
	Err               error
}
