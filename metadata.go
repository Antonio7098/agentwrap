package agentwrap

import "time"

// RuntimeContext identifies the runtime/provider/model context for events,
// artifacts, and final results.
type RuntimeContext struct {
	RuntimeKind RuntimeKind
	RuntimeName string
	Provider    ProviderID
	Model       ModelID
}

// RunMetadata contains dashboard- and audit-oriented run metadata. Most fields
// are best-effort until later adapter and observability sprints define stronger
// guarantees.
type RunMetadata struct {
	Context        RuntimeContext
	ParentRunID    RunID
	Attempt        int
	Status         LifecycleState
	StartedAt      time.Time
	FinishedAt     time.Time
	Duration       time.Duration
	Session        SessionMetadata
	Artifacts      []ArtifactRef
	Warnings       []string
	Errors         []SDKError
	Usage          Usage
	EstimatedCost  *CostEstimate
	ThroughputTPS  *float64
	NativeMetadata map[string]any
}

// SessionMetadata describes retained-session state when supported.
type SessionMetadata struct {
	ID          SessionID
	Retained    bool
	Continued   bool
	ForkedFrom  SessionID
	Replaced    SessionID
	Unsupported []UnsupportedCapability
}

// ArtifactRef references durable runtime output without embedding large data.
type ArtifactRef struct {
	ID          ArtifactID
	URI         string
	Kind        string
	Description string
	Metadata    map[string]string
}

// Usage records best-effort usage values. Unknown values remain nil.
type Usage struct {
	InputTokens  *int64
	OutputTokens *int64
	TotalTokens  *int64
	Native       map[string]any
}

// CostEstimate records best-effort cost metadata.
type CostEstimate struct {
	Amount   float64
	Currency string
	Estimate bool
}
