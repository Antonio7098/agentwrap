package agentwrap

import "time"

// Event is the canonical event envelope emitted by runtimes.
type Event struct {
	ID        EventID
	RunID     RunID
	SessionID SessionID
	Time      time.Time
	Type      string
	Payload   EventPayload
	Raw       *RawPayload
}

// EventPayload is intentionally open for future event types and adapter facts.
type EventPayload map[string]any

// EventKind is a lightweight caller-facing projection of native runtime event
// types. It belongs in payload metadata because the native event Type remains
// the canonical event name.
type EventKind string

const (
	EventLifecycle       EventKind = "lifecycle"
	EventSession         EventKind = "session"
	EventMessage         EventKind = "message"
	EventProgress        EventKind = "progress"
	EventTool            EventKind = "tool"
	EventArtifact        EventKind = "artifact"
	EventPermission      EventKind = "permission"
	EventBlocking        EventKind = "blocking"
	EventUsage           EventKind = "usage"
	EventWarning         EventKind = "warning"
	EventFatalError      EventKind = "fatal_error"
	EventRateLimit       EventKind = "rate_limit"
	EventValidation      EventKind = "validation"
	EventRetry           EventKind = "retry"
	EventFallback        EventKind = "fallback"
	EventFinalResult     EventKind = "final_result"
	EventNativeExtension EventKind = "native_extension"
)

const eventKindPayloadKey = "event_kind"

// EventPayloadWithKind returns a payload containing the SDK projection kind.
func EventPayloadWithKind(kind EventKind, values EventPayload) EventPayload {
	payload := EventPayload{}
	for k, v := range values {
		payload[k] = v
	}
	payload[eventKindPayloadKey] = kind
	return payload
}

// Kind returns the SDK event projection stored in the payload, if present.
func (e Event) Kind() EventKind {
	if e.Payload == nil {
		return ""
	}
	if kind, ok := e.Payload[eventKindPayloadKey].(EventKind); ok {
		return kind
	}
	if kind, ok := e.Payload[eventKindPayloadKey].(string); ok {
		return EventKind(kind)
	}
	return ""
}

// RawPayload preserves native structured runtime data for diagnostics. Callers
// must treat it as sensitive until adapter-specific redaction rules say it is
// safe to persist or display.
type RawPayload struct {
	Source   string
	Encoding string
	Data     []byte
	Safe     bool
}
