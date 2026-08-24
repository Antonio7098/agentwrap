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

const (
	ToolCallIDPayloadKey    = "tool_call_id"
	ToolNamePayloadKey      = "tool_name"
	ToolStatusPayloadKey    = "tool_status"
	ToolArgumentsPayloadKey = "tool_arguments"
	ToolResultPayloadKey    = "tool_result"
	ToolErrorPayloadKey     = "tool_error"
)

// ToolObservation is the provider-neutral view of a tool call. Arguments and
// results retain their structured shape after recursive credential redaction.
// Adapters may leave fields unset when their native protocol does not expose
// them.
type ToolObservation struct {
	CallID    string
	Name      string
	Status    string
	Arguments any
	Result    any
	Error     any
}

// EventPayloadWithToolObservation adds normalized tool facts to an event
// payload without removing provider-native fields.
func EventPayloadWithToolObservation(values EventPayload, tool ToolObservation) EventPayload {
	payload := EventPayload{}
	for key, value := range values {
		payload[key] = value
	}
	if tool.CallID != "" {
		payload[ToolCallIDPayloadKey] = tool.CallID
	}
	if tool.Name != "" {
		payload[ToolNamePayloadKey] = tool.Name
		payload["tool"] = tool.Name
	}
	if tool.Status != "" {
		payload[ToolStatusPayloadKey] = tool.Status
	}
	if tool.Arguments != nil {
		payload[ToolArgumentsPayloadKey] = redactMetadataValue(tool.Arguments)
	}
	if tool.Result != nil {
		payload[ToolResultPayloadKey] = redactMetadataValue(tool.Result)
	}
	if tool.Error != nil {
		payload[ToolErrorPayloadKey] = redactMetadataValue(tool.Error)
	}
	return payload
}

// ToolObservation returns normalized tool facts carried by the event.
func (e Event) ToolObservation() (ToolObservation, bool) {
	if e.Kind() != EventTool || e.Payload == nil {
		return ToolObservation{}, false
	}
	tool := ToolObservation{
		CallID:    stringPayloadValue(e.Payload[ToolCallIDPayloadKey]),
		Name:      stringPayloadValue(e.Payload[ToolNamePayloadKey]),
		Status:    stringPayloadValue(e.Payload[ToolStatusPayloadKey]),
		Arguments: e.Payload[ToolArgumentsPayloadKey],
		Result:    e.Payload[ToolResultPayloadKey],
		Error:     e.Payload[ToolErrorPayloadKey],
	}
	if tool.Name == "" {
		tool.Name = stringPayloadValue(e.Payload["tool"])
	}
	return tool, tool.CallID != "" || tool.Name != "" || tool.Arguments != nil || tool.Result != nil || tool.Error != nil
}

func stringPayloadValue(value any) string {
	text, _ := value.(string)
	return text
}

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
