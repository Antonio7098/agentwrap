package opencode

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Antonio7098/agentwrap"
)

type projectionInput struct {
	runID  agentwrap.RunID
	turnID agentwrap.TurnID
	ctx    agentwrap.RuntimeContext
	seq    int64
	now    time.Time
	record nativeRecord
}

type projectionResult struct {
	event     agentwrap.Event
	final     bool
	usage     agentwrap.Usage
	artifacts []agentwrap.ArtifactRef
	warnings  []string
	fatal     *agentwrap.SDKError
	rateLimit *agentwrap.RateLimitInfo
}

func projectNative(in projectionInput) projectionResult {
	record := in.record
	category, typ := classify(record)
	payload := agentwrap.EventPayload{}
	for k, v := range record.Data {
		if k == "type" || k == "timestamp" {
			continue
		}
		payload[k] = v
	}
	payload["native_type"] = record.Type
	payload["line"] = record.Line
	payload["turn_id"] = string(in.turnID)
	payload["context"] = in.ctx
	event := agentwrap.Event{
		ID:        agentwrap.EventID(fmt.Sprintf("%s:%d", in.runID, in.seq)),
		RunID:     in.runID,
		SessionID: firstSessionID(agentwrap.SessionID(record.SessionID), agentwrap.SessionID(stringValue(record.Data["sessionID"]))),
		Time:      eventTime(record.Timestamp, in.now),
		Type:      typ,
		Payload:   agentwrap.EventPayloadWithKind(category, payload),
		Raw: &agentwrap.RawPayload{
			Source:   "opencode.stdout",
			Encoding: "json",
			Data:     append([]byte(nil), record.Raw...),
			Safe:     false,
		},
	}
	if event.SessionID == "" {
		event.SessionID = agentwrap.SessionID(stringValue(record.Data["session_id"]))
	}
	result := projectionResult{event: event}
	if category == agentwrap.EventFinalResult {
		result.final = true
	}
	if category == agentwrap.EventUsage {
		result.usage = usageFrom(record.Data)
	}
	if category == agentwrap.EventArtifact {
		result.artifacts = artifactsFrom(record.Data)
	}
	if category == agentwrap.EventWarning {
		if msg := messageFrom(record.Data); msg != "" {
			result.warnings = append(result.warnings, msg)
		}
	}
	if category == agentwrap.EventFatalError {
		result.final = true
		if classified := classifyRateLimitData("opencode event", record.Data, in.ctx); classified != nil {
			result.fatal = classified.err
			result.rateLimit = classified.info
			event.Payload["rate_limit"] = classified.info
		} else {
			result.fatal = agentwrap.NewError(agentwrap.ErrorRuntimeExit, "opencode event", "OpenCode reported a fatal session error", nil, agentwrap.WithDebugDetail(messageFrom(record.Data)))
		}
	}
	return result
}

func classify(record nativeRecord) (agentwrap.EventKind, string) {
	nativeType := strings.TrimSpace(record.Type)
	switch nativeType {
	case "step_start":
		return agentwrap.EventProgress, nativeType
	case "step_finish":
		return agentwrap.EventFinalResult, nativeType
	case "text", "reasoning":
		return agentwrap.EventMessage, nativeType
	case "tool_use":
		return agentwrap.EventTool, nativeType
	case "error":
		return agentwrap.EventFatalError, nativeType
	}
	if strings.Contains(nativeType, "permission") {
		return agentwrap.EventPermission, nativeType
	}
	if strings.Contains(nativeType, "question") || strings.Contains(nativeType, "status") && strings.Contains(nativeType, "waiting") {
		return agentwrap.EventBlocking, nativeType
	}
	if strings.Contains(nativeType, "usage") || strings.Contains(nativeType, "token") || strings.Contains(nativeType, "cost") {
		return agentwrap.EventUsage, nativeType
	}
	if strings.Contains(nativeType, "artifact") || strings.Contains(nativeType, "file") {
		return agentwrap.EventArtifact, nativeType
	}
	if strings.Contains(nativeType, "warn") {
		return agentwrap.EventWarning, nativeType
	}
	if strings.Contains(nativeType, "session") {
		return agentwrap.EventSession, nativeType
	}
	return agentwrap.EventNativeExtension, nativeType
}

func eventTime(value any, fallback time.Time) time.Time {
	switch v := value.(type) {
	case float64:
		if v > 1e12 {
			return time.UnixMilli(int64(v))
		}
		return time.Unix(int64(v), 0)
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return parsed
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			if n > 1e12 {
				return time.UnixMilli(n)
			}
			return time.Unix(n, 0)
		}
	}
	return fallback
}

func firstSessionID(values ...agentwrap.SessionID) agentwrap.SessionID {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func stringValue(value any) string {
	s, _ := value.(string)
	return s
}

func messageFrom(data map[string]any) string {
	for _, key := range []string{"message", "error"} {
		if value := stringValue(data[key]); value != "" {
			return value
		}
	}
	if errObj, ok := data["error"].(map[string]any); ok {
		if dataObj, ok := errObj["data"].(map[string]any); ok {
			return stringValue(dataObj["message"])
		}
		return stringValue(errObj["message"])
	}
	return ""
}

func usageFrom(data map[string]any) agentwrap.Usage {
	usage := agentwrap.Usage{Native: data}
	if n, ok := int64From(data["input_tokens"]); ok {
		usage.InputTokens = &n
	}
	if n, ok := int64From(data["output_tokens"]); ok {
		usage.OutputTokens = &n
	}
	if n, ok := int64From(data["total_tokens"]); ok {
		usage.TotalTokens = &n
	}
	return usage
}

func int64From(value any) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

func artifactsFrom(data map[string]any) []agentwrap.ArtifactRef {
	uri := stringValue(data["uri"])
	if uri == "" {
		uri = stringValue(data["path"])
	}
	if uri == "" {
		return nil
	}
	return []agentwrap.ArtifactRef{{
		URI:         uri,
		Kind:        stringValue(data["kind"]),
		Description: stringValue(data["description"]),
	}}
}
