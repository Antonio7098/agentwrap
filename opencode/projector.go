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
	event            agentwrap.Event
	final            bool
	idle             bool
	usage            agentwrap.Usage
	artifacts        []agentwrap.ArtifactRef
	warnings         []string
	fatal            *agentwrap.SDKError
	rateLimit        *agentwrap.RateLimitInfo
	finishReason     string
	terminalEvidence string
}

func projectNative(in projectionInput) projectionResult {
	record := in.record
	category, typ := classify(record)
	finishReason := finishReasonFrom(record.Data)
	nativeType := strings.TrimSpace(record.Type)
	idle := isSessionIdleStatus(record)
	terminalEvidence := ""
	if nativeType == "step_finish" {
		if isTerminalFinish(finishReason) {
			category = agentwrap.EventFinalResult
			terminalEvidence = "step_finish"
		} else {
			category = agentwrap.EventProgress
		}
	} else if idle {
		terminalEvidence = "session.status.idle"
	}
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
	if finishReason != "" {
		payload["finish_reason"] = finishReason
	}
	event := agentwrap.Event{
		ID:        agentwrap.EventID(fmt.Sprintf("%s:%d", in.runID, in.seq)),
		RunID:     in.runID,
		SessionID: firstSessionID(agentwrap.SessionID(record.SessionID), agentwrap.SessionID(stringValue(record.Data["sessionID"])), agentwrap.SessionID(propertiesStringValue(record.Data, "sessionID"))),
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
	result := projectionResult{event: event, idle: idle, finishReason: finishReason, terminalEvidence: terminalEvidence}
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
			result.fatal = classifyFatalEventError(record.Data, in.ctx)
		}
	}
	return result
}

func finishReasonFrom(data map[string]any) string {
	for _, key := range []string{"finish_reason", "finishReason", "stopReason", "stop_reason", "reason", "finish"} {
		if value := stringValue(data[key]); value != "" {
			return value
		}
	}
	if part, ok := data["part"].(map[string]any); ok {
		for _, key := range []string{"finish_reason", "finishReason", "stopReason", "stop_reason", "reason", "finish"} {
			if value := stringValue(part[key]); value != "" {
				return value
			}
		}
	}
	return ""
}

func isTerminalFinish(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "", "stop", "end_turn", "end-turn", "stop_sequence", "stop-sequence", "complete", "completed":
		return true
	case "tool_calls", "tool-calls", "tool_call", "tool-call", "tool", "max_tokens", "max-tokens", "length", "content_filter", "content-filter":
		return false
	default:
		return false
	}
}

func isSessionIdleStatus(record nativeRecord) bool {
	if strings.TrimSpace(record.Type) != "session.status" {
		return false
	}
	if strings.EqualFold(stringValue(record.Data["status"]), "idle") {
		return true
	}
	if status, ok := record.Data["status"].(map[string]any); ok {
		return strings.EqualFold(stringValue(status["type"]), "idle")
	}
	if properties, ok := record.Data["properties"].(map[string]any); ok {
		if strings.EqualFold(stringValue(properties["status"]), "idle") {
			return true
		}
		if status, ok := properties["status"].(map[string]any); ok {
			return strings.EqualFold(stringValue(status["type"]), "idle")
		}
	}
	return false
}

func classifyFatalEventError(data map[string]any, ctx agentwrap.RuntimeContext) *agentwrap.SDKError {
	message := messageFrom(data)
	normalized := strings.ToLower(message)
	switch {
	case strings.Contains(normalized, "model not found") || strings.Contains(normalized, "unknown model"):
		return agentwrap.NewError(agentwrap.ErrorModelUnavailable, "opencode event", "OpenCode model is unavailable", nil, agentwrap.WithDebugDetail(message), agentwrap.WithProviderModel(ctx.Provider, ctx.Model), agentwrap.WithRuntimeKind(ctx.RuntimeKind))
	case strings.Contains(normalized, "unauthorized") || strings.Contains(normalized, "authentication") || strings.Contains(normalized, "invalid api key") || strings.Contains(normalized, "api key"):
		return agentwrap.NewError(agentwrap.ErrorAuthentication, "opencode event", "OpenCode provider authentication failed", nil, agentwrap.WithDebugDetail(message), agentwrap.WithProviderModel(ctx.Provider, ctx.Model), agentwrap.WithRuntimeKind(ctx.RuntimeKind))
	default:
		return agentwrap.NewError(agentwrap.ErrorRuntimeExit, "opencode event", "OpenCode reported a fatal session error", nil, agentwrap.WithDebugDetail(message), agentwrap.WithProviderModel(ctx.Provider, ctx.Model), agentwrap.WithRuntimeKind(ctx.RuntimeKind))
	}
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

func propertiesStringValue(data map[string]any, key string) string {
	properties, ok := data["properties"].(map[string]any)
	if !ok {
		return ""
	}
	return stringValue(properties[key])
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
