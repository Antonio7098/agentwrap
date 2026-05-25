package opencode

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Antonio7098/agentwrap"
)

type rateLimitClassification struct {
	err  *agentwrap.SDKError
	info *agentwrap.RateLimitInfo
}

func classifyRateLimitText(op, text string, runtimeCtx agentwrap.RuntimeContext) *rateLimitClassification {
	return classifyRateLimitPayload(op, text, nil, runtimeCtx)
}

func classifyRateLimitData(op string, data map[string]any, runtimeCtx agentwrap.RuntimeContext) *rateLimitClassification {
	if len(data) == 0 {
		return nil
	}
	if nested, ok := data["error"].(map[string]any); ok {
		if strings.EqualFold(stringValue(nested["type"]), "rate_limit_error") {
			message := stringValue(firstNonNil(nested["message"], data["message"]))
			headers := headerMapFromAny(firstNonNil(nested["responseHeaders"], nested["headers"], data["responseHeaders"], data["headers"]))
			body := stringValue(firstNonNil(nested["responseBody"], nested["body"], data["responseBody"], data["body"]))
			metadata := stringMapFromAny(nested["metadata"])
			if len(metadata) == 0 {
				metadata = stringMapFromAny(data["metadata"])
			}
			status, _ := intFromAny(firstNonNil(nested["statusCode"], nested["status"], nested["code"], data["statusCode"], data["status"], data["code"]))
			info := rateLimitInfoFrom(headers, metadata, runtimeCtx, body, message, "nested_error_type")
			return &rateLimitClassification{
				err:  agentwrap.NewError(agentwrap.ErrorRateLimit, op, userRateLimitDetail(body, message), nil, rateLimitErrorOptions(status, headers, body, metadata, runtimeCtx, info)...),
				info: info,
			}
		}
	}
	message := messageFrom(data)
	headers := headerMapFromAny(data["responseHeaders"])
	if len(headers) == 0 {
		headers = headerMapFromAny(data["headers"])
	}
	body := stringValue(data["responseBody"])
	if body == "" {
		body = stringValue(data["body"])
	}
	status, _ := intFromAny(firstNonNil(data["statusCode"], data["status"], data["code"]))
	metadata := stringMapFromAny(data["metadata"])
	if len(metadata) == 0 {
		if nested, ok := data["error"].(map[string]any); ok {
			metadata = stringMapFromAny(nested["metadata"])
		}
	}
	return classifyRateLimitDetails(op, message, body, headers, metadata, status, runtimeCtx)
}

func classifyRateLimitPayload(op, text string, headers map[string]string, runtimeCtx agentwrap.RuntimeContext) *rateLimitClassification {
	status, body, message, parsedHeaders, metadata := parseStructuredRateLimitText(text)
	if len(headers) == 0 {
		headers = parsedHeaders
	}
	return classifyRateLimitDetails(op, message, body, headers, metadata, status, runtimeCtx)
}

func classifyRateLimitDetails(op, message, body string, headers, metadata map[string]string, status int, runtimeCtx agentwrap.RuntimeContext) *rateLimitClassification {
	body = expandNestedRateLimitBody(body)
	normalizedBody := strings.ToLower(body)
	normalizedMessage := strings.ToLower(message)
	if status == 429 {
		if containsAny(normalizedBody, "insufficient_quota", "quota_exceeded", "freeusagelimiterror") ||
			containsAny(normalizedMessage, "insufficient_quota", "quota_exceeded", "freeusagelimiterror", "quota exceeded") {
			return nil
		}
		if containsAny(normalizedBody, "content_policy", "content_filter", "safety") ||
			containsAny(normalizedMessage, "content_policy", "content_filter", "safety") {
			return nil
		}
		info := rateLimitInfoFrom(headers, metadata, runtimeCtx, body, message, "http_429")
		return &rateLimitClassification{
			err:  agentwrap.NewError(agentwrap.ErrorRateLimit, op, userRateLimitDetail(body, message), nil, rateLimitErrorOptions(status, headers, body, metadata, runtimeCtx, info)...),
			info: info,
		}
	}
	if containsAny(normalizedBody, "gousagelimiterror", "too_many_requests", "rate_limit_error", "usage limit exceeded", "\"rate_limit\"", "\"resource_exhausted\"", "\"unavailable\"") ||
		containsAny(normalizedMessage, "rate limit", "too many requests", "usage limit exceeded", "rate_limit_error", "rate increased too quickly", "provider is overloaded", "resource_exhausted", "too_many_requests") {
		info := rateLimitInfoFrom(headers, metadata, runtimeCtx, body, message, "message_or_body")
		return &rateLimitClassification{
			err:  agentwrap.NewError(agentwrap.ErrorRateLimit, op, userRateLimitDetail(body, message), nil, rateLimitErrorOptions(status, headers, body, metadata, runtimeCtx, info)...),
			info: info,
		}
	}
	if status == 503 || status == 504 || status == 529 {
		info := rateLimitInfoFrom(headers, metadata, runtimeCtx, body, message, "retryable_status")
		return &rateLimitClassification{
			err:  agentwrap.NewError(agentwrap.ErrorRateLimit, op, "OpenCode provider is temporarily rate limited or overloaded", nil, rateLimitErrorOptions(status, headers, body, metadata, runtimeCtx, info)...),
			info: info,
		}
	}
	return nil
}

func expandNestedRateLimitBody(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return body
	}
	var payload any
	if json.Unmarshal([]byte(body), &payload) != nil {
		return body
	}
	parts := []string{body}
	collectRateLimitStrings(payload, &parts)
	return strings.Join(parts, "\n")
}

func collectRateLimitStrings(value any, parts *[]string) {
	switch v := value.(type) {
	case map[string]any:
		for _, nested := range v {
			collectRateLimitStrings(nested, parts)
		}
	case []any:
		for _, nested := range v {
			collectRateLimitStrings(nested, parts)
		}
	case string:
		if strings.TrimSpace(v) == "" {
			return
		}
		*parts = append(*parts, v)
		var nested any
		if json.Unmarshal([]byte(v), &nested) == nil {
			collectRateLimitStrings(nested, parts)
		}
	}
}

func rateLimitErrorOptions(status int, headers map[string]string, body string, metadata map[string]string, runtimeCtx agentwrap.RuntimeContext, info *agentwrap.RateLimitInfo) []agentwrap.ErrorOption {
	opts := []agentwrap.ErrorOption{
		agentwrap.WithDebugDetail(debugRateLimitDetail(status, headers, body, "")),
		agentwrap.WithStatusCode(status),
		agentwrap.WithResponse(headers, body),
		agentwrap.WithMetadata(metadata),
		agentwrap.WithProviderModel(runtimeCtx.Provider, runtimeCtx.Model),
		agentwrap.WithRuntimeKind(runtimeCtx.RuntimeKind),
	}
	if info != nil && info.RetryAfter > 0 {
		opts = append(opts, agentwrap.WithRetryAfter(info.RetryAfter))
	}
	return opts
}

func parseStructuredRateLimitText(text string) (status int, body, message string, headers, metadata map[string]string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, "", "", nil, nil
	}
	var payload map[string]any
	if json.Unmarshal([]byte(text), &payload) != nil {
		return 0, text, text, nil, nil
	}
	status, _ = intFromAny(firstNonNil(payload["statusCode"], payload["status"], payload["code"]))
	body = stringValue(firstNonNil(payload["responseBody"], payload["body"]))
	message = stringValue(firstNonNil(payload["message"], payload["error"]))
	headers = headerMapFromAny(firstNonNil(payload["responseHeaders"], payload["headers"]))
	metadata = stringMapFromAny(payload["metadata"])
	if errObj, ok := payload["error"].(map[string]any); ok {
		if strings.EqualFold(stringValue(errObj["type"]), "rate_limit_error") {
			message = stringValue(firstNonNil(errObj["message"], errObj["type"], message))
			if body == "" {
				body = stringValue(firstNonNil(errObj["body"], errObj["type"]))
			}
		}
		if message == "" {
			message = stringValue(firstNonNil(errObj["message"], errObj["type"], errObj["code"]))
		}
		if body == "" {
			body = stringValue(errObj["body"])
		}
		if len(headers) == 0 {
			headers = headerMapFromAny(firstNonNil(errObj["responseHeaders"], errObj["headers"]))
		}
		if len(metadata) == 0 {
			metadata = stringMapFromAny(errObj["metadata"])
		}
	}
	return status, body, message, headers, metadata
}

func rateLimitInfoFrom(headers, metadata map[string]string, runtimeCtx agentwrap.RuntimeContext, body, message, source string) *agentwrap.RateLimitInfo {
	info := &agentwrap.RateLimitInfo{
		Provider:   runtimeCtx.Provider,
		Model:      runtimeCtx.Model,
		Source:     source,
		UserDetail: userRateLimitDetail(body, message),
	}
	if retryAfter := firstNonEmptyCI(headers, "retry-after-ms"); retryAfter != "" {
		if ms, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil && ms > 0 {
			info.RetryAfter = time.Duration(ms) * time.Millisecond
		}
	}
	if info.RetryAfter == 0 {
		if retryAfter := firstNonEmptyCI(headers, "retry-after"); retryAfter != "" {
			info.RetryAfter = parseRetryDelay(retryAfter)
		}
	}
	if info.RetryAfter == 0 {
		if retryAfter := firstNonEmptyCI(metadata, "retry-after-ms"); retryAfter != "" {
			if ms, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil && ms > 0 {
				info.RetryAfter = time.Duration(ms) * time.Millisecond
			}
		}
	}
	if info.RetryAfter == 0 {
		if retryAfter := firstNonEmptyCI(metadata, "retry-after"); retryAfter != "" {
			info.RetryAfter = parseRetryDelay(retryAfter)
		}
	}
	limit := map[string]string{}
	remaining := map[string]string{}
	reset := map[string]string{}
	for key, value := range headers {
		lower := strings.ToLower(strings.TrimSpace(key))
		switch {
		case strings.HasPrefix(lower, "x-ratelimit-limit-"):
			limit[strings.TrimPrefix(lower, "x-ratelimit-limit-")] = value
		case strings.HasPrefix(lower, "x-ratelimit-remaining-"):
			remaining[strings.TrimPrefix(lower, "x-ratelimit-remaining-")] = value
		case strings.HasPrefix(lower, "x-ratelimit-reset-"):
			scope := strings.TrimPrefix(lower, "x-ratelimit-reset-")
			reset[scope] = value
			if info.ResetAt.IsZero() {
				info.ResetAt = parseResetAt(value)
			}
		case strings.HasPrefix(lower, "anthropic-ratelimit-") && strings.Contains(lower, "-limit"):
			scope := strings.TrimSuffix(strings.TrimPrefix(lower, "anthropic-ratelimit-"), "-limit")
			limit[scope] = value
		case strings.HasPrefix(lower, "anthropic-ratelimit-") && strings.Contains(lower, "-remaining"):
			scope := strings.TrimSuffix(strings.TrimPrefix(lower, "anthropic-ratelimit-"), "-remaining")
			remaining[scope] = value
		case strings.HasPrefix(lower, "anthropic-ratelimit-") && strings.Contains(lower, "-reset"):
			scope := strings.TrimSuffix(strings.TrimPrefix(lower, "anthropic-ratelimit-"), "-reset")
			reset[scope] = value
			if info.ResetAt.IsZero() {
				info.ResetAt = parseResetAt(value)
			}
		}
	}
	for key, value := range metadata {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "provider") && info.Provider == "" {
			info.Provider = agentwrap.ProviderID(value)
		}
		if strings.Contains(lower, "model") && info.Model == "" {
			info.Model = agentwrap.ModelID(value)
		}
	}
	info.NativeMetadata = agentwrap.RedactMetadata(map[string]any{
		"headers":   agentwrap.RedactStringMap(headers),
		"metadata":  agentwrap.RedactStringMap(metadata),
		"body":      agentwrap.RedactString(body),
		"message":   agentwrap.RedactString(message),
		"limit":     limit,
		"remaining": remaining,
		"reset":     reset,
	})
	return info
}

func userRateLimitDetail(body, message string) string {
	lowerBody := strings.ToLower(body)
	lowerMessage := strings.ToLower(message)
	lower := lowerBody + " " + lowerMessage
	switch {
	case strings.Contains(lower, "gousagelimiterror"):
		return "OpenCode account rate limit reached"
	case strings.Contains(lower, "freeusagelimiterror"):
		return "OpenCode free-tier usage limit reached"
	case strings.Contains(lower, "resource_exhausted"), strings.Contains(lower, "\"unavailable\""):
		return "OpenCode provider is overloaded"
	}
	if strings.TrimSpace(message) != "" {
		return agentwrap.RedactString(message)
	}
	return "OpenCode provider rate limit reached"
}

func debugRateLimitDetail(status int, headers map[string]string, body, message string) string {
	return fmt.Sprintf("status=%d headers=%v message=%q body=%q", status, agentwrap.RedactStringMap(headers), agentwrap.RedactString(message), agentwrap.RedactString(body))
}

func parseRetryDelay(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		return time.Duration(seconds * float64(time.Second))
	}
	if when, err := time.Parse(time.RFC1123, value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	if when, err := time.Parse(time.RFC1123Z, value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	if parsed, err := time.ParseDuration(strings.ToLower(value)); err == nil {
		return parsed
	}
	return 0
}

func parseResetAt(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	if d := parseRetryDelay(value); d > 0 {
		return time.Now().Add(d)
	}
	if when, err := time.Parse(time.RFC3339, value); err == nil {
		return when
	}
	return time.Time{}
}

func firstNonEmptyCI(values map[string]string, key string) string {
	for k, v := range values {
		if strings.EqualFold(strings.TrimSpace(k), key) && strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func headerMapFromAny(value any) map[string]string {
	switch v := value.(type) {
	case map[string]string:
		return agentwrap.RedactStringMap(v)
	case map[string]any:
		out := make(map[string]string, len(v))
		for key, value := range v {
			out[key] = stringify(value)
		}
		return agentwrap.RedactStringMap(out)
	default:
		return nil
	}
}

func stringMapFromAny(value any) map[string]string {
	switch v := value.(type) {
	case map[string]string:
		return agentwrap.RedactStringMap(v)
	case map[string]any:
		out := make(map[string]string, len(v))
		for key, value := range v {
			out[key] = stringify(value)
		}
		return agentwrap.RedactStringMap(out)
	default:
		return nil
	}
}

func stringify(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(v)
	}
}

func intFromAny(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		return n, err == nil
	default:
		return 0, false
	}
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
