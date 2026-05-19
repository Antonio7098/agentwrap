package agentwrap

import (
	"fmt"
	"regexp"
	"strings"
)

var secretNamePattern = regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password|authorization|credential|bearer)`)
var bearerPattern = regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+/=-]+`)
var assignmentSecretPattern = regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password|authorization|credential)=([^ \n\r\t]+)`)

// RedactString removes common credential forms from diagnostic text.
func RedactString(value string) string {
	value = bearerPattern.ReplaceAllString(value, "Bearer [REDACTED]")
	return assignmentSecretPattern.ReplaceAllString(value, "$1=[REDACTED]")
}

// RedactMetadata recursively redacts secret-looking metadata values.
func RedactMetadata(metadata map[string]any) map[string]any {
	if metadata == nil {
		return nil
	}
	out := make(map[string]any, len(metadata))
	for key, value := range metadata {
		if secretNamePattern.MatchString(key) {
			out[key] = "[REDACTED]"
			continue
		}
		out[key] = redactMetadataValue(value)
	}
	return out
}

func redactMetadataValue(value any) any {
	switch v := value.(type) {
	case string:
		return RedactString(v)
	case map[string]any:
		return RedactMetadata(v)
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = redactMetadataValue(v[i])
		}
		return out
	default:
		return value
	}
}

// RedactStringMap redacts secret-looking string map keys and values.
func RedactStringMap(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if secretNamePattern.MatchString(key) {
			out[key] = "[REDACTED]"
			continue
		}
		out[key] = RedactString(value)
	}
	return out
}

// SecretFromEnv records secret presence from an env assignment without value.
func SecretFromEnv(env string, source ConfigSource) (SecretValue, bool) {
	name, _, ok := strings.Cut(env, "=")
	if !ok || !secretNamePattern.MatchString(name) {
		return SecretValue{}, false
	}
	return SecretValue{Name: name, Source: source, Present: true}, true
}

// RedactEnv redacts secret-looking environment assignments.
func RedactEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, item := range env {
		name, value, ok := strings.Cut(item, "=")
		if !ok {
			out = append(out, RedactString(item))
			continue
		}
		if secretNamePattern.MatchString(name) {
			out = append(out, fmt.Sprintf("%s=[REDACTED]", name))
			continue
		}
		out = append(out, name+"="+RedactString(value))
	}
	return out
}
