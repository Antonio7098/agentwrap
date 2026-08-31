package agentwrap

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// MetadataPromptCacheKey and related constants keep compatibility with
	// callers that carried cache directives through request metadata before the
	// typed RunRequest field existed.
	MetadataPromptCacheKey             = "prompt_cache_key"
	MetadataPromptCacheBreakpointBytes = "prompt_cache_breakpoint_bytes"
	MetadataPromptCachePrefixSHA256    = "prompt_cache_prefix_sha256"
	MetadataPromptCacheMode            = "prompt_cache_mode"
)

// PromptCacheDirective describes a caller-owned stable prompt prefix. Runtime
// adapters must preserve the prompt bytes and report which parts of the
// directive they actually applied.
type PromptCacheDirective struct {
	Key             string
	BreakpointBytes int
	PrefixSHA256    string
	Mode            string
	RequireNative   bool
}

// Enabled reports whether the caller requested prompt-cache handling.
func (d PromptCacheDirective) Enabled() bool {
	return d.Key != "" || d.BreakpointBytes != 0 || d.PrefixSHA256 != "" || d.Mode != "" || d.RequireNative
}

// ValidatePromptCacheDirective checks both the directive shape and the exact
// byte prefix it describes. Validation never normalizes the prompt.
func ValidatePromptCacheDirective(prompt string, directive PromptCacheDirective) *SDKError {
	if !directive.Enabled() {
		return nil
	}
	if strings.TrimSpace(directive.Key) == "" {
		return promptCacheValidationError("cache key is required")
	}
	if strings.TrimSpace(directive.Mode) == "" {
		return promptCacheValidationError("cache mode is required")
	}
	if directive.BreakpointBytes <= 0 {
		return promptCacheValidationError("cache breakpoint must be greater than zero")
	}
	if directive.BreakpointBytes > len(prompt) {
		return promptCacheValidationError("cache breakpoint exceeds prompt length")
	}
	digest := strings.ToLower(strings.TrimSpace(directive.PrefixSHA256))
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size {
		return promptCacheValidationError("cache prefix SHA-256 must contain 64 hexadecimal characters")
	}
	want := sha256.Sum256([]byte(prompt[:directive.BreakpointBytes]))
	if digest != hex.EncodeToString(want[:]) {
		return promptCacheValidationError("cache prefix SHA-256 does not match prompt bytes")
	}
	return nil
}

func promptCacheValidationError(detail string) *SDKError {
	return NewError(ErrorConfiguration, "prompt cache directive", detail, errors.New(detail))
}

// PromptCacheKeySHA256 returns a safe identity for audit metadata without
// exposing the caller's routing key.
func PromptCacheKeySHA256(key string) string {
	digest := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", digest[:])
}

// PromptCacheDirectiveFromMetadata promotes the legacy metadata transport to
// the typed contract. New callers should set RunRequest.PromptCache directly.
func PromptCacheDirectiveFromMetadata(metadata map[string]string) (PromptCacheDirective, bool, *SDKError) {
	key := strings.TrimSpace(metadata[MetadataPromptCacheKey])
	breakpoint := strings.TrimSpace(metadata[MetadataPromptCacheBreakpointBytes])
	digest := strings.TrimSpace(metadata[MetadataPromptCachePrefixSHA256])
	mode := strings.TrimSpace(metadata[MetadataPromptCacheMode])
	if key == "" && breakpoint == "" && digest == "" && mode == "" {
		return PromptCacheDirective{}, false, nil
	}
	value, err := strconv.Atoi(breakpoint)
	if err != nil {
		return PromptCacheDirective{}, true, promptCacheValidationError("cache breakpoint metadata must be an integer")
	}
	return PromptCacheDirective{Key: key, BreakpointBytes: value, PrefixSHA256: digest, Mode: mode}, true, nil
}
