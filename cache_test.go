package agentwrap

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestValidatePromptCacheDirectiveChecksExactPrefix(t *testing.T) {
	prompt := "stable prefix\nvolatile suffix"
	prefix := "stable prefix\n"
	digest := sha256.Sum256([]byte(prefix))
	directive := PromptCacheDirective{
		Key:             "qa-investigator/cohort-a",
		BreakpointBytes: len(prefix),
		PrefixSHA256:    fmt.Sprintf("%x", digest[:]),
		Mode:            "stable-prefix",
	}
	if err := ValidatePromptCacheDirective(prompt, directive); err != nil {
		t.Fatalf("valid directive rejected: %v", err)
	}
	directive.BreakpointBytes++
	if err := ValidatePromptCacheDirective(prompt, directive); err == nil {
		t.Fatal("mismatched prefix bytes accepted")
	}
}

func TestValidatePromptCacheDirectiveRejectsPartialDirective(t *testing.T) {
	if err := ValidatePromptCacheDirective("prompt", PromptCacheDirective{Mode: "stable-prefix"}); err == nil {
		t.Fatal("partial directive accepted")
	}
}

func TestPromptCacheDirectiveFromMetadata(t *testing.T) {
	directive, found, err := PromptCacheDirectiveFromMetadata(map[string]string{
		MetadataPromptCacheKey:             "cohort",
		MetadataPromptCacheBreakpointBytes: "12",
		MetadataPromptCachePrefixSHA256:    "digest",
		MetadataPromptCacheMode:            "stable-prefix",
	})
	if err != nil || !found || directive.Key != "cohort" || directive.BreakpointBytes != 12 || directive.PrefixSHA256 != "digest" || directive.Mode != "stable-prefix" {
		t.Fatalf("directive = %#v found=%t err=%v", directive, found, err)
	}
}
