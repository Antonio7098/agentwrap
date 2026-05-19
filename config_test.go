package agentwrap

import (
	"testing"
	"time"
)

func TestMergeEffectiveConfigTracksPrecedenceAndSources(t *testing.T) {
	defaultProvider := ProviderID("default")
	envProvider := ProviderID("env")
	callerProvider := ProviderID("caller")
	timeout := 5 * time.Second

	cfg := MergeEffectiveConfig("fake",
		ConfigLayer{Source: ConfigSourceDefault, Provider: &defaultProvider},
		ConfigLayer{Source: ConfigSourceEnvironment, Provider: &envProvider},
		ConfigLayer{Source: ConfigSourceCallerRequest, Provider: &callerProvider, Timeout: &timeout, Metadata: map[string]string{"k": "v"}},
	)

	if cfg.Provider.Value != "caller" || cfg.Provider.Source != ConfigSourceCallerRequest {
		t.Fatalf("provider = %#v", cfg.Provider)
	}
	if cfg.Timeout.Value != timeout || cfg.Timeout.Source != ConfigSourceCallerRequest {
		t.Fatalf("timeout = %#v", cfg.Timeout)
	}
	if cfg.Metadata["k"].Value != "v" || cfg.Metadata["k"].Source != ConfigSourceCallerRequest {
		t.Fatalf("metadata = %#v", cfg.Metadata)
	}
}

func TestValidateEffectiveConfigRejectsInvalidTimeout(t *testing.T) {
	timeout := -time.Second
	cfg := MergeEffectiveConfig("fake", ConfigLayer{Source: ConfigSourceCallerRequest, Timeout: &timeout})
	err := ValidateEffectiveConfig(cfg)
	if err == nil || err.Category != ErrorConfiguration || !err.Unrecoverable {
		t.Fatalf("err = %#v, want configuration unrecoverable", err)
	}
}
