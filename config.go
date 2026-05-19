package agentwrap

import (
	"errors"
	"fmt"
	"time"
)

// ConfigSource identifies where an effective config value came from.
type ConfigSource string

const (
	ConfigSourceDefault           ConfigSource = "default"
	ConfigSourceAdapterOption     ConfigSource = "adapter_option"
	ConfigSourceEnvironment       ConfigSource = "environment"
	ConfigSourceConfigProvider    ConfigSource = "config_provider"
	ConfigSourceCallerRequest     ConfigSource = "caller_request"
	ConfigSourceRuntimeDiscovered ConfigSource = "runtime_discovered"
)

// ConfigValue stores a value with field-level provenance.
type ConfigValue[T comparable] struct {
	Value  T
	Source ConfigSource
	Set    bool
}

// SecretValue reports secret presence and source without exposing content.
type SecretValue struct {
	Name    string
	Source  ConfigSource
	Present bool
}

// EffectiveConfig is an immutable post-merge view of runtime setup.
type EffectiveConfig struct {
	RuntimeKind RuntimeKind
	RuntimeName ConfigValue[string]
	Executable  ConfigValue[string]
	Provider    ConfigValue[ProviderID]
	Model       ConfigValue[ModelID]
	WorkDir     ConfigValue[string]
	Permissions ConfigValue[PermissionMode]
	Sandbox     ConfigValue[SandboxMode]
	Timeout     ConfigValue[time.Duration]
	SessionID   ConfigValue[SessionID]
	Metadata    map[string]ConfigValue[string]
	Secrets     []SecretValue
}

// ConfigLayer is one source layer in precedence order.
type ConfigLayer struct {
	Source      ConfigSource
	RuntimeName *string
	Executable  *string
	Provider    *ProviderID
	Model       *ModelID
	WorkDir     *string
	Permissions *PermissionMode
	Sandbox     *SandboxMode
	Timeout     *time.Duration
	SessionID   *SessionID
	Metadata    map[string]string
	Secrets     []SecretValue
}

// MergeEffectiveConfig applies layers from low to high precedence.
func MergeEffectiveConfig(kind RuntimeKind, layers ...ConfigLayer) EffectiveConfig {
	cfg := EffectiveConfig{RuntimeKind: kind, Metadata: map[string]ConfigValue[string]{}}
	for _, layer := range layers {
		if layer.RuntimeName != nil {
			cfg.RuntimeName = ConfigValue[string]{Value: *layer.RuntimeName, Source: layer.Source, Set: true}
		}
		if layer.Executable != nil {
			cfg.Executable = ConfigValue[string]{Value: *layer.Executable, Source: layer.Source, Set: true}
		}
		if layer.Provider != nil {
			cfg.Provider = ConfigValue[ProviderID]{Value: *layer.Provider, Source: layer.Source, Set: true}
		}
		if layer.Model != nil {
			cfg.Model = ConfigValue[ModelID]{Value: *layer.Model, Source: layer.Source, Set: true}
		}
		if layer.WorkDir != nil {
			cfg.WorkDir = ConfigValue[string]{Value: *layer.WorkDir, Source: layer.Source, Set: true}
		}
		if layer.Permissions != nil {
			cfg.Permissions = ConfigValue[PermissionMode]{Value: *layer.Permissions, Source: layer.Source, Set: true}
		}
		if layer.Sandbox != nil {
			cfg.Sandbox = ConfigValue[SandboxMode]{Value: *layer.Sandbox, Source: layer.Source, Set: true}
		}
		if layer.Timeout != nil {
			cfg.Timeout = ConfigValue[time.Duration]{Value: *layer.Timeout, Source: layer.Source, Set: true}
		}
		if layer.SessionID != nil {
			cfg.SessionID = ConfigValue[SessionID]{Value: *layer.SessionID, Source: layer.Source, Set: true}
		}
		for key, value := range layer.Metadata {
			cfg.Metadata[key] = ConfigValue[string]{Value: value, Source: layer.Source, Set: true}
		}
		for _, secret := range layer.Secrets {
			secret.Source = layer.Source
			cfg.Secrets = append(cfg.Secrets, secret)
		}
	}
	return cfg
}

// ValidateEffectiveConfig rejects invalid values before runtime execution.
func ValidateEffectiveConfig(cfg EffectiveConfig) *SDKError {
	switch {
	case cfg.RuntimeName.Set && cfg.RuntimeName.Value == "":
		return configValidationError("runtime name cannot be empty")
	case cfg.Executable.Set && cfg.Executable.Value == "":
		return configValidationError("runtime executable cannot be empty")
	case cfg.Provider.Set && cfg.Provider.Value == "":
		return configValidationError("provider cannot be empty when set")
	case cfg.Model.Set && cfg.Model.Value == "":
		return configValidationError("model cannot be empty when set")
	case cfg.Timeout.Set && cfg.Timeout.Value < 0:
		return configValidationError("timeout cannot be negative")
	case cfg.WorkDir.Set && cfg.WorkDir.Value == "":
		return configValidationError("workdir cannot be empty when set")
	}
	return nil
}

func configValidationError(detail string) *SDKError {
	return NewError(ErrorConfiguration, "config validation", detail, errors.New(detail), WithUserActionable(true), WithUnrecoverable(true))
}

// CallerConfigLayer converts a run request into highest-precedence config.
func CallerConfigLayer(req RunRequest) ConfigLayer {
	layer := ConfigLayer{Source: ConfigSourceCallerRequest, Metadata: req.Metadata}
	if req.Provider != "" {
		layer.Provider = &req.Provider
	}
	if req.Model != "" {
		layer.Model = &req.Model
	}
	if req.WorkDir != "" {
		layer.WorkDir = &req.WorkDir
	}
	if req.Permissions != "" {
		layer.Permissions = &req.Permissions
	}
	if req.Sandbox != "" {
		layer.Sandbox = &req.Sandbox
	}
	if req.Timeout != 0 {
		layer.Timeout = &req.Timeout
	}
	if req.SessionID != "" {
		layer.SessionID = &req.SessionID
	}
	return layer
}

func (v ConfigValue[T]) String() string {
	if !v.Set {
		return ""
	}
	return fmt.Sprint(v.Value)
}
