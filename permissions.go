package agentwrap

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// PermissionAction is the SDK-level decision for a runtime tool class.
type PermissionAction string

const (
	PermissionActionDefault PermissionAction = ""
	PermissionActionAllow   PermissionAction = "allow"
	PermissionActionDeny    PermissionAction = "deny"
	PermissionActionAsk     PermissionAction = "ask"
)

// PermissionTool is a runtime-neutral tool class. Adapters translate these
// values to their native permission vocabulary.
type PermissionTool string

const (
	PermissionToolRead              PermissionTool = "read"
	PermissionToolEdit              PermissionTool = "edit"
	PermissionToolShell             PermissionTool = "shell"
	PermissionToolSearch            PermissionTool = "search"
	PermissionToolList              PermissionTool = "list"
	PermissionToolTask              PermissionTool = "task"
	PermissionToolTodo              PermissionTool = "todo"
	PermissionToolQuestion          PermissionTool = "question"
	PermissionToolWebFetch          PermissionTool = "web_fetch"
	PermissionToolWebSearch         PermissionTool = "web_search"
	PermissionToolRepoClone         PermissionTool = "repo_clone"
	PermissionToolExternalDirectory PermissionTool = "external_directory"
	PermissionToolLanguageServer    PermissionTool = "language_server"
	PermissionToolSkill             PermissionTool = "skill"
)

// PermissionUnsupportedBehavior controls how adapters handle policy features
// they cannot enforce natively.
type PermissionUnsupportedBehavior string

const (
	PermissionUnsupportedFail       PermissionUnsupportedBehavior = ""
	PermissionUnsupportedBestEffort PermissionUnsupportedBehavior = "best_effort"
)

// PermissionEnforcement describes how a policy feature is handled.
type PermissionEnforcement string

const (
	PermissionEnforcementNative      PermissionEnforcement = "native"
	PermissionEnforcementSDKManaged  PermissionEnforcement = "sdk_managed"
	PermissionEnforcementBestEffort  PermissionEnforcement = "best_effort"
	PermissionEnforcementUnsupported PermissionEnforcement = "unsupported"
)

// PermissionPathRule is reserved for path-level policy. The first OpenCode
// subprocess implementation classifies these before launch instead of silently
// pretending native config can enforce them.
type PermissionPathRule struct {
	Path   string
	Action PermissionAction
}

// PermissionPolicy is caller intent for runtime permissions at run
// initialization. It is intentionally runtime-neutral.
type PermissionPolicy struct {
	Default             PermissionAction
	Tools               map[PermissionTool]PermissionAction
	PathRules           []PermissionPathRule
	UnsupportedBehavior PermissionUnsupportedBehavior
	Metadata            map[string]string
}

// PermissionFeatureSupport records adapter support for one policy feature.
type PermissionFeatureSupport struct {
	Feature     string
	Enforcement PermissionEnforcement
	Reason      string
}

// PermissionAudit records a safe permission policy or decision fact.
type PermissionAudit struct {
	Source      string
	Tool        PermissionTool
	Action      PermissionAction
	Enforcement PermissionEnforcement
	Reason      string
	Metadata    map[string]string
}

// PermissionMetadata summarizes the effective permission posture for a run.
type PermissionMetadata struct {
	Mode        PermissionMode
	Policy      PermissionPolicySummary
	PolicyID    string
	Support     []PermissionFeatureSupport
	Audit       []PermissionAudit
	Unsupported []PermissionFeatureSupport
}

// PermissionPolicySummary is safe to expose in run metadata.
type PermissionPolicySummary struct {
	ID                  string
	Default             PermissionAction
	Tools               map[PermissionTool]PermissionAction
	PathRuleCount       int
	UnsupportedBehavior PermissionUnsupportedBehavior
	Metadata            map[string]string
}

// Summary returns a defensive, metadata-safe policy summary.
func (p PermissionPolicy) Summary() PermissionPolicySummary {
	summary := PermissionPolicySummary{
		Default:             p.Default,
		Tools:               copyPermissionToolMap(p.Tools),
		PathRuleCount:       len(p.PathRules),
		UnsupportedBehavior: p.UnsupportedBehavior,
		Metadata:            RedactStringMap(p.Metadata),
	}
	summary.ID = summary.StableID()
	return summary
}

// StableID returns a deterministic, safe identifier for this policy summary.
func (s PermissionPolicySummary) StableID() string {
	copy := s
	copy.ID = ""
	encoded, err := json.Marshal(copy)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return "perm_" + hex.EncodeToString(sum[:8])
}

// ValidatePermissionPolicy rejects invalid SDK-level permission policy values.
func ValidatePermissionPolicy(policy *PermissionPolicy) *SDKError {
	if policy == nil {
		return nil
	}
	if err := validatePermissionAction(policy.Default, "default permission action"); err != nil {
		return err
	}
	for tool, action := range policy.Tools {
		if tool == "" {
			return permissionConfigError("permission tool cannot be empty")
		}
		if err := validatePermissionAction(action, fmt.Sprintf("permission action for %s", tool)); err != nil {
			return err
		}
	}
	for _, rule := range policy.PathRules {
		if rule.Path == "" {
			return permissionConfigError("permission path rule cannot have an empty path")
		}
		if err := validatePermissionAction(rule.Action, fmt.Sprintf("permission path rule for %s", rule.Path)); err != nil {
			return err
		}
	}
	switch policy.UnsupportedBehavior {
	case PermissionUnsupportedFail, PermissionUnsupportedBestEffort:
		return nil
	default:
		return permissionConfigError(fmt.Sprintf("unsupported permission fallback behavior %q", policy.UnsupportedBehavior))
	}
}

func validatePermissionAction(action PermissionAction, field string) *SDKError {
	switch action {
	case PermissionActionDefault, PermissionActionAllow, PermissionActionDeny, PermissionActionAsk:
		return nil
	default:
		return permissionConfigError(fmt.Sprintf("%s must be allow, deny, or ask", field))
	}
}

func permissionConfigError(detail string) *SDKError {
	return NewError(ErrorConfiguration, "permission policy", detail, nil)
}

func copyPermissionToolMap(src map[PermissionTool]PermissionAction) map[PermissionTool]PermissionAction {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[PermissionTool]PermissionAction, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
