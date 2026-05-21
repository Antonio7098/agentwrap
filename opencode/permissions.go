package opencode

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Antonio7098/agentwrap"
)

const opencodeConfigContentEnv = "OPENCODE_CONFIG_CONTENT="

var opencodePermissionTools = map[agentwrap.PermissionTool]string{
	agentwrap.PermissionToolRead:              "read",
	agentwrap.PermissionToolEdit:              "edit",
	agentwrap.PermissionToolShell:             "bash",
	agentwrap.PermissionToolGlob:              "glob",
	agentwrap.PermissionToolSearch:            "grep",
	agentwrap.PermissionToolList:              "list",
	agentwrap.PermissionToolTask:              "task",
	agentwrap.PermissionToolTodo:              "todowrite",
	agentwrap.PermissionToolQuestion:          "question",
	agentwrap.PermissionToolWebFetch:          "webfetch",
	agentwrap.PermissionToolWebSearch:         "websearch",
	agentwrap.PermissionToolRepoClone:         "repo_clone",
	agentwrap.PermissionToolRepoOverview:      "repo_overview",
	agentwrap.PermissionToolExternalDirectory: "external_directory",
	agentwrap.PermissionToolDoomLoop:          "doom_loop",
	agentwrap.PermissionToolLanguageServer:    "lsp",
	agentwrap.PermissionToolSkill:             "skill",
}

type permissionTranslation struct {
	config   map[string]string
	metadata agentwrap.PermissionMetadata
}

func translatePermissions(req agentwrap.RunRequest) (permissionTranslation, error) {
	metadata := agentwrap.PermissionMetadata{Mode: req.Permissions}
	if req.PermissionPolicy == nil {
		return permissionTranslation{metadata: metadata}, nil
	}
	if err := agentwrap.ValidatePermissionPolicy(req.PermissionPolicy); err != nil {
		return permissionTranslation{}, err
	}
	metadata.Policy = req.PermissionPolicy.Summary()
	metadata.PolicyID = metadata.Policy.ID
	permissionConfig := map[string]string{}
	if req.PermissionPolicy.Default != agentwrap.PermissionActionDefault {
		for _, nativeTool := range opencodePermissionTools {
			permissionConfig[nativeTool] = string(req.PermissionPolicy.Default)
		}
		metadata.Support = append(metadata.Support, agentwrap.PermissionFeatureSupport{
			Feature:     "default",
			Enforcement: agentwrap.PermissionEnforcementNative,
			Reason:      "expanded to OpenCode native tool permissions",
		})
	}
	for tool, action := range req.PermissionPolicy.Tools {
		nativeTool, ok := opencodePermissionTools[tool]
		if !ok {
			unsupported := unsupportedFeature(string(tool), req.PermissionPolicy.UnsupportedBehavior, "OpenCode subprocess permission config has no native tool mapping")
			metadata.Support = append(metadata.Support, unsupported)
			metadata.Unsupported = append(metadata.Unsupported, unsupported)
			if req.PermissionPolicy.UnsupportedBehavior != agentwrap.PermissionUnsupportedBestEffort {
				return permissionTranslation{}, unsupportedPermissionError(unsupported)
			}
			continue
		}
		if action == agentwrap.PermissionActionDefault {
			continue
		}
		permissionConfig[nativeTool] = string(action)
		metadata.Support = append(metadata.Support, agentwrap.PermissionFeatureSupport{
			Feature:     string(tool),
			Enforcement: agentwrap.PermissionEnforcementNative,
			Reason:      fmt.Sprintf("mapped to OpenCode permission %q", nativeTool),
		})
		metadata.Audit = append(metadata.Audit, agentwrap.PermissionAudit{
			Source:      "opencode.config",
			Tool:        tool,
			Action:      action,
			Enforcement: agentwrap.PermissionEnforcementNative,
			Reason:      "initialized from SDK permission policy",
		})
	}
	for _, rule := range req.PermissionPolicy.PathRules {
		unsupported := unsupportedFeature("path:"+rule.Path, req.PermissionPolicy.UnsupportedBehavior, "OpenCode static permission config cannot enforce SDK path-level rules in subprocess mode")
		metadata.Support = append(metadata.Support, unsupported)
		metadata.Unsupported = append(metadata.Unsupported, unsupported)
		if req.PermissionPolicy.UnsupportedBehavior != agentwrap.PermissionUnsupportedBestEffort {
			return permissionTranslation{}, unsupportedPermissionError(unsupported)
		}
	}
	if len(permissionConfig) == 0 {
		return permissionTranslation{metadata: metadata}, nil
	}
	return permissionTranslation{config: permissionConfig, metadata: metadata}, nil
}

func unsupportedFeature(feature string, behavior agentwrap.PermissionUnsupportedBehavior, reason string) agentwrap.PermissionFeatureSupport {
	enforcement := agentwrap.PermissionEnforcementUnsupported
	if behavior == agentwrap.PermissionUnsupportedBestEffort {
		enforcement = agentwrap.PermissionEnforcementBestEffort
	}
	return agentwrap.PermissionFeatureSupport{Feature: feature, Enforcement: enforcement, Reason: reason}
}

func unsupportedPermissionError(feature agentwrap.PermissionFeatureSupport) *agentwrap.SDKError {
	return agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode permissions", feature.Reason, nil, agentwrap.WithDebugDetail(feature.Feature))
}

func mergeEnv(base []string, permissionConfig map[string]string) ([]string, error) {
	if len(permissionConfig) == 0 {
		return append([]string(nil), base...), nil
	}
	out := make([]string, 0, len(base)+1)
	existing := map[string]any{}
	for _, value := range base {
		if strings.HasPrefix(value, opencodeConfigContentEnv) {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(value, opencodeConfigContentEnv)), &existing); err != nil {
				return nil, agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode permissions", "existing OPENCODE_CONFIG_CONTENT is not valid JSON", err)
			}
			continue
		}
		out = append(out, value)
	}
	permissionAny, ok := existing["permission"]
	permission := map[string]any{}
	if ok {
		var castOK bool
		permission, castOK = permissionAny.(map[string]any)
		if !castOK {
			return nil, agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode permissions", "existing OPENCODE_CONFIG_CONTENT permission value must be an object", nil)
		}
	}
	for key, value := range permissionConfig {
		permission[key] = value
	}
	existing["permission"] = permission
	content, err := json.Marshal(existing)
	if err != nil {
		return nil, agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode permissions", "OpenCode permission config could not be encoded", err)
	}
	return append(out, opencodeConfigContentEnv+string(content)), nil
}
