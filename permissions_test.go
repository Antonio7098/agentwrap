package agentwrap

import "testing"

func TestValidatePermissionPolicyRejectsInvalidAction(t *testing.T) {
	err := ValidatePermissionPolicy(&PermissionPolicy{
		Tools: map[PermissionTool]PermissionAction{
			PermissionToolShell: PermissionAction("maybe"),
		},
	})
	if err == nil || err.Category != ErrorConfiguration {
		t.Fatalf("ValidatePermissionPolicy error = %#v, want configuration error", err)
	}
}

func TestPermissionPolicySummaryRedactsMetadata(t *testing.T) {
	summary := PermissionPolicy{
		Default: PermissionActionDeny,
		Tools: map[PermissionTool]PermissionAction{
			PermissionToolRead: PermissionActionAllow,
		},
		PathRules: []PermissionPathRule{{Path: "/tmp/x", Action: PermissionActionDeny}},
		Metadata:  map[string]string{"api_key": "secret", "owner": "team"},
	}.Summary()
	if summary.PathRuleCount != 1 || summary.Tools[PermissionToolRead] != PermissionActionAllow {
		t.Fatalf("summary = %#v", summary)
	}
	if summary.Metadata["api_key"] == "secret" {
		t.Fatalf("secret metadata was not redacted: %#v", summary.Metadata)
	}
}
