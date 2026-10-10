package workerproto

import "testing"

func TestHostObservedCapability(t *testing.T) {
	for name, want := range map[string]bool{
		CapabilityCoordinatorClient:      true,
		CapabilityAskRelay:               true,
		CapabilityHuyangTrusted:          true,
		"git-push-t3-steward":            true,
		"git-push-a":                     true,
		"git-push-":                      false,
		"git-push--x":                    false,
		"git-push-X":                     false,
		"git":                            false,
		"huyang":                         false,
		PackageCapabilityCommitBundle:    false,
		CapabilityRepositoryRefResolve:   false,
		CapabilityCampaignSupervision:    false,
		"ask-relay":                      false,
		"coordinator-client-v1-extended": false,
	} {
		if got := HostObservedCapability(name); got != want {
			t.Errorf("HostObservedCapability(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestGitPushCapability(t *testing.T) {
	for project, want := range map[string]string{
		"t3-steward":        "git-push-t3-steward",
		"t3_steward-github": "git-push-t3_steward-github",
		"":                  "",
		"Upper":             "",
		"trailing-":         "",
		"two--hyphens":      "",
		"dotted.name":       "",
		"9lives":            "",
	} {
		got := GitPushCapability(project)
		if got != want {
			t.Errorf("GitPushCapability(%q) = %q, want %q", project, got, want)
		}
		if got != "" && !HostObservedCapability(got) {
			t.Errorf("GitPushCapability(%q) = %q is not a host capability", project, got)
		}
	}
}
