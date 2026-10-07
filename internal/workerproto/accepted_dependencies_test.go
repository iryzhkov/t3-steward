package workerproto

import "testing"

// An accepted commit lets the worker publish a staged commit, so the list and
// the capability that promises to honour it travel together, and only a
// producer of this run can carry it, naming its own artifacts once each.
func TestValidateExecutionPackageBindsAcceptedDependenciesToTheirCapability(t *testing.T) {
	accepted := validPackage()
	accepted.Dependencies[0].AcceptedCommits = []string{"findings.md"}
	accepted.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies}
	if err := ValidateExecutionPackage(accepted); err != nil {
		t.Fatalf("accepted dependency with its capability was refused: %v", err)
	}

	unmarked := validPackage()
	unmarked.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies}
	if err := ValidateExecutionPackage(unmarked); err == nil {
		t.Fatal("accepted dependencies capability without an accepted dependency was accepted")
	}

	undeclared := validPackage()
	undeclared.Dependencies[0].AcceptedCommits = []string{"findings.md"}
	if err := ValidateExecutionPackage(undeclared); err == nil {
		t.Fatal("accepted dependency without its capability was accepted")
	}

	carried := validPackage()
	carried.Dependencies[0].AcceptedCommits = []string{"findings.md"}
	carried.Dependencies[0].Provenance = &DependencyProvenance{RunID: "run-0", TaskID: "inspect", AttemptID: "attempt-0",
		SourceArtifacts: map[string]string{carried.Dependencies[0].Artifacts[0].ID: "source-artifact"}}
	carried.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies}
	if err := ValidateExecutionPackage(carried); err == nil {
		t.Fatal("a carried input from another run was marked accepted")
	}

	for name, commits := range map[string][]string{
		"not one of the artifacts": {"other.md"},
		"a path prefix":            {"inspect/findings.md"},
		"named twice":              {"findings.md", "findings.md"},
	} {
		pkg := validPackage()
		pkg.Dependencies[0].AcceptedCommits = commits
		pkg.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies}
		if err := ValidateExecutionPackage(pkg); err == nil {
			t.Fatalf("accepted commit %s was accepted: %v", name, commits)
		}
	}
}
