package workerproto

import (
	"slices"
	"testing"
)

// Commit output marks tell the worker which dependency files are commit
// references, so they travel only with the capability that promises them,
// name the dependency's own artifacts once each, and cover every accepted
// commit.
func TestValidateExecutionPackageBindsCommitOutputsToTheirCapability(t *testing.T) {
	marked := validPackage()
	marked.Dependencies[0].CommitOutputs = []string{"findings.md"}
	marked.RequiredCapabilities = []string{PackageCapabilityCommitOutputs}
	if err := ValidateExecutionPackage(marked); err != nil {
		t.Fatalf("marked dependency with its capability was refused: %v", err)
	}

	none := validPackage()
	none.RequiredCapabilities = []string{PackageCapabilityCommitOutputs}
	if err := ValidateExecutionPackage(none); err != nil {
		t.Fatalf("a package marking no commit output was refused: %v", err)
	}

	undeclared := validPackage()
	undeclared.Dependencies[0].CommitOutputs = []string{"findings.md"}
	if err := ValidateExecutionPackage(undeclared); err == nil {
		t.Fatal("commit outputs without their capability were accepted")
	}

	for name, commits := range map[string][]string{
		"not one of the artifacts": {"other.md"},
		"a path prefix":            {"inspect/findings.md"},
		"named twice":              {"findings.md", "findings.md"},
	} {
		pkg := validPackage()
		pkg.Dependencies[0].CommitOutputs = commits
		pkg.RequiredCapabilities = []string{PackageCapabilityCommitOutputs}
		if err := ValidateExecutionPackage(pkg); err == nil {
			t.Fatalf("commit output %s was accepted: %v", name, commits)
		}
	}

	unmarkedAccepted := validPackage()
	unmarkedAccepted.Dependencies[0].AcceptedCommits = []string{"findings.md"}
	unmarkedAccepted.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies, PackageCapabilityCommitOutputs}
	if err := ValidateExecutionPackage(unmarkedAccepted); err == nil {
		t.Fatal("an accepted commit that is not a marked commit output was accepted")
	}
	unmarkedAccepted.Dependencies[0].CommitOutputs = []string{"findings.md"}
	if err := ValidateExecutionPackage(unmarkedAccepted); err != nil {
		t.Fatalf("a marked accepted commit was refused: %v", err)
	}
}

// A worker resolves records only from the marked files of a package that
// marks them, and from any file of a package from a coordinator that does not.
func TestDependencyCommitOutputsFollowTheMarks(t *testing.T) {
	pkg := validPackage()
	dependency := pkg.Dependencies[0]
	if got := pkg.DependencyCommitOutputs(dependency); !slices.Equal(got, []string{"findings.md"}) {
		t.Fatalf("unmarked package candidates = %v", got)
	}
	pkg.RequiredCapabilities = []string{PackageCapabilityCommitOutputs}
	if got := pkg.DependencyCommitOutputs(dependency); len(got) != 0 {
		t.Fatalf("marked package without marks resolves %v", got)
	}
	dependency.CommitOutputs = []string{"findings.md"}
	if got := pkg.DependencyCommitOutputs(dependency); !slices.Equal(got, []string{"findings.md"}) {
		t.Fatalf("marked package candidates = %v", got)
	}
}
