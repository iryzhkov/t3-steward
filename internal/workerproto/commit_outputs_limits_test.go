package workerproto

import (
	"fmt"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// Fix round 2 self-review: validating commit output marks scanned every
// artifact for every mark, so a package with tens of thousands of them took
// seconds to validate on every check. It is linear now; the bound is on
// growth, not on wall-clock time, so a loaded host does not fail it.
func TestCommitOutputValidationIsLinear(t *testing.T) {
	if err := validateCommitOutputs(commitOutputsDependency(1000)); err != nil {
		t.Fatal(err)
	}
	if err := testtiming.CheckLinear(500, func(n int) func() {
		dependency := commitOutputsDependency(n)
		return func() {
			if err := validateCommitOutputs(dependency); err != nil {
				t.Error(err)
			}
		}
	}); err != nil {
		t.Fatalf("commit output validation is not linear: %v", err)
	}
}

// The growth bound above detects the regression it was written for: a
// validation that scans every artifact for every mark fails it.
func TestCommitOutputGrowthBoundRejectsAScanPerMark(t *testing.T) {
	quadratic := func(dependency DependencyInput) int {
		found := 0
		for _, name := range dependency.CommitOutputs {
			for _, artifact := range dependency.Artifacts {
				if strings.TrimPrefix(artifact.Path, "dependencies/inspect/") == name {
					found++
					break
				}
			}
		}
		return found
	}
	if err := testtiming.CheckLinear(200, func(n int) func() {
		dependency := commitOutputsDependency(n)
		return func() { quadratic(dependency) }
	}); err == nil {
		t.Fatal("a validation that scans every artifact for every mark passed the growth bound")
	}
}

// commitOutputsDependency is a valid dependency with n artifacts, each
// marked as a commit output.
func commitOutputsDependency(n int) DependencyInput {
	pkg := validPackage()
	artifacts := make([]ArtifactObject, 0, n)
	names := make([]string, 0, n)
	for index := range n {
		name := fmt.Sprintf("f%d.md", index)
		artifacts = append(artifacts, artifactObject(fmt.Sprintf("a%d", index), "dependencies/inspect/"+name, []byte("x")))
		names = append(names, name)
	}
	pkg.Dependencies[0].Artifacts = artifacts
	pkg.Dependencies[0].CommitOutputs = names
	return pkg.Dependencies[0]
}
