package workerproto

import (
	"fmt"
	"testing"
	"time"
)

// Fix round 2 self-review: validating commit output marks scanned every
// artifact for every mark, so a package with tens of thousands of them took
// seconds to validate on every check. It is linear now.
func TestCommitOutputValidationIsLinear(t *testing.T) {
	const n = 20000
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
	pkg.RequiredCapabilities = []string{PackageCapabilityCommitOutputs}
	start := time.Now()
	if err := validateCommitOutputs(pkg.Dependencies[0]); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("validating %d commit outputs took %v", n, elapsed)
	}
}
