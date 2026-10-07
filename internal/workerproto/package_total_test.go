package workerproto

import (
	"math"
	"testing"
)

// packageWithExtraInput is validPackage plus one more input of the given kind
// and size. validPackage already carries a prompt, one static input and one
// dependency input, so every kind is added on top of a non-zero total.
func packageWithExtraInput(kind string, size int64) ExecutionPackage {
	pkg := validPackage()
	switch kind {
	case "static":
		extra := artifactObject("artifact-extra", "inputs/extra.md", []byte("x"))
		extra.Size = size
		pkg.StaticInputs = append(pkg.StaticInputs, extra)
	case "dependency":
		extra := artifactObject("artifact-extra", "dependencies/inspect/extra.md", []byte("x"))
		extra.Size = size
		pkg.Dependencies[0].Artifacts = append(pkg.Dependencies[0].Artifacts, extra)
	case "bundle":
		extra := artifactObject("artifact-extra", CommitBundlePath("run-0", "task-producer", "repair"), []byte("x"))
		extra.Size = size
		pkg.CommitBundles = []CommitBundleInput{{WorkflowRunID: "run-0", TaskID: "task-producer", Name: "repair", Bundle: &extra}}
		pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, PackageCapabilityCommitBundle)
	}
	return pkg
}

func packageInputTotal(pkg ExecutionPackage) int64 {
	total := pkg.Prompt.Size
	for _, input := range pkg.StaticInputs {
		total += input.Size
	}
	for _, dependency := range pkg.Dependencies {
		for _, input := range dependency.Artifacts {
			total += input.Size
		}
	}
	return total
}

func TestExecutionPackageInputTotalBoundary(t *testing.T) {
	for _, kind := range []string{"static", "dependency", "bundle"} {
		t.Run(kind, func(t *testing.T) {
			base := packageWithExtraInput(kind, 0)
			if err := ValidateExecutionPackage(base); err != nil {
				t.Fatalf("fixture with an empty extra input: %v", err)
			}
			remaining := base.Limits.MaxTotalBytes - packageInputTotal(base)
			if remaining > base.Limits.MaxArtifactBytes {
				remaining = base.Limits.MaxArtifactBytes
				base.Limits.MaxTotalBytes = packageInputTotal(base) + remaining
			}
			at := packageWithExtraInput(kind, remaining)
			at.Limits = base.Limits
			if err := ValidateExecutionPackage(at); err != nil {
				t.Fatalf("inputs exactly at the total limit refused: %v", err)
			}
			over := packageWithExtraInput(kind, remaining)
			over.Limits = base.Limits
			over.Limits.MaxTotalBytes--
			if err := ValidateExecutionPackage(over); err == nil {
				t.Fatal("inputs one byte over the total limit accepted")
			}
		})
	}
}

// A total that wraps past MaxInt64 turns negative and would pass a check made
// only after every addition.
func TestExecutionPackageRefusesOverflowingInputTotal(t *testing.T) {
	for _, kind := range []string{"static", "dependency", "bundle"} {
		t.Run(kind, func(t *testing.T) {
			pkg := packageWithExtraInput(kind, 1)
			pkg.Limits.MaxArtifactBytes = math.MaxInt64
			pkg.Limits.MaxTotalBytes = math.MaxInt64
			pkg.Prompt.Size = math.MaxInt64
			// Leave only the extra input of this kind next to the prompt.
			if kind != "static" {
				pkg.StaticInputs = nil
			} else {
				pkg.StaticInputs = pkg.StaticInputs[1:]
			}
			if kind != "dependency" {
				pkg.Dependencies = nil
			} else {
				pkg.Dependencies[0].Artifacts = pkg.Dependencies[0].Artifacts[1:]
			}
			if err := ValidateExecutionPackage(pkg); err == nil {
				t.Fatalf("accepted a MaxInt64-byte prompt plus a one-byte %s input", kind)
			}
		})
	}
	// The audit's original reproduction: a large addition wraps the running
	// total from below rather than from the prompt.
	pkg := validPackage()
	pkg.Limits.MaxArtifactBytes = math.MaxInt64
	pkg.Limits.MaxTotalBytes = math.MaxInt64
	pkg.StaticInputs[0].Size = math.MaxInt64
	if err := ValidateExecutionPackage(pkg); err == nil {
		t.Fatal("accepted a MaxInt64-byte static input next to a non-empty prompt")
	}
}
