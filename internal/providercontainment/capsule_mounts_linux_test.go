//go:build linux

package providercontainment

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestKernelCapsuleMountsPreserveWorkspaceLinksAndReadOnlyCustody(t *testing.T) {
	if os.Getenv("T3_STEWARD_REQUIRE_CONTAINMENT_TESTS") != "1" {
		t.Skip("requires bubblewrap")
	}
	spec := fixture(t)
	root := filepath.Dir(spec.Workspace.Registration.Path)
	for _, name := range []string{"inputs", "dependencies"} {
		path := filepath.Join(root, name)
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "value"), []byte(name), 0400); err != nil {
			t.Fatal(err)
		}
		identity := inspected(t, path, name, false)
		if name == "inputs" {
			spec.Inputs = &identity
		} else {
			spec.Dependencies = &identity
		}
	}
	metadata := filepath.Join(spec.Workspace.Registration.Path, ".t3")
	if err := os.Mkdir(metadata, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"inputs", "dependencies"} {
		if err := os.Symlink("../../"+name, filepath.Join(metadata, name)); err != nil {
			t.Fatal(err)
		}
	}
	spec.Command = []string{"/bin/sh", "-ec", `test "$(cat .t3/inputs/value)" = inputs
test "$(cat .t3/dependencies/value)" = dependencies
if echo bad > .t3/inputs/value; then exit 91; fi
if echo bad > .t3/dependencies/new; then exit 92; fi
cat /data/0/source > result`}
	if err := Run(context.Background(), spec, Streams{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(spec.Workspace.Registration.Path, "result"))
	if err != nil || string(data) != "original" {
		t.Fatalf("output=%q err=%v", data, err)
	}
	// Replacing a bound input directory must fail before any command starts.
	path := spec.Inputs.Registration.Path
	if err := os.Rename(path, path+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), spec, Streams{}); err == nil {
		t.Fatal("replaced input accepted")
	}
}
