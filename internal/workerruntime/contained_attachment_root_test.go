package workerruntime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/providercontainment"
)

// symlinkedStateParent returns a directory reached through a symlink, the way
// macOS reaches its temporary directory through /var -> /private/var.
func symlinkedStateParent(t *testing.T) (link, target string) {
	t.Helper()
	base := t.TempDir()
	target = filepath.Join(base, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link = filepath.Join(base, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	return link, target
}

func TestContainedAttachmentStorageAcceptsCanonicalisedSymlinkedParent(t *testing.T) {
	link, target := symlinkedStateParent(t)
	root, err := providercontainment.CanonicalRoot(filepath.Join(link, "contained"))
	if err != nil {
		t.Fatal(err)
	}
	resolvedReal, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	if root != filepath.Join(resolvedReal, "contained") {
		t.Fatalf("canonical root=%s, want below %s", root, resolvedReal)
	}
	if err = os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	path, err := (ContainedT3{Supervisor: providercontainment.Supervisor{Root: root}}).recordPath(testPackage())
	if err != nil {
		t.Fatalf("canonicalised storage behind a symlinked parent refused: %v", err)
	}
	if filepath.Dir(path) != root {
		t.Fatalf("record path %s escaped storage %s", path, root)
	}
	// The same storage named through the link is still not canonical.
	if _, err = (ContainedT3{Supervisor: providercontainment.Supervisor{Root: filepath.Join(link, "contained")}}).recordPath(testPackage()); err == nil ||
		!strings.Contains(err.Error(), "canonical and private") {
		t.Fatalf("non-canonical storage path accepted: %v", err)
	}
}

func TestContainedAttachmentStorageRefusesPlantedSymlinks(t *testing.T) {
	base, err := providercontainment.CanonicalRoot(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(base, 0700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "contained")
	if err = os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: root}}
	pkg := testPackage()
	path, err := manager.recordPath(pkg)
	if err != nil {
		t.Fatal(err)
	}

	// A record planted inside the storage as a symlink to a private,
	// well-formed file elsewhere is never read.
	decoy := filepath.Join(base, "decoy.json")
	if err = os.WriteFile(decoy, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(decoy, path); err != nil {
		t.Fatal(err)
	}
	if _, err = manager.load(pkg); err == nil || !strings.Contains(err.Error(), "invalid contained attachment storage") {
		t.Fatalf("symlinked attachment record accepted: %v", err)
	}

	// Storage replaced by a link to another private directory is refused even
	// though the configured root string is unchanged.
	elsewhere := filepath.Join(base, "elsewhere")
	if err = os.Mkdir(elsewhere, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(root, filepath.Join(base, "moved")); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(elsewhere, root); err != nil {
		t.Fatal(err)
	}
	if _, err = manager.recordPath(pkg); err == nil || !strings.Contains(err.Error(), "canonical and private") {
		t.Fatalf("symlinked storage accepted: %v", err)
	}

	// A link swapped in above the storage after configuration is refused too.
	if err = os.Remove(root); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(base, "nested")
	if err = os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(elsewhere, "contained"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(elsewhere, filepath.Join(nested, "parent")); err != nil {
		t.Fatal(err)
	}
	swapped := ContainedT3{Supervisor: providercontainment.Supervisor{Root: filepath.Join(nested, "parent", "contained")}}
	if _, err = swapped.recordPath(pkg); err == nil || !strings.Contains(err.Error(), "canonical and private") {
		t.Fatalf("storage below a planted parent link accepted: %v", err)
	}
}
