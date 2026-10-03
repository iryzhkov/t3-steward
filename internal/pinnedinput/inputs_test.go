package pinnedinput

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func canonicalTemp(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}
func TestSnapshotFreezesBytesAndNames(t *testing.T) {
	source := filepath.Join(canonicalTemp(t), "plan.md")
	if err := os.WriteFile(source, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := SnapshotFiles([]string{source})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("after"), 0600); err != nil {
		t.Fatal(err)
	}
	root := canonicalTemp(t)
	if err := snapshot.Write(root); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "plan.md"))
	if err != nil || string(got) != "before" {
		t.Fatalf("snapshot %q: %v", got, err)
	}
	entry := snapshot.Manifest.Entries[0]
	if entry.Name != "plan.md" || entry.Size != 6 || entry.SHA256 != fmt.Sprintf("%x", sha256.Sum256(got)) {
		t.Fatalf("%+v", entry)
	}
	again, err := NewManifest([]Entry{entry})
	if err != nil || again.Digest != snapshot.Manifest.Digest {
		t.Fatalf("digest %v %v", again, err)
	}
}
func TestSnapshotRefusesUnsafeAndOversizedSources(t *testing.T) {
	root := canonicalTemp(t)
	file := filepath.Join(root, "plan.md")
	if err := os.WriteFile(file, []byte("ok"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.md")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(root, "alias")
	if err := os.Symlink(root, parent); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(root, "big")
	if err := os.WriteFile(big, make([]byte, MaxFileBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	for _, files := range [][]string{{link}, {filepath.Join(parent, "plan.md")}, {root}, {big}, {file, file}, {filepath.Join(root, "..", "missing")}} {
		if _, err := SnapshotFiles(files); err == nil {
			t.Fatalf("accepted %v", files)
		}
	}
}
func TestManifestCanonicalOrderAndLimits(t *testing.T) {
	hash := fmt.Sprintf("%x", sha256.Sum256(nil))
	a := Entry{Name: "a", SHA256: hash}
	b := Entry{Name: "b", SHA256: hash}
	x, _ := NewManifest([]Entry{b, a})
	y, _ := NewManifest([]Entry{a, b})
	if x.Digest != y.Digest || x.Entries[0].Name != "a" {
		t.Fatal("noncanonical manifest")
	}
	for _, name := range []string{"/abs", "../up", "a/../b", "a\\b", "a*", "."} {
		if _, err := NewManifest([]Entry{{Name: name, SHA256: hash}}); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	if _, err := NewManifest([]Entry{{Name: "a", Size: MaxFileBytes + 1, SHA256: hash}}); err == nil {
		t.Fatal("accepted oversized input")
	}
}
