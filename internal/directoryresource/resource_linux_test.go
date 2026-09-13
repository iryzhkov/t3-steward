//go:build linux

package directoryresource

import (
	"os"
	"path/filepath"
	"testing"
)

func registration(path string) Registration {
	return Registration{WorkerID: "worker", ResourceID: "dataset", Revision: "revision-1", Path: path}
}

func TestPinnedIdentityAndReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "data")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	r := registration(path)
	f, id, err := Open(r)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	reopened, err := Reopen(id, r)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	// Ordinary data updates must not change directory identity.
	if err := os.WriteFile(filepath.Join(path, "new"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err = Reopen(id, r)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	if err := os.Rename(path, path+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if f2, err := Reopen(id, r); err == nil {
		f2.Close()
		t.Fatal("replacement accepted")
	}
	// An already-open descriptor still names the original object.
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.Stat(path + "-old")
	if err != nil || !os.SameFile(info, old) {
		t.Fatal("pin retargeted")
	}
	changed := r
	changed.Revision = "revision-2"
	if f2, err := Reopen(id, changed); err == nil {
		f2.Close()
		t.Fatal("catalog revision changed")
	}
}

func TestMissingAndSymlinkDirectoriesRefused(t *testing.T) {
	root := t.TempDir()
	if f, _, err := Open(registration(filepath.Join(root, "absent"))); err == nil {
		f.Close()
		t.Fatal("missing accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "absent")); !os.IsNotExist(err) {
		t.Fatal("created missing data")
	}
	real := filepath.Join(root, "real")
	if err := os.MkdirAll(filepath.Join(real, "child"), 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{alias, filepath.Join(alias, "child")} {
		if f, _, err := Open(registration(path)); err == nil {
			f.Close()
			t.Fatalf("symlink accepted: %s", path)
		}
	}
}

func TestAccessAndOverlappingRoots(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "child")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	f, parentID, err := Open(registration(root))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f2, childID, err := Open(registration(child))
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	reader, err := Bind(parentID, "")
	if err != nil || reader.Access != ReadOnly {
		t.Fatalf("default: %v", err)
	}
	if _, err := Bind(parentID, ReadWrite); err == nil {
		t.Fatal("unauthorized writer accepted")
	}
	parentID.Registration.Writable = true
	writer, err := Bind(parentID, ReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	childReader, err := Bind(childID, ReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	if Conflicts(reader, childReader) {
		t.Fatal("readers conflict")
	}
	if !Conflicts(writer, childReader) || !Conflicts(childReader, writer) {
		t.Fatal("overlap missed")
	}
	// Simulate the same parent inode exposed through a different bind-mount path.
	writer.Identity.Registration.Path = "/alias"
	if !Conflicts(writer, childReader) {
		t.Fatal("ancestor alias missed")
	}
	childReader.Identity.Registration.WorkerID = "another-worker"
	if Conflicts(writer, childReader) {
		t.Fatal("different hosts conflict")
	}
}
