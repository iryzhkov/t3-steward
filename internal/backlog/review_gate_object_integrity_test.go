package backlog

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// forgeSubtree commits pkg/main.go in the repository at dir, then stages an
// unreviewed edit to it and overwrites the loose object of HEAD's pkg tree
// with a tree that names the edited blob, keeping the reviewed tree's name.
// HEAD still names the reviewed commit, and its index and worktree agree with
// what the forged tree says. It returns the new HEAD.
func forgeSubtree(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "pkg"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pkg", "main.go"), []byte("good\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "add", "-A")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "pkg")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	reviewed := gitOutput(t, dir, "rev-parse", "HEAD:pkg")
	if err := os.WriteFile(filepath.Join(dir, "pkg", "main.go"), []byte("evil\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "add", "-A")
	mktree := exec.Command("git", "-C", dir, "mktree")
	mktree.Stdin = bytes.NewBufferString("100644 blob " + gitOutput(t, dir, "rev-parse", ":pkg/main.go") + "\tmain.go\n")
	out, err := mktree.Output()
	if err != nil {
		t.Fatal(err)
	}
	forged := string(bytes.TrimSpace(out))
	raw, err := exec.Command("git", "-C", dir, "cat-file", "tree", forged).Output()
	if err != nil {
		t.Fatal(err)
	}
	var loose bytes.Buffer
	writer := zlib.NewWriter(&loose)
	fmt.Fprintf(writer, "tree %d\x00", len(raw))
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	objects := gitOutput(t, dir, "rev-parse", "--path-format=absolute", "--git-path", "objects")
	object := filepath.Join(objects, reviewed[:2], reviewed[2:])
	if err := os.Chmod(object, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(object, loose.Bytes(), 0o444); err != nil {
		t.Fatal(err)
	}
	if shown := gitOutput(t, dir, "show", "HEAD:pkg/main.go"); shown != "evil" {
		t.Fatalf("fixture: the forged tree is not read: %q", shown)
	}
	return head
}

// The workspace's object store is the executor's, and Git does not check the
// name of a subtree it reads. A tree object rewritten under the reviewed
// tree's name must not make an unreviewed edit equal to HEAD.
func TestCaptureWorkspaceHeadRefusesAForgedSubtree(t *testing.T) {
	dir, _ := workspaceHeadRepository(t)
	head := forgeSubtree(t, dir)
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	if captured.Error == "" && !captured.Dirty {
		t.Fatalf("forged subtree hid an edit at %s: %+v", head, captured)
	}
}

// The same holds for a submodule's object store.
func TestCaptureWorkspaceHeadRefusesAForgedSubtreeInASubmodule(t *testing.T) {
	library, _ := workspaceHeadRepository(t)
	dir, _ := workspaceHeadRepository(t)
	gitRun(t, dir, "-c", "protocol.file.allow=always", "submodule", "add", "-q", library, "lib")
	submodule := filepath.Join(dir, "lib")
	forgeSubtree(t, submodule)
	gitRun(t, dir, "add", "lib")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "submodule")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	if captured.Error == "" && !captured.Dirty {
		t.Fatalf("forged subtree in a submodule hid an edit at %s: %+v", head, captured)
	}
}

// Attributes come from the reviewed tree, but ident would let any text
// between "$Id:" and "$" compare equal to the committed "$Id$".
func TestCaptureWorkspaceHeadDoesNotCollapseIdent(t *testing.T) {
	dir, _ := workspaceHeadRepository(t)
	if err := os.WriteFile(filepath.Join(dir, ".gitattributes"), []byte("main.go ident\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("X=$Id$\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "add", "-A")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "ident")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("X=$Id: ; unreviewed ; #$\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "main.go")
}
