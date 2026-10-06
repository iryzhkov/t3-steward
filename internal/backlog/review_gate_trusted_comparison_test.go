package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The second review's reproduction, kept as written: after review accepts a
// head, executor-controlled Git configuration hides an unreviewed physical
// change from both comparisons, and the real coordinator import must still
// fail the task for a dirty tree.
func TestReviewProbeCoordinatorRejectsPhysicalBypasses(t *testing.T) {
	for _, kind := range []string{"symlink", "clean-filter"} {
		t.Run(kind, func(t *testing.T) {
			dir, head := workspaceHeadRepository(t)
			if kind == "symlink" {
				if err := os.Symlink("main.go", filepath.Join(dir, "link.go")); err != nil {
					t.Fatal(err)
				}
				gitRun(t, dir, "add", "link.go")
				gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "tracked symlink")
				head = gitOutput(t, dir, "rev-parse", "HEAD")
			}
			f := newReviewGateFixture(t, true, false)
			f.openRound(t, "cp-1", head, "accept")
			if kind == "symlink" {
				gitRun(t, dir, "config", "core.symlinks", "false")
				if err := os.Remove(filepath.Join(dir, "link.go")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "link.go"), []byte("main.go"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				gitRun(t, dir, "config", "filter.hide.clean", "printf 'one\\n'")
				if err := os.WriteFile(filepath.Join(dir, ".git/info/attributes"), []byte("main.go filter=hide\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("two\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
			f.finishTurn(t)
			attempt := f.collect(t, f.store, &captured, "")
			if attempt.Progress != domain.ProgressFailed || attempt.ReviewGate == nil || attempt.ReviewGate.Code != domain.ReviewGateDirtyTree {
				t.Fatalf("unreviewed physical change succeeded: capture=%+v progress=%s gate=%+v", captured, attempt.Progress, attempt.ReviewGate)
			}
		})
	}
}

func requireDirtyPath(t *testing.T, captured domain.WorkspaceHead, head, name string) {
	t.Helper()
	if captured.Error != "" || captured.Head != head || !captured.Dirty || !slices.Contains(captured.DirtyPaths, name) {
		t.Fatalf("change to %s hidden: %+v", name, captured)
	}
}

func requireCleanCapture(t *testing.T, captured domain.WorkspaceHead, head string) {
	t.Helper()
	if captured.Error != "" || captured.Head != head || captured.Dirty {
		t.Fatalf("clean workspace reported as %+v", captured)
	}
}

// A clean filter defined in the executor's global configuration, applied
// through the global attributes file, must not establish equality either.
func TestCaptureWorkspaceHeadIgnoresGlobalCleanFilters(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	home := t.TempDir()
	attributes := filepath.Join(home, "attributes")
	if err := os.WriteFile(attributes, []byte("main.go filter=hide\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, "gitconfig")
	if err := os.WriteFile(config, []byte("[core]\n\tattributesFile = "+attributes+"\n[filter \"hide\"]\n\tclean = printf 'one\\\\n'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", config)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if status := gitOutput(t, dir, "status", "--porcelain", "--untracked-files=no"); status != "" {
		t.Fatalf("fixture: the global filter does not hide the edit from git status: %q", status)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "main.go")
}

// An untracked .gitattributes is not part of the reviewed tree, so it cannot
// declare a converted file equal to HEAD's: here it would turn CRLF line
// endings back into the committed LF ones.
func TestCaptureWorkspaceHeadIgnoresUntrackedAttributes(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	if err := os.WriteFile(filepath.Join(dir, ".gitattributes"), []byte("main.go text eol=lf\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("one\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Adding the file converts it back to the committed blob and records the
	// new size, so the workspace's own index agrees that nothing changed.
	gitRun(t, dir, "add", "main.go")
	if status := gitOutput(t, dir, "status", "--porcelain", "--untracked-files=no"); status != "" {
		t.Fatalf("fixture: the attribute does not hide the edit from git status: %q", status)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "main.go")
}

// The attributes committed in the reviewed tree still apply, so a checkout
// they converted is clean, and so is a SHA-256 repository and a linked
// worktree whose objects live in the main repository.
func TestCaptureWorkspaceHeadTrustsTheReviewedTreesOwnAttributes(t *testing.T) {
	dir, _ := workspaceHeadRepository(t)
	if err := os.WriteFile(filepath.Join(dir, ".gitattributes"), []byte("*.txt text eol=crlf\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "add", ".gitattributes")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "attributes")
	if err := os.Remove(filepath.Join(dir, "answer.txt")); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "checkout", "--", "answer.txt")
	if raw, err := os.ReadFile(filepath.Join(dir, "answer.txt")); err != nil || string(raw) != "one\r\n" {
		t.Fatalf("fixture: checkout did not convert: %q %v", raw, err)
	}
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	requireCleanCapture(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head)

	linked := filepath.Join(t.TempDir(), "linked")
	gitRun(t, dir, "worktree", "add", "-q", "--detach", linked, head)
	requireCleanCapture(t, CaptureWorkspaceHead(context.Background(), "", linked, nil), head)
	if err := os.WriteFile(filepath.Join(linked, "main.go"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", linked, nil), head, "main.go")
}

// The scratch repository has no submodule configuration of the workspace, yet
// a submodule checked out at another commit, or edited, is still a change.
func TestCaptureWorkspaceHeadSeesAChangedSubmodule(t *testing.T) {
	library, _ := workspaceHeadRepository(t)
	dir, _ := workspaceHeadRepository(t)
	gitRun(t, dir, "-c", "protocol.file.allow=always", "submodule", "add", "-q", library, "lib")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "submodule")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	requireCleanCapture(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head)

	submodule := filepath.Join(dir, "lib")
	if err := os.WriteFile(filepath.Join(submodule, "main.go"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "lib/main.go")

	gitRun(t, submodule, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qam", "moved")
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "lib")
}

// The submodule's configuration is the executor's as well, so a clean filter
// there must not hide an edit inside the submodule.
func TestCaptureWorkspaceHeadIgnoresCleanFiltersInASubmodule(t *testing.T) {
	library, _ := workspaceHeadRepository(t)
	dir, _ := workspaceHeadRepository(t)
	gitRun(t, dir, "-c", "protocol.file.allow=always", "submodule", "add", "-q", library, "lib")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "submodule")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	submodule := filepath.Join(dir, "lib")
	gitRun(t, submodule, "config", "filter.hide.clean", "printf 'one\\n'")
	attributes := gitOutput(t, submodule, "rev-parse", "--path-format=absolute", "--git-path", "info/attributes")
	if err := os.MkdirAll(filepath.Dir(attributes), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(attributes, []byte("main.go filter=hide\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(submodule, "main.go"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, submodule, "add", "main.go")
	if status := gitOutput(t, dir, "status", "--porcelain", "--untracked-files=no", "--ignore-submodules=none"); status != "" {
		t.Fatalf("fixture: the submodule's filter does not hide the edit from git status: %q", status)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "lib/main.go")
}

// Git does not look inside a submodule directory without .git, so removing it
// would hide edits to the files left there. An empty directory, the state of
// a submodule that is not checked out, stays clean.
func TestCaptureWorkspaceHeadSeesEditsInADepopulatedSubmodule(t *testing.T) {
	library, _ := workspaceHeadRepository(t)
	dir, _ := workspaceHeadRepository(t)
	gitRun(t, dir, "-c", "protocol.file.allow=always", "submodule", "add", "-q", library, "lib")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "submodule")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	gitRun(t, dir, "submodule", "deinit", "-q", "lib")
	requireCleanCapture(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head)

	if err := os.WriteFile(filepath.Join(dir, "lib", "main.go"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "lib")
}

// The alternates file holds one path per line, so an object directory whose
// path has a line break cannot be borrowed; the capture says so rather than
// failing with Git's error about a malformed alternate.
func TestCaptureWorkspaceHeadExplainsAnObjectPathWithALineBreak(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a\nb")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "init", "-q")
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "add", "main.go")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "base")
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	if captured.Head != "" || captured.Dirty || !strings.Contains(captured.Error, "line break") {
		t.Fatalf("capture = %+v", captured)
	}
}

// plantMarkingFilter configures a clean filter for main.go in the repository
// at dir, through its own configuration and info/attributes, that records
// each run in the returned marker file and passes its input through.
func plantMarkingFilter(t *testing.T, dir string) string {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "ran")
	gitDir := gitOutput(t, dir, "rev-parse", "--absolute-git-dir")
	script := filepath.Join(gitDir, "mark.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho ran >> '"+marker+"'\ncat\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "config", "filter.mark.clean", script)
	if err := os.MkdirAll(filepath.Join(gitDir, "info"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "info", "attributes"), []byte("main.go filter=mark\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return marker
}

// A filter is code, and code the executor planted could put a file back for
// the length of the capture and redo the edit afterwards. The capture must
// therefore not run the workspace's filters at all, even for a file whose
// stat data says it changed.
func TestCaptureWorkspaceHeadRunsNoWorkspaceFilter(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	marker := plantMarkingFilter(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "main.go")
	if ran, err := os.ReadFile(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the workspace's filter ran during the capture: %q %v", ran, err)
	}
}

// The same holds for a submodule's filters, which Git would run when it
// compares the submodule's content inside the submodule.
func TestCaptureWorkspaceHeadRunsNoSubmoduleFilter(t *testing.T) {
	library, _ := workspaceHeadRepository(t)
	dir, _ := workspaceHeadRepository(t)
	gitRun(t, dir, "-c", "protocol.file.allow=always", "submodule", "add", "-q", library, "lib")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "submodule")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	submodule := filepath.Join(dir, "lib")
	marker := plantMarkingFilter(t, submodule)
	// A changed modification time makes Git compare the content.
	past := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(submodule, "main.go"), past, past); err != nil {
		t.Fatal(err)
	}
	requireCleanCapture(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head)
	if ran, err := os.ReadFile(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the submodule's filter ran during the capture: %q %v", ran, err)
	}
}

func TestCaptureWorkspaceHeadComparesASHA256Repository(t *testing.T) {
	dir := t.TempDir()
	gitRun(t, dir, "init", "-q", "--object-format=sha256")
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "add", "main.go")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "base")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	requireCleanCapture(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireDirtyPath(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head, "main.go")
}
