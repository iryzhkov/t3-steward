package backlog

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The third review's reproduction, kept as written: a source file staged in a
// populated submodule's own index, with neither HEAD moved, is unreviewed
// tracked work, and the real coordinator import must fail the task for a
// dirty tree.
func TestReviewRound3StagedSubmoduleAddition(t *testing.T) {
	library, _ := workspaceHeadRepository(t)
	dir, _ := workspaceHeadRepository(t)
	gitRun(t, dir, "-c", "protocol.file.allow=always", "submodule", "add", "-q", library, "lib")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "submodule")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	f := newReviewGateFixture(t, true, false)
	f.openRound(t, "cp-1", head, "accept")
	sub := filepath.Join(dir, "lib")
	if err := os.WriteFile(filepath.Join(sub, "extra.go"), []byte("unreviewed source\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, sub, "add", "extra.go")
	if status := gitOutput(t, dir, "status", "--porcelain", "--ignore-submodules=none"); status == "" {
		t.Fatal("fixture not dirty")
	}
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	f.finishTurn(t)
	attempt := f.collect(t, f.store, &captured, "")
	if attempt.Progress != domain.ProgressFailed || attempt.ReviewGate == nil || attempt.ReviewGate.Code != domain.ReviewGateDirtyTree {
		t.Fatalf("staged submodule source succeeded: capture=%+v gate=%+v progress=%s", captured, attempt.ReviewGate, attempt.Progress)
	}
}

// nestedSubmoduleWorkspace returns a committed workspace whose submodule lib
// has a submodule of its own, inner, both populated, and the workspace's HEAD.
func nestedSubmoduleWorkspace(t *testing.T) (string, string) {
	t.Helper()
	inner, _ := workspaceHeadRepository(t)
	library, _ := workspaceHeadRepository(t)
	gitRun(t, library, "-c", "protocol.file.allow=always", "submodule", "add", "-q", inner, "inner")
	gitRun(t, library, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "inner")
	dir, _ := workspaceHeadRepository(t)
	gitRun(t, dir, "-c", "protocol.file.allow=always", "submodule", "add", "-q", library, "lib")
	gitRun(t, dir, "-c", "protocol.file.allow=always", "submodule", "update", "-q", "--init", "--recursive")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "submodule")
	return dir, gitOutput(t, dir, "rev-parse", "HEAD")
}

// The report is published as an artifact and its paths are capped, so the
// same workspace must give the same paths in the same order every time, staged
// deletions included.
func TestCaptureWorkspaceHeadReportsStagedDeletionsInAStableOrder(t *testing.T) {
	dir, _ := workspaceHeadRepository(t)
	for index := range 2 * domain.MaxWorkspaceHeadDirtyPaths {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%02d.txt", index)), []byte("x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gitRun(t, dir, "add", "-A")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "many")
	head := gitOutput(t, dir, "rev-parse", "HEAD")
	gitRun(t, dir, "rm", "-q", "--cached", "f*.txt")
	first := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	requireDirtyPath(t, first, head, "f00.txt")
	if !slices.IsSorted(first.DirtyPaths) {
		t.Fatalf("staged deletions out of order: %v", first.DirtyPaths)
	}
	for range 5 {
		if again := CaptureWorkspaceHead(context.Background(), "", dir, nil); !slices.Equal(again.DirtyPaths, first.DirtyPaths) {
			t.Fatalf("repeated capture differs:\n%v\n%v", first.DirtyPaths, again.DirtyPaths)
		}
	}
}

// gitWithInput runs Git in repository with input on its standard input.
func gitWithInput(t *testing.T, repository, input string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", repository}, args...)...)
	command.Stdin = strings.NewReader(input)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

// Every Git state a repository can be left in is judged the same way in the
// workspace, in a populated submodule and in a submodule of that submodule:
// any difference between the reviewed commit and either the repository's own
// index or its worktree is a change, while files that neither the commit nor
// the index tracks are not the reviewed work.
func TestCaptureWorkspaceHeadJudgesEveryRepositoryStateAtEveryLevel(t *testing.T) {
	write := func(t *testing.T, dir, name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	exclude := func(t *testing.T, dir, pattern string) {
		t.Helper()
		file := gitOutput(t, dir, "rev-parse", "--path-format=absolute", "--git-path", "info/exclude")
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(pattern+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(t *testing.T, dir string) {
		t.Helper()
		gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qam", "moved")
	}
	states := []struct {
		name   string
		change func(t *testing.T, dir string)
		// dirty is the changed path relative to the repository, or "" for
		// a state that leaves the reviewed work as it was.
		dirty string
		// nestedOnly marks a state that only a submodule can be in.
		nestedOnly bool
	}{
		{name: "clean", change: func(*testing.T, string) {}},
		{name: "worktree edit", dirty: "main.go", change: func(t *testing.T, dir string) {
			write(t, dir, "main.go", "two\n")
		}},
		{name: "worktree deletion", dirty: "main.go", change: func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, "main.go")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "worktree edit hidden by assume-unchanged", dirty: "main.go", change: func(t *testing.T, dir string) {
			write(t, dir, "main.go", "two\n")
			gitRun(t, dir, "update-index", "--assume-unchanged", "main.go")
		}},
		{name: "staged edit", dirty: "main.go", change: func(t *testing.T, dir string) {
			write(t, dir, "main.go", "two\n")
			gitRun(t, dir, "add", "main.go")
		}},
		{name: "staged edit restored in the worktree", dirty: "main.go", change: func(t *testing.T, dir string) {
			write(t, dir, "main.go", "two\n")
			gitRun(t, dir, "add", "main.go")
			write(t, dir, "main.go", "one\n")
		}},
		{name: "staged addition", dirty: "extra.go", change: func(t *testing.T, dir string) {
			write(t, dir, "extra.go", "unreviewed\n")
			gitRun(t, dir, "add", "extra.go")
		}},
		{name: "staged addition removed from the worktree", dirty: "extra.go", change: func(t *testing.T, dir string) {
			write(t, dir, "extra.go", "unreviewed\n")
			gitRun(t, dir, "add", "extra.go")
			if err := os.Remove(filepath.Join(dir, "extra.go")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "intent to add", dirty: "extra.go", change: func(t *testing.T, dir string) {
			write(t, dir, "extra.go", "unreviewed\n")
			gitRun(t, dir, "add", "-N", "extra.go")
		}},
		{name: "staged deletion kept in the worktree", dirty: "main.go", change: func(t *testing.T, dir string) {
			gitRun(t, dir, "rm", "-q", "--cached", "main.go")
		}},
		{name: "staged mode change", dirty: "main.go", change: func(t *testing.T, dir string) {
			gitRun(t, dir, "update-index", "--chmod=+x", "main.go")
		}},
		{name: "unmerged entry", dirty: "main.go", change: func(t *testing.T, dir string) {
			blob := gitOutput(t, dir, "rev-parse", "HEAD:main.go")
			zero := strings.Repeat("0", len(blob))
			gitWithInput(t, dir, "0 "+zero+"\tmain.go\n100644 "+blob+" 1\tmain.go\n100644 "+blob+" 2\tmain.go\n",
				"update-index", "--index-info")
		}},
		{name: "untracked file", change: func(t *testing.T, dir string) {
			write(t, dir, "extra.go", "scratch\n")
		}},
		{name: "ignored file", change: func(t *testing.T, dir string) {
			exclude(t, dir, "*.log")
			write(t, dir, "extra.log", "scratch\n")
		}},
		{name: "ignored file added by force", dirty: "extra.log", change: func(t *testing.T, dir string) {
			exclude(t, dir, "*.log")
			write(t, dir, "extra.log", "unreviewed\n")
			gitRun(t, dir, "add", "-f", "extra.log")
		}},
		{name: "submodule commit moved", nestedOnly: true, change: func(t *testing.T, dir string) {
			write(t, dir, "main.go", "two\n")
			commit(t, dir)
		}},
	}
	for _, level := range []string{"", "lib", "lib/inner"} {
		for _, state := range states {
			if state.nestedOnly && level == "" {
				continue
			}
			name := level
			if name == "" {
				name = "workspace"
			}
			t.Run(name+"/"+state.name, func(t *testing.T) {
				dir, head := nestedSubmoduleWorkspace(t)
				requireCleanCapture(t, CaptureWorkspaceHead(context.Background(), "", dir, nil), head)
				state.change(t, filepath.Join(dir, filepath.FromSlash(level)))
				captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
				switch {
				case state.nestedOnly:
					// The submodule's own commit is the change, reported
					// under the path that records it.
					requireDirtyPath(t, captured, head, level)
				case state.dirty != "":
					requireDirtyPath(t, captured, head, path.Join(level, state.dirty))
				default:
					requireCleanCapture(t, captured, head)
				}
			})
		}
	}
}
