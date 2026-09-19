package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Test 3 of the stage. "task result" leaves the tree alone.
//
// It used to write ./.t3/results/<run>/<task>/ into the working tree, nothing
// ignored it, and every later "task run" from that checkout warned that the
// tree had uncommitted changes -- true, useless, and about the tool's own
// output. The regression is stated the way it was observed: git status in a
// real repository, byte for byte, before and after.
func TestTaskResultLeavesTheCheckoutByteIdentical(t *testing.T) {
	repository := newGitFixture(t)
	f := newTaskResultFixture(t)
	cli := f.cli()
	cli.workdir = repository

	before := gitStatus(t, repository)
	if err := cli.run(t.Context(), []string{"run-1"}); err != nil {
		t.Fatal(err)
	}
	after := gitStatus(t, repository)
	if before != after {
		t.Fatalf("collecting a result changed the checkout.\nbefore: %q\nafter:  %q", before, after)
	}

	// The files exist, outside the tree, and the command said where.
	if got := readFile(t, f.resultsDir(), "run-1", "task", "final-message.md"); got != "the job" {
		t.Fatalf("final message = %q", got)
	}
	absolute, err := filepath.Abs(filepath.Join(f.resultsDir(), "run-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.stdout.String(), absolute) {
		t.Fatalf("the command did not print the absolute path it wrote:\n%s", f.stdout.String())
	}
}

// --output . is the escape hatch for anyone who wants the old location, and it
// is taken against the working directory rather than against the state one.
func TestTaskResultOutputDotWritesIntoTheWorkingDirectory(t *testing.T) {
	repository := newGitFixture(t)
	f := newTaskResultFixture(t)
	cli := f.cli()
	cli.workdir = repository
	if err := cli.run(t.Context(), []string{"run-1", "--output", "."}); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, repository, "run-1", "task", "final-message.md"); got != "the job" {
		t.Fatalf("final message = %q", got)
	}
}

// An older checkout still holding a .t3 directory from the previous default
// stops producing the dirty-tree warning, because the only thing making that
// tree dirty is output this tool wrote.
func TestTheDirtyTreeWarningIgnoresThisToolsOwnOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		status string
		want   bool
	}{
		{name: "a clean tree", status: "", want: false},
		{name: "only the tool's own output", status: "?? .t3/", want: false},
		{name: "a collected result", status: "?? .t3/results/run-1/task/final-message.md", want: false},
		{name: "real work", status: " M internal/wait/node.go", want: true},
		{name: "real work beside the output", status: "?? .t3/\n M internal/wait/node.go", want: true},
		{name: "a file that merely starts the same way", status: "?? .t3rc", want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := hasSendableChanges(test.status); got != test.want {
				t.Fatalf("hasSendableChanges(%q) = %t, want %t", test.status, got, test.want)
			}
		})
	}
}

// newGitFixture is a real repository with one commit, so that git status has
// something to be silent about.
func newGitFixture(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not on PATH")
	}
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "README.md"), []byte("fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"init", "--quiet"},
		{"config", "user.email", "fixture@example.invalid"},
		{"config", "user.name", "Fixture"},
		{"add", "README.md"},
		{"commit", "--quiet", "-m", "first"},
	} {
		command := exec.Command("git", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		if out, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	return directory
}

func gitStatus(t *testing.T, directory string) string {
	t.Helper()
	var out bytes.Buffer
	command := exec.Command("git", "status", "--porcelain")
	command.Dir = directory
	command.Stdout = &out
	if err := command.Run(); err != nil {
		t.Fatal(err)
	}
	return out.String()
}
