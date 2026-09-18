package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

func captureStderr(t *testing.T, body func()) string {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = write
	defer func() { os.Stderr = stderr }()
	body()
	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	captured, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	return string(captured)
}

// The check protocol reads every exit other than 0 and 2 as "not yet", so a
// check that is broken in a way that exits 7 is registered and polls until its
// deadline. Registration cannot know the difference, so it says what it saw:
// the exit code and the first output line, in every mode, and a warning when
// the exit is not the conventional 1.
func TestRegistrationReportsAndWarnsOnAnUnconventionalFirstExit(t *testing.T) {
	ctx := context.Background()
	cfg, coordinator := taskWaitCLIFixture(t)

	var stdout string
	var registerErr error
	stderr := captureStderr(t, func() {
		stdout = captureStdout(t, func() {
			registerErr = cmdTaskWaitAdd(ctx, cfg, []string{
				"--task", "current", "--request-id", "exit-7", "--json", "--",
				"sh", "-c", "echo boom; echo more; exit 7",
			})
		})
	})
	if registerErr != nil {
		t.Fatalf("an exit 7 check was refused; the protocol treats it as not yet: %v", registerErr)
	}
	var registered struct {
		ID              string `json:"id"`
		FirstExit       int    `json:"firstExit"`
		FirstOutputLine string `json:"firstOutputLine"`
	}
	if err := json.Unmarshal([]byte(stdout), &registered); err != nil {
		t.Fatalf("--json output is not one JSON document: %v: %s", err, stdout)
	}
	if registered.ID == "" || registered.FirstExit != 7 || registered.FirstOutputLine != "boom" {
		t.Fatalf("the JSON document does not carry the first run: %+v", registered)
	}
	for _, want := range []string{"exited 7", "boom", "not yet", "may never succeed"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("the warning does not say %q: %q", want, stderr)
		}
	}
	waits, err := coordinator.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 {
		t.Fatalf("the wait was not registered: %v %v", waits, err)
	}

	// The conventional "not yet" exit is reported and warns about nothing.
	stderr = captureStderr(t, func() {
		stdout = captureStdout(t, func() {
			registerErr = cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "exit-1", "--", "false"})
		})
	})
	if registerErr != nil {
		t.Fatal(registerErr)
	}
	if strings.Contains(stderr, "may never succeed") {
		t.Fatalf("an exit 1 check was warned about: %q", stderr)
	}
	if !strings.Contains(stdout, "first check exited 1") {
		t.Fatalf("the human report does not name the first exit: %q", stdout)
	}
}

func TestFirstOutputLine(t *testing.T) {
	for input, want := range map[string]string{
		"":                       "",
		"\n\n  \n":               "",
		"boom\nmore\n":           "boom",
		"\n  spaced  \nsecond":   "spaced",
		strings.Repeat("x", 300): strings.Repeat("x", 200) + "...",
	} {
		if got := firstOutputLine(input); got != want {
			t.Fatalf("firstOutputLine(%q) = %q, want %q", input, got, want)
		}
	}
}
