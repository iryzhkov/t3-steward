package main

import (
	"path/filepath"
	"strings"
	"testing"
)

var workerVerbs = []string{
	"serve", "bridge", "inspect-bootstrap", "inspect-journal", "enroll", "list",
	"contained-exec", "contained-child", "inspect-directory",
	"contained-t3", "contained-start", "contained-show", "contained-stop",
}

// F-12: "worker --help" printed a refusal that named four of thirteen verbs.
// Help must work before any bootstrap or configuration is read.
func TestCmdWorkerPrintsUsage(t *testing.T) {
	g := globalFlags{configPath: filepath.Join(t.TempDir(), "absent.yaml")}
	for _, args := range [][]string{nil, {"help"}, {"--help"}, {"-h"}} {
		output := captureStdout(t, func() {
			if err := cmdWorker(g, args); err != nil {
				t.Fatalf("worker %v: %v", args, err)
			}
		})
		if output != workerUsage {
			t.Fatalf("worker %v printed %q", args, output)
		}
	}
	for _, verb := range workerVerbs {
		if !strings.Contains(workerUsage, verb) {
			t.Errorf("worker usage does not list %q", verb)
		}
	}
}

func TestCmdWorkerRefusalNamesEveryVerb(t *testing.T) {
	g := globalFlags{configPath: filepath.Join(t.TempDir(), "absent.yaml")}
	err := cmdWorker(g, []string{"no-such-verb"})
	if err == nil {
		t.Fatal("worker accepted an unknown verb")
	}
	for _, verb := range workerVerbs {
		if !strings.Contains(err.Error(), verb) {
			t.Errorf("refusal %q does not name %q", err, verb)
		}
	}
}
