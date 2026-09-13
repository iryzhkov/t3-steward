package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestParseGraphAmendmentCLI(t *testing.T) {
	fence := []string{"--expected-revision", "7", "--request-id", "stable", "--reason", "add a check"}
	args := append([]string{"task", "add", "run/check", "--provider", "codex", "--model", "model", "--prompt", "check outputs", "--verify", "test -f result.txt", "--needs", "build,other/__sink", "--timeout", "30m"}, fence...)
	r, err := parseGraphAmendment(args)
	if err != nil {
		t.Fatal(err)
	}
	if r.Operation != "task-add" || r.RunID != "run" || r.Task.Name != "check" || r.Task.Timeout != 30*time.Minute || len(r.Task.ExternalNeeds) != 1 || len(r.Task.Needs) != 1 {
		t.Fatalf("request=%+v", r)
	}
	r, err = parseGraphAmendment(append([]string{"task", "set", "run/check", "--options", `{"effort":"low"}`}, fence...))
	if err != nil || r.Options == nil || (*r.Options)["effort"] != "low" {
		t.Fatal(r, err)
	}
	r, err = parseGraphAmendment(append([]string{"task", "set", "run/check", "--verify", "test -f result.txt", "--verify", "git diff --exit-code"}, fence...))
	if err != nil || r.Verification == nil || len(*r.Verification) != 2 || (*r.Verification)[0] != "test -f result.txt" {
		t.Fatal(r, err)
	}
	r, err = parseGraphAmendment(append([]string{"run", "clone", "--from", "run"}, fence...))
	if err != nil || r.Operation != "clone" {
		t.Fatal(r, err)
	}
	for _, args := range [][]string{
		append([]string{"task", "add", "run/check", "--provider", "codex", "--model", "model", "--prompt", "check"}, fence...),
		append([]string{"task", "set", "run/check", "--verify", " "}, fence...),
		append([]string{"task", "set", "run/check", "--verify", "bad\x00command"}, fence...),
		{"task", "set", "run/check", "--model", "new"},
		append([]string{"edge", "add", "run/check", "--from", "build", "--model", "bad"}, fence...),
		append([]string{"task", "set", "run/check", "--timeout", "-1m"}, fence...),
		append([]string{"task", "set", "run/check", "--options", "null"}, fence...),
	} {
		if _, err := parseGraphAmendment(args); err == nil {
			t.Fatalf("invalid args accepted: %v", args)
		}
	}
}

func TestGraphDOTEscapesIdentityAndReportsRevision(t *testing.T) {
	var out bytes.Buffer
	graph := backlogadmin.Graph{WorkflowRunID: "run", GraphRevision: 4, Nodes: []backlogadmin.GraphNode{{TaskID: "a\"b", Name: "quoted \"node\""}}, Edges: []backlogadmin.GraphEdge{{FromTaskID: "other/task", ToTaskID: "a\"b"}}}
	if err := renderGraphDOT(&out, graph); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "graph revision 4") || !strings.Contains(text, `"a\"b"`) || !strings.Contains(text, `"other/task" -> "a\"b"`) {
		t.Fatal(text)
	}
}
