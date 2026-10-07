package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const noOutputsLine = "outputs none: only final-message.md is kept; declare files the task writes with --output FILE"

// --output is the spelling the request asked for: one file per flag,
// repeatable, and the same list --outputs builds with commas. A comma inside
// one --output value is part of the name, because the flag never splits.
func TestTaskRunOutputFlagIsRepeatableAndJoinsOutputs(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"repeated --output", []string{"--output", "a.md", "--output", "b.md"}, "a.md|b.md"},
		{"--outputs then --output", []string{"--outputs", "a.md", "--output", "b.md"}, "a.md|b.md"},
		{"--output then --outputs", []string{"--output", "a.md", "--outputs", "b.md,c.md"}, "a.md|b.md|c.md"},
		{"a comma is kept whole", []string{"--output", "a,b.md"}, "a,b.md"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTaskRunHarness()
			args := append([]string{"--model", "opus", "--json"}, tc.args...)
			if err := h.run(append(args, "--", "work")...); err != nil {
				t.Fatal(err)
			}
			manifest, _ := h.manifest(t)
			if got := strings.Join(manifest.Tasks["task"].Outputs, "|"); got != tc.want {
				t.Fatalf("manifest outputs = %q, want %q", got, tc.want)
			}
		})
	}
}

// The two spellings are one list, so they are one run: the same key and the
// same archive, and a repeat in the other spelling replays rather than
// conflicting.
func TestTaskRunOutputAndOutputsGiveTheSameKeyAndArchive(t *testing.T) {
	repeated := newTaskRunHarness()
	if err := repeated.run("--model", "opus", "--json", "--output", "a.md", "--output", "b.md", "--", "work"); err != nil {
		t.Fatal(err)
	}
	comma := newTaskRunHarness()
	if err := comma.run("--model", "opus", "--json", "--outputs", "a.md,b.md", "--", "work"); err != nil {
		t.Fatal(err)
	}
	if one, two := repeated.record(t).IdempotencyKey, comma.record(t).IdempotencyKey; one != two {
		t.Fatalf("keys differ: --output %q, --outputs %q", one, two)
	}
	if len(repeated.archives) != 1 || len(comma.archives) != 1 || !bytes.Equal(repeated.archives[0], comma.archives[0]) {
		t.Fatal("the two spellings submitted different archives")
	}
}

// A bad or repeated name is refused before anything is asked of the
// coordinator, in the campaign validator's own words under the flag's name.
// The prompt file does not exist, so a refusal that came after the prompt was
// read would name the prompt instead.
func TestTaskRunRefusesRepeatedOrUnsafeOutputBeforeContactingTheCoordinator(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--output", "a.md", "--output", "a.md"}, `--output path "a.md" is duplicated`},
		{[]string{"--outputs", "a.md,a.md"}, `--output path "a.md" is duplicated`},
		{[]string{"--outputs", "a.md", "--output", "a.md"}, `--output path "a.md" is duplicated`},
		{[]string{"--output", "../x"}, `--output path "../x": path escapes the workflow bundle`},
		{[]string{"--output", "/abs"}, `--output path "/abs": absolute paths are not allowed`},
		{[]string{"--output", "r*.md"}, `--output path "r*.md": glob characters are not allowed`},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			h := newTaskRunHarness()
			args := append([]string{"--model", "opus", "--prompt-file", filepath.Join(t.TempDir(), "absent.md")}, tc.args...)
			err := h.run(args...)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if len(h.queries) != 0 || len(h.viability) != 0 || len(h.requests) != 0 || len(h.archives) != 0 || len(h.notified) != 0 {
				t.Fatalf("a refused start reached the coordinator: queries=%v viability=%d requests=%d notified=%d",
					h.queries, len(h.viability), len(h.requests), len(h.notified))
			}
		})
	}
}

// --output with no value is the shared helper's refusal, not an empty name.
func TestTaskRunOutputNeedsAValue(t *testing.T) {
	for _, args := range [][]string{{"--output"}, {"--output", " ", "--", "x"}} {
		if _, err := parseTaskRunArgs(args); err == nil || err.Error() != "--output needs a value" {
			t.Fatalf("%q: error = %v", args, err)
		}
	}
}

// The dry run says what will be kept, and says so when nothing but the final
// message will be: findings were lost because nothing ever said that.
func TestTaskRunDryRunShowsDeclaredOutputs(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.run("--dry-run", "--model", "opus", "--output", "findings.md", "--output", "notes.md", "--", "Survey X"); err != nil {
		t.Fatal(err)
	}
	assertOutputsLineFollowsRoute(t, h.stdout.String(), "outputs findings.md, notes.md (final-message.md is always kept)")

	h = newTaskRunHarness()
	if err := h.run("--dry-run", "--model", "opus", "--", "Survey X"); err != nil {
		t.Fatal(err)
	}
	assertOutputsLineFollowsRoute(t, h.stdout.String(), noOutputsLine)

	h = newTaskRunHarness()
	if err := h.run("--dry-run", "--json", "--model", "opus", "--output", "findings.md", "--output", "notes.md", "--", "Survey X"); err != nil {
		t.Fatal(err)
	}
	if got := jsonOutputs(t, h.stdout.Bytes()); got == nil || strings.Join(*got, "|") != "findings.md|notes.md" {
		t.Fatalf("dry-run outputs = %v\n%s", got, h.stdout.String())
	}

	h = newTaskRunHarness()
	if err := h.run("--dry-run", "--json", "--model", "opus", "--", "Survey X"); err != nil {
		t.Fatal(err)
	}
	if got := jsonOutputs(t, h.stdout.Bytes()); got == nil || len(*got) != 0 {
		t.Fatalf("dry-run outputs = %v, want an empty array that is present\n%s", got, h.stdout.String())
	}
	if !strings.Contains(h.stdout.String(), `"outputs": []`) {
		t.Fatalf("dry run does not print \"outputs\": []\n%s", h.stdout.String())
	}
}

// The start record says the same thing in both forms, for one task and for a
// fan-out, where every task declares the same names.
func TestTaskRunRecordShowsDeclaredOutputs(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.run("--model", "opus", "--output", "findings.md", "--output", "notes.md", "--", "Survey X"); err != nil {
		t.Fatal(err)
	}
	assertOutputsLineFollowsRoute(t, h.stdout.String(), "outputs findings.md, notes.md (final-message.md is always kept)")

	h = newTaskRunHarness()
	if err := h.run("--model", "opus", "--", "Survey X"); err != nil {
		t.Fatal(err)
	}
	assertOutputsLineFollowsRoute(t, h.stdout.String(), noOutputsLine)

	h = newTaskRunHarness()
	if err := h.run("--model", "opus", "--json", "--output", "findings.md", "--", "Survey X"); err != nil {
		t.Fatal(err)
	}
	if got := jsonOutputs(t, h.stdout.Bytes()); got == nil || strings.Join(*got, "|") != "findings.md" {
		t.Fatalf("record outputs = %v\n%s", got, h.stdout.String())
	}

	h = newTaskRunHarness()
	if err := h.run("--model", "opus", "--json", "--", "Survey X"); err != nil {
		t.Fatal(err)
	}
	if got := jsonOutputs(t, h.stdout.Bytes()); got != nil {
		t.Fatalf("a record with no outputs carries %v", *got)
	}

	dir := t.TempDir()
	for name, body := range map[string]string{"alpha.md": "first", "beta.md": "second"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, asJSON := range []bool{true, false} {
		h = newTaskRunHarness()
		args := []string{"--model", "opus", "--fan-out", filepath.Join(dir, "*.md"), "--output", "findings.md"}
		if asJSON {
			args = append(args, "--json")
		}
		if err := h.run(args...); err != nil {
			t.Fatal(err)
		}
		if asJSON {
			record := h.record(t)
			if strings.Join(record.Tasks, ",") != "alpha,beta" || strings.Join(record.Outputs, "|") != "findings.md" {
				t.Fatalf("fan-out record = %+v", record)
			}
		} else {
			assertOutputsLineFollowsRoute(t, h.stdout.String(), "outputs findings.md (final-message.md is always kept)")
		}
		manifest, _ := h.manifest(t)
		for name, task := range manifest.Tasks {
			if strings.Join(task.Outputs, "|") != "findings.md" {
				t.Fatalf("fan-out task %s outputs = %v", name, task.Outputs)
			}
		}
	}
}

// The help names the new spelling and what it promises.
func TestTaskRunUsageDocumentsOutput(t *testing.T) {
	flat := strings.Join(strings.Fields(taskRunUsage), " ")
	for _, phrase := range []string{
		"--output FILE",
		"--outputs a.md,b.md",
		"final-message.md is always kept",
		"naming the files with --output",
		"t3-steward task result",
	} {
		if !strings.Contains(flat, phrase) {
			t.Errorf("task run usage does not say %q", phrase)
		}
	}
	if !strings.Contains(strings.Join(strings.Fields(taskResultUsage), " "), "not retained") {
		t.Error("task result usage does not mention declared outputs that were not retained")
	}
}

// assertOutputsLineFollowsRoute finds the outputs line and checks it is the
// line right after the route line.
func assertOutputsLineFollowsRoute(t *testing.T, text, want string) {
	t.Helper()
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if line != want {
			continue
		}
		if i == 0 || !strings.HasPrefix(lines[i-1], "route ") {
			t.Fatalf("the outputs line does not follow the route line:\n%s", text)
		}
		return
	}
	t.Fatalf("no line %q in:\n%s", want, text)
}

// jsonOutputs reads the outputs key from a printed document, nil when absent.
func jsonOutputs(t *testing.T, raw []byte) *[]string {
	t.Helper()
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("not one JSON document: %v\n%s", err, raw)
	}
	value, ok := document["outputs"]
	if !ok {
		return nil
	}
	var outputs []string
	if err := json.Unmarshal(value, &outputs); err != nil || outputs == nil && string(value) != "[]" {
		t.Fatalf("outputs is not an array: %s", value)
	}
	if outputs == nil {
		outputs = []string{}
	}
	return &outputs
}
