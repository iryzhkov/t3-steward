package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/compat"
)

// Decode the public JSON contract independently of the production document.
type dryRunTestDocument struct {
	Project          string       `json:"project"`
	Ref              string       `json:"ref"`
	Route            taskRunRoute `json:"route"`
	IdempotencyKey   string       `json:"idempotencyKey"`
	NotifyThread     string       `json:"notifyThread"`
	PromptCharacters int          `json:"promptCharacters"`
}

func TestTaskRunDryRunKeyAndPromptMatchSubmission(t *testing.T) {
	h := newTaskRunHarness()
	args := []string{"--model", "opus", "--outputs", "report.md", "--json", "--", "Write résumé 😀"}
	dryArgs := append([]string{"--dry-run"}, args...)
	if err := h.cli().run(context.Background(), dryArgs); err != nil {
		t.Fatal(err)
	}
	var dry dryRunTestDocument
	if err := json.Unmarshal(h.stdout.Bytes(), &dry); err != nil {
		t.Fatal(err)
	}
	h.stdout.Reset()
	if err := h.cli().run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	var actual taskRunRecord
	if err := json.Unmarshal(h.stdout.Bytes(), &actual); err != nil {
		t.Fatal(err)
	}
	manifest, files := h.manifest(t)
	expected := 0
	for _, task := range manifest.Tasks {
		expected += compat.TurnInputLength(backlog.FirstTurnPrompt(files[task.PromptFile], task.OutputDeclarations()))
	}
	if dry.PromptCharacters != expected {
		t.Fatalf("characters=%d want %d from submitted archive", dry.PromptCharacters, expected)
	}
	if len(h.notified) != 1 || dry.NotifyThread != h.notified[0].Request.ThreadID {
		t.Fatalf("dry notify=%q actual=%+v", dry.NotifyThread, h.notified)
	}
	if dry.IdempotencyKey != actual.IdempotencyKey || dry.Project != actual.Project || dry.Ref != actual.Ref || dry.Route != actual.Route {
		t.Fatalf("dry=%+v actual=%+v", dry, actual)
	}
}

func TestTaskRunDryRunTextLabelsTurnInputUnitsAndLimit(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.run("--dry-run", "--model", "opus", "--", "Check 😀"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(h.stdout.String(), "characters (limit 120000)") {
		t.Fatalf("output=%s", h.stdout.String())
	}
}

func TestTaskRunDryRunExplicitRouteOnOlderCoordinator(t *testing.T) {
	h := newTaskRunHarness()
	h.projectsErr = errRC69RefusesTheProjectsQuery
	args := []string{"--dry-run", "--project", "steward", "--model", "t3-primary/opus", "--no-notify", "--json", "--", "Check"}
	if err := h.cli().run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if len(h.requests) != 0 || len(h.notified) != 0 {
		t.Fatal("dry run submitted")
	}
	var dry dryRunTestDocument
	if err := json.Unmarshal(h.stdout.Bytes(), &dry); err != nil {
		t.Fatal(err)
	}
	if dry.NotifyThread != "" || dry.Route.Instance != "t3-primary" {
		t.Fatalf("dry=%+v", dry)
	}
}

func TestTaskRunDryRunRefusesOverLimitComposedPrompt(t *testing.T) {
	for _, dry := range []bool{true, false} {
		h := newTaskRunHarness()
		args := []string{"--model", "opus", "--", strings.Repeat("😀", compat.MaxTurnInputLength/2)}
		if dry {
			args = append([]string{"--dry-run"}, args...)
		}
		err := h.cli().run(context.Background(), args)
		var tooLarge *backlog.TurnInputTooLargeError
		if !errors.As(err, &tooLarge) || len(h.requests) != 0 || len(h.viability) != 0 {
			t.Fatalf("dry=%t err=%v requests=%d viability=%d", dry, err, len(h.requests), len(h.viability))
		}
	}
}

func TestTaskRunDryRunValidatesLocalManifest(t *testing.T) {
	h := newTaskRunHarness()
	err := h.cli().run(context.Background(), []string{"--dry-run", "--model", "opus", "--outputs", "../bad", "--", "test"})
	if err == nil || len(h.requests) != 0 {
		t.Fatalf("err=%v", err)
	}
	h = newTaskRunHarness()
	h.threadErr = errors.New("no calling thread")
	err = h.cli().run(context.Background(), []string{"--dry-run", "--model", "opus", "--", "test"})
	if err == nil || !strings.Contains(err.Error(), "--no-notify") {
		t.Fatalf("err=%v", err)
	}
}
