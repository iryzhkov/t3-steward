package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

func TestTaskRunDryRunKeyAndPromptMatchSubmission(t *testing.T) {
	h := newTaskRunHarness()
	args := []string{"--model", "opus", "--outputs", "report.md", "--json", "--", "Write résumé"}
	dryArgs := append([]string{"--dry-run"}, args...)
	if err := h.cli().run(context.Background(), dryArgs); err != nil {
		t.Fatal(err)
	}
	var dry taskRunDryDocument
	if err := json.Unmarshal(h.stdout.Bytes(), &dry); err != nil {
		t.Fatal(err)
	}
	expected := len("Write résumé\n") + len(workerruntime.TaskCompletionSupplement([]domain.ArtifactDeclaration{{Name: "report.md"}}))
	if dry.PromptBytes != expected {
		t.Fatalf("bytes=%d want %d", dry.PromptBytes, expected)
	}
	h.stdout.Reset()
	if err := h.cli().run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	var actual taskRunRecord
	if err := json.Unmarshal(h.stdout.Bytes(), &actual); err != nil {
		t.Fatal(err)
	}
	if dry.IdempotencyKey != actual.IdempotencyKey || dry.Project != actual.Project || dry.Ref != actual.Ref || dry.Route != actual.Route {
		t.Fatalf("dry=%+v actual=%+v", dry, actual)
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
	var dry taskRunDryDocument
	if err := json.Unmarshal(h.stdout.Bytes(), &dry); err != nil {
		t.Fatal(err)
	}
	if dry.NotifyThread != "" || dry.Route.Instance != "t3-primary" {
		t.Fatalf("dry=%+v", dry)
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
