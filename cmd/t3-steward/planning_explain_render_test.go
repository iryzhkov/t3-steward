package main

import (
	"bytes"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"os"
	"strings"
	"testing"
	"time"
)

func TestB1PlanningExplanationRendering(t *testing.T) {
	explanation := backlogadmin.Explanation{WorkflowRunID: "run-1", TaskID: "task-a", Summary: "waiting: not assigned in the last planning pass", Blockers: []backlogadmin.Blocker{{Code: "quota-pool-concurrency", Detail: "quota pool claude-main is at its concurrency limit (4 of 4)", WorkerID: "homelab", QuotaPoolID: "claude-main"}}, Details: []string{"queue position 3 of 7 ready tasks; ahead: 2 required tasks"}}
	for _, diagnose := range []bool{false, true} {
		var out bytes.Buffer
		if diagnose {
			fixture := diagnoseFixture(time.Now())
			fixture.Explanations = []backlogadmin.Explanation{explanation}
			renderDiagnosis(&out, fixture)
		} else {
			renderExplanation(&out, &explanation)
		}
		for _, want := range []string{"quota-pool-concurrency", "[worker homelab, pool claude-main]", "queue position 3 of 7 ready tasks"} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("diagnose=%t missing %q in %s", diagnose, want, out.String())
			}
		}
	}
}

func TestB1NoBlockedArtifact(t *testing.T) {
	if _, err := os.Stat("../../BLOCKED.md"); !os.IsNotExist(err) {
		t.Fatalf("BLOCKED.md must not ship: %v", err)
	}
}
