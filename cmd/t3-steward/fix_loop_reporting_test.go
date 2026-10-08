package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignShowAndWakeUseFixLoopEvidence(t *testing.T) {
	detail := backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{{
		Task:    domain.Task{ID: "review", Name: "review", FixLoop: &domain.FixLoopTask{Name: "patch", Round: 2, MaxRounds: 2, Kind: "review"}},
		Attempt: &domain.Attempt{TaskID: "review", Progress: domain.ProgressSucceeded, ReviewVerdict: &domain.ReviewVerdict{Verdict: "changes-requested"}},
	}}}
	var out bytes.Buffer
	renderWorkflow(&out, &detail)
	for _, want := range []string{"rounds=2/2", "final verdict=changes-requested", "escalation=fix-loop-exhausted"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in %s", want, out.String())
		}
	}
	run := summaryRunOf(detail)
	if len(run.FixLoops) != 1 || !run.FixLoops[0].Exhausted {
		t.Fatalf("wake projection: %+v", run.FixLoops)
	}
}

func TestCampaignFixLoopsHelpTopic(t *testing.T) {
	var out bytes.Buffer
	handled, err := admitCampaignHelp(&out, []string{"help", "fix-loops"})
	if !handled || err != nil || !strings.Contains(out.String(), "needs_verdict") || !strings.Contains(out.String(), "max_rounds") {
		t.Fatalf("help handled=%v err=%v: %s", handled, err, out.String())
	}
	if !strings.Contains(campaign.FixLoopsHelp, "fix-loop-exhausted") {
		t.Fatal("missing exhaustion contract")
	}
}
