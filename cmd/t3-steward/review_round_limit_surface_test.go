package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestReviewCheckpointRoundLimitReasonCountsAllocatedRounds(t *testing.T) {
	for _, allocatedOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "materialized", true: "allocated-only"}[allocatedOnly], func(t *testing.T) {
			ctx := context.Background()
			h := reviewCheckpointFixture(t, true)
			records, err := h.db.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			task := records.Tasks[0]
			task.ReviewRequirements.RoundLimit = 1
			assignment := h.assign
			assignment.TaskDigest = domain.TaskDigest(task)
			if err := h.db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Tasks: []domain.Task{task}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			h.refs.push(h.branch(), strings.Repeat("d", 40))
			if allocatedOnly {
				h.op.fault = func(phase string) error {
					if phase == "allocated" {
						return errors.New("crash after allocation")
					}
					return nil
				}
			}
			_, err = h.call(t, h.request)
			if allocatedOnly {
				requireCheckpointRefusal(t, err, domain.ReviewCheckpointInternal, true)
			} else if err != nil {
				t.Fatal(err)
			}
			h.op.fault = nil
			next := h.request
			next.CheckpointID = "cp-2"
			h.refs.push(domain.ReviewCheckpointBranch(next.WorkflowRunID, next.TaskID, next.CheckpointID), strings.Repeat("e", 40))
			_, err = h.call(t, next)
			requireCheckpointRefusal(t, err, domain.ReviewCheckpointRoundLimit, false)
			var refusal *domain.ReviewCheckpointRefusal
			if !errors.As(err, &refusal) || !strings.Contains(refusal.Reason, "1 of 1") {
				t.Fatalf("refusal must name allocated count and frozen limit: %v", err)
			}
		})
	}
}

func roundLimitSurfaceGate() domain.ReviewCompletionGate {
	head := strings.Repeat("d", 40)
	return domain.EvaluateReviewCompletionGate(
		&domain.ReviewRoundHead{RoundID: "round-2", Number: 2, CheckpointID: "cp-2", HeadCommit: head, Verdict: "reject", RoundsUsed: 2, RoundLimit: 2},
		&domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema, Head: head}, nil)
}

func TestReviewRoundLimitTaskResultTextAndJSON(t *testing.T) {
	gate := roundLimitSurfaceGate()
	for _, asJSON := range []bool{false, true} {
		f := newTaskResultFixture(t)
		f.detail.Tasks[0].Attempt.Progress = domain.ProgressFailed
		f.detail.Tasks[0].Attempt.Failure = gate.Failure()
		f.detail.Tasks[0].Attempt.ReviewGate = &gate
		f.detail.Summary.Run.Progress = domain.ProgressFailed
		args := []string{"run-1"}
		if asJSON {
			args = append(args, "--json")
		}
		_ = f.run(args...)
		if asJSON {
			doc := f.document(t)
			got := doc.Tasks[0].ReviewGate
			if got == nil || got.Code != domain.ReviewGateRoundLimitExhausted || got.RoundsUsed != 2 || got.RoundLimit != 2 || got.NewRoundNeeded {
				t.Fatalf("JSON gate = %+v", got)
			}
		} else {
			for _, want := range []string{"review-round-limit-exhausted", "2 of 2", "campaign rerun"} {
				if !strings.Contains(f.stdout.String(), want) {
					t.Fatalf("missing %q: %s", want, f.stdout.String())
				}
			}
		}
	}
}

func TestReviewRoundLimitExplainTextAndJSON(t *testing.T) {
	ctx := context.Background()
	h := reviewCheckpointFixture(t, true)
	gate := roundLimitSurfaceGate()
	parent := h.parent
	parent.Progress, parent.Failure, parent.ReviewGate = domain.ProgressFailed, gate.Failure(), &gate
	parent.Revision++
	if err := h.db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}}); err != nil {
		t.Fatal(err)
	}
	response, err := h.admin.Query(ctx, backlogadmin.Query{Version: backlogadmin.Version, Kind: backlogadmin.QueryExplanation, Principal: checkpointPrincipal, WorkflowRunID: h.request.WorkflowRunID, TaskID: h.request.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	renderExplanation(&out, response.Explanation)
	for _, want := range []string{"review-round-limit-exhausted", "2 of 2", "campaign rerun"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q: %s", want, out.String())
		}
	}
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded backlogadmin.Response
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Explanation == nil {
		t.Fatal("missing explanation")
	}
	got := decoded.Explanation.ReviewGate
	if got == nil || got.Code != domain.ReviewGateRoundLimitExhausted || got.RoundsUsed != 2 || got.RoundLimit != 2 || got.NewRoundNeeded {
		t.Fatalf("JSON gate = %+v", got)
	}
}
