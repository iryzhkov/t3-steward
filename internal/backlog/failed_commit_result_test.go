package backlog

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestFailedCommitResultAdmission(t *testing.T) {
	task := domain.Task{ID: "implement", Outputs: []domain.ArtifactDeclaration{{Name: "candidate", Commit: &domain.CommitOutput{}}}, Verification: []string{"exit 7"}}
	attempt := domain.Attempt{ID: "failed-attempt", TaskID: task.ID, WorkflowRunID: "run"}
	failure := "verification command failed (7): exit 7"
	p := CommitProvenance{WorkflowRunID: "run", TaskID: task.ID, Name: "candidate", Repository: "repository", Base: strings.Repeat("1", 40), Commit: strings.Repeat("2", 40), Ref: FailedCampaignRef("run", task.ID, attempt.ID, "candidate"), FailedAttempt: &FailedCommitAttempt{ID: attempt.ID, VerificationFailures: []string{failure}}}
	raw, err := MarshalCommitProvenance(p)
	if err != nil {
		t.Fatal(err)
	}
	verification, _ := json.Marshal(VerificationReport{Command: "exit 7", ExitCode: 7, StartedAt: time.Now()})
	artifacts := []domain.Artifact{{Kind: domain.ArtifactGitState, Name: FailedCommitArtifactName("candidate"), MediaType: "application/json"}, {Kind: domain.ArtifactVerification, Name: "verification/001.json"}}
	payloads := [][]byte{raw, verification}
	records, missing, err := failedCommitResultRecords(task, attempt, artifacts, payloads, []string{"candidate"})
	if err != nil || len(records) != 1 || len(missing) != 0 {
		t.Fatalf("valid record refused: %+v %v %v", records, missing, err)
	}
	if err := validateFailedCommitResultOutcome(records, false, failure); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{"missing explicit success", failure + "; missing declared output: handoff.md", ""} {
		if err := validateFailedCommitResultOutcome(records, false, reason); err == nil {
			t.Fatalf("admitted nonverification outcome %q", reason)
		}
	}
	if err := validateFailedCommitResultOutcome(records, true, failure); err == nil {
		t.Fatal("admitted passing outcome")
	}
	for _, change := range []func(*CommitProvenance){
		func(p *CommitProvenance) {
			p.FailedAttempt.ID = "other-attempt"
			p.Ref = FailedCampaignRef("run", task.ID, "other-attempt", "candidate")
		},
		func(p *CommitProvenance) {
			p.FailedAttempt.VerificationFailures = []string{"verification command failed (9): exit 9"}
		},
		func(p *CommitProvenance) { p.Ref = CampaignRef("run", task.ID, "candidate") },
	} {
		q := p
		a := *p.FailedAttempt
		q.FailedAttempt = &a
		change(&q)
		altered, _ := MarshalCommitProvenance(q)
		if _, _, err := failedCommitResultRecords(task, attempt, artifacts, [][]byte{altered, verification}, []string{"candidate"}); err == nil {
			t.Fatalf("admitted forged provenance %+v", q)
		}
	}
	if _, _, err := failedCommitResultRecords(task, attempt, artifacts, payloads, nil); err == nil {
		t.Fatal("admitted failed commit also published as ordinary output")
	}
	// Admission uses the declaration and media type, then validates payload above.
	object := workerproto.ArtifactObject{ID: "failed-record", Path: "results/" + FailedCommitArtifactName("candidate"), Kind: string(domain.ArtifactGitState), MediaType: "application/json", Size: int64(len(raw)), SHA256: strings.Repeat("1", 64)}
	if _, err := resultArtifact(object, workerproto.ArtifactTransferManifest{}, attempt, task, time.Now()); err != nil {
		t.Fatal(err)
	}
	object.MediaType = "text/plain"
	if _, err := resultArtifact(object, workerproto.ArtifactTransferManifest{}, attempt, task, time.Now()); err == nil {
		t.Fatal("admitted failed provenance with arbitrary media type")
	}
}

func TestFailedCommitProvenanceIdentityFence(t *testing.T) {
	p := CommitProvenance{WorkflowRunID: "run", TaskID: "task", Name: "commit", Repository: "repository", Base: strings.Repeat("1", 40), Commit: strings.Repeat("2", 40), Ref: FailedCampaignRef("run", "task", "attempt", "commit"), FailedAttempt: &FailedCommitAttempt{ID: "attempt", VerificationFailures: []string{"verification command failed (7): exit 7"}}}
	if err := ValidateCommitProvenance(p); err != nil {
		t.Fatal(err)
	}
	p.FailedAttempt.ID = "../escape"
	if err := ValidateCommitProvenance(p); err == nil {
		t.Fatal("accepted unsafe failed attempt")
	}
	p.FailedAttempt.ID = "attempt"
	p.FailedAttempt.VerificationFailures = []string{"missing output"}
	if err := ValidateCommitProvenance(p); err == nil {
		t.Fatal("accepted nonverification failure")
	}
}
