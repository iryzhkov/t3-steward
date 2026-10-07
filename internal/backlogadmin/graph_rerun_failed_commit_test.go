package backlogadmin

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"io"
	"slices"
	"strings"
	"testing"
)

func failedCommitRerunFixture(t *testing.T) (*Service, *sqlite.Store, string) {
	t.Helper()
	service, store, root := rerunFixture(t)
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	failure := "verification command failed (1): go test ./..."
	for i := range records.Tasks {
		if records.Tasks[i].Name == "implement" {
			records.Tasks[i].Outputs = append(records.Tasks[i].Outputs, domain.ArtifactDeclaration{Name: "implementation", Commit: &domain.CommitOutput{}})
		}
		if records.Tasks[i].Name == "qualify" {
			records.Tasks[i].DependencyInputs["implement"] = []string{"implementation", "result.txt"}
		}
	}
	for i := range records.Attempts {
		if records.Attempts[i].TaskID == "implement" {
			records.Attempts[i].Failure = failure
		}
	}
	p := backlog.CommitProvenance{Version: backlog.CampaignCommitRecordVersion, WorkflowRunID: "run", TaskID: "implement", Name: "implementation", Repository: "repo", Base: strings.Repeat("a", 40), Commit: strings.Repeat("a", 40), Ref: backlog.FailedCampaignRef("run", "implement", "attempt-implement", "implementation"), FailedAttempt: &backlog.FailedCommitAttempt{ID: "attempt-implement", VerificationFailures: []string{failure}}}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := backlog.PrepareGraphInput(root, "failed-record", "run", "implement", string(raw), service.now())
	if err != nil {
		t.Fatal(err)
	}
	artifact.Kind = domain.ArtifactGitState
	artifact.Name = backlog.FailedCommitArtifactName("implementation")
	artifact.AttemptID = "attempt-implement"
	records.Artifacts = append(records.Artifacts, artifact)
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	return service, store, root
}

func TestRerunUseCommitCarriesFailedEvidenceAndReplays(t *testing.T) {
	ctx := context.Background()
	service, store, root := failedCommitRerunFixture(t)
	before := sourceSnapshot(t, store)
	request := rerunRequest("failed-commit-reuse", "qualify")
	if _, err := service.AmendGraph(ctx, Principal{ID: "operator"}, request); err == nil {
		t.Fatal("ordinary rerun accepted failed ancestor")
	}
	request.UseCommit = true
	result, err := service.AmendGraph(ctx, Principal{ID: "operator"}, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Graph.Tasks) != 1 || len(result.Graph.Tasks[0].CarriedInputs) != 2 {
		t.Fatalf("wrong inputs: %+v", result.Graph.Tasks)
	}
	if !slices.Contains(result.Graph.Tasks[0].Placement.Capabilities, "campaign-failed-commit-v1") {
		t.Fatal("failed candidate can be placed on an old bundle-only worker")
	}
	reused := result.Graph.RerunOf.ReusedCommits
	if reused == nil || len(*reused) != 1 || (*reused)[0].SourceAttemptID != "attempt-implement" || (*reused)[0].VerificationFailures[0] != "verification command failed (1): go test ./..." {
		t.Fatalf("lost failure provenance: %+v", reused)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v := newView(records, nil, nil, RuntimeInfo{}, service.now())
	explanation, ok := v.explanation(result.Run.ID, result.Graph.Tasks[0].ID)
	if !ok || !strings.Contains(strings.Join(explanation.Details, " "), "attempt-implement") || !strings.Contains(strings.Join(explanation.Details, " "), "verification command failed") {
		t.Fatalf("explain lost failed provenance: %+v", explanation)
	}
	artifacts := backlog.CoordinatorArtifactStore{Root: root, Catalog: store}
	for _, input := range result.Graph.Tasks[0].CarriedInputs {
		if input.Name != "implementation" {
			continue
		}
		_, content, err := artifacts.Open(ctx, input.ArtifactID)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(content)
		content.Close()
		if err != nil {
			t.Fatal(err)
		}
		p, err := backlog.ParseCommitProvenance(raw)
		if err != nil || p.FailedAttempt == nil || p.FailedAttempt.ID != input.SourceAttemptID {
			t.Fatalf("consumer record: %+v %v", p, err)
		}
	}
	replay, err := service.AmendGraph(ctx, Principal{ID: "operator"}, request)
	if err != nil || !replay.Replay || replay.Run.ID != result.Run.ID {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	request.ID = "other-key"
	other, err := service.AmendGraph(ctx, Principal{ID: "operator"}, request)
	if err != nil || other.Run.ID == result.Run.ID {
		t.Fatalf("new key: %+v %v", other, err)
	}
	if sourceSnapshot(t, store) != before {
		t.Fatal("source mutated")
	}
}

func TestRerunUseCommitRefusesMissingRecordAndOtherFailures(t *testing.T) {
	for _, scenario := range []string{"missing-record", "other-failure", "stale-attempt", "missing-file"} {
		t.Run(scenario, func(t *testing.T) {
			service, store, _ := failedCommitRerunFixture(t)
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "missing-record":
				var kept []domain.Artifact
				for _, a := range records.Artifacts {
					if a.ID != "failed-record" {
						kept = append(kept, a)
					}
				}
				records.Artifacts = kept
				// SaveCoordinatorRecords is additive; use a separate reader snapshot for missing custody.
				service.reader = &failedRerunReader{Store: store, records: records}
			case "other-failure":
				for i := range records.Attempts {
					if records.Attempts[i].TaskID == "implement" {
						records.Attempts[i].Failure += "; missing explicit success"
					}
				}
			case "stale-attempt":
				for i := range records.Attempts {
					if records.Attempts[i].TaskID == "implement" {
						records.Attempts[i].ID = "new-attempt"
						records.Attempts[i].Number = 2
					}
				}
			case "missing-file":
				var kept []domain.Artifact
				for _, a := range records.Artifacts {
					if a.ID != "output-implement" {
						kept = append(kept, a)
					}
				}
				records.Artifacts = kept
				service.reader = &failedRerunReader{Store: store, records: records}
			}
			if scenario == "other-failure" || scenario == "stale-attempt" {
				if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
					t.Fatal(err)
				}
			}
			request := rerunRequest("refuse-"+scenario, "qualify")
			request.UseCommit = true
			_, err = service.AmendGraph(context.Background(), Principal{ID: "operator"}, request)
			if err == nil {
				t.Fatal("accepted unusable source")
			}
			if scenario == "missing-record" && (!strings.Contains(err.Error(), "implement") || !strings.Contains(err.Error(), "implementation")) {
				t.Fatalf("missing diagnostic: %v", err)
			}
			if scenario == "missing-file" && !strings.Contains(err.Error(), "result.txt") {
				t.Fatalf("missing file diagnostic: %v", err)
			}
		})
	}
}

type failedRerunReader struct {
	*sqlite.Store
	records sqlite.CoordinatorRecords
}

func (r *failedRerunReader) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return r.records, nil
}
