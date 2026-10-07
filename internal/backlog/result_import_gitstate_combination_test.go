package backlog

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// rc.117 combination: M16-0 uploads a declared commit's bundle, and main's
// failed attempts a work-in-progress bundle, as git-state artifacts; M16-3
// uploads the workspace HEAD report as a git-state artifact too. Only the
// report is a workspace HEAD: a bundle must neither count as one nor be read
// as the HEAD the review gate compares.
func TestResultImportTellsTheWorkspaceHeadFromCommitBundles(t *testing.T) {
	for _, declared := range []bool{false, true} {
		name := "undeclared"
		if declared {
			name = "review-declared"
		}
		t.Run(name, func(t *testing.T) {
			f := newReviewGateFixture(t, declared, true)
			var head *domain.WorkspaceHead
			if declared {
				f.openRound(t, "cp-1", reviewGateHeadA, "accept")
				head = cleanWorkspaceHead(reviewGateHeadA)
			}
			f.finishTurn(t)
			response, data := f.result(t, head, reviewGateHeadA)
			bundle := []byte("# v2 git bundle\n")
			data["bundle-1"] = bundle
			objects := append(response.Manifest.Objects,
				resultObject("bundle-1", "results/"+CommitBundleArtifactName("change"), string(domain.ArtifactGitState), CommitBundleMediaType, bundle))
			manifest := resultManifest(coordinatorTestTime, f.assignment, objects)
			response = workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
			report, err := reviewGateImporter(t, f.store).Import(context.Background(), response, data)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Transition) != 1 {
				t.Fatalf("report = %#v", report)
			}
			records, err := f.store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, attempt := range records.Attempts {
				if attempt.ID != f.attempt.ID {
					continue
				}
				if attempt.Progress != domain.ProgressSucceeded {
					t.Fatalf("attempt = %s: %s", attempt.Progress, attempt.Failure)
				}
				if declared && (attempt.ReviewGate == nil || !attempt.ReviewGate.Passed || attempt.ReviewGate.PhysicalHead != reviewGateHeadA) {
					t.Fatalf("review gate = %+v", attempt.ReviewGate)
				}
				return
			}
			t.Fatal("attempt is gone")
		})
	}
}
