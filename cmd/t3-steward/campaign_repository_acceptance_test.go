package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// probeCampaignManifest names the project whose repository the probe observes,
// so the acceptance path exercises the same catalog entry the matrix does.
const probeCampaignManifest = `version: 2
name: dev-fleet-migration
environment:
  project: dev-fleet
inputs:
  - inputs/plan.md
tasks:
  implement:
    prompt_file: prompts/implement.md
    outputs: [report.md]
`

func probeCampaignFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range map[string]string{
		"workflow.yaml":        probeCampaignManifest,
		"inputs/plan.md":       "the plan\n",
		"prompts/implement.md": "implement the plan\n",
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// probeSubmissions builds the coordinator's submission path with the acceptance
// gate wired to the same readiness composer the client asks.
func probeSubmissions(t *testing.T, service *backlogadmin.Service, store *sqlite.Store) *backlog.SubmissionService {
	t.Helper()
	storage := filepath.Join(t.TempDir(), "bundles")
	// An ingested tree is made immutable on purpose, so the test has to restore
	// write permission before the temporary directory can be removed.
	t.Cleanup(func() {
		_ = filepath.WalkDir(storage, func(path string, _ os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			_ = os.Chmod(path, 0o700)
			return nil
		})
	})
	return &backlog.SubmissionService{
		StorageRoot: storage,
		Store:       store,
		MaxBytes:    4 << 20,
		MaxFiles:    1000,
		Permanent:   coordinatorPermanentValidator{admin: service},
	}
}

func probeWorkflowCount(t *testing.T, store *sqlite.Store) int {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return len(records.Workflows)
}

// TestSubmissionRefusesACampaignNamingAnUnreachableRepository is the failure
// that started this work, closed end to end and in process.
//
// The campaign names a repository that does not exist. Every candidate worker
// is asked over the worker protocol, every one of them answers
// repository-not-found, and the coordinator refuses the submission before a
// workflow record exists. Previously this bundle was accepted and failed hours
// later, when a worker first tried to prepare its workspace.
func TestSubmissionRefusesACampaignNamingAnUnreachableRepository(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{
		"homelab": absentRepositoryWorker(t, "homelab"),
	})
	service, store := probeReadinessService(t, observer, "homelab")
	submissions := probeSubmissions(t, service, store)

	_, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "campaign-1",
		BundleDir:      probeCampaignFixture(t),
		Principal:      "local:1000",
	})
	if err == nil {
		t.Fatal("a campaign naming an unreachable repository was accepted")
	}
	if !strings.Contains(err.Error(), "can never run as written") ||
		!strings.Contains(err.Error(), backlogadmin.ReasonRepositoryNotFound) {
		t.Fatalf("refusal did not name the observed reason: %v", err)
	}
	if count := probeWorkflowCount(t, store); count != 0 {
		t.Fatalf("a refused campaign created %d workflow(s)", count)
	}
}

// TestSubmissionProceedsForAReachableRepository states that the gate only
// refuses. An observed, reachable repository changes nothing about what the
// submission becomes.
func TestSubmissionProceedsForAReachableRepository(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{
		"homelab": reachableWorker(t, "homelab"),
	})
	service, store := probeReadinessService(t, observer, "homelab")
	submissions := probeSubmissions(t, service, store)

	result, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "campaign-2",
		BundleDir:      probeCampaignFixture(t),
		Principal:      "local:1000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Record.WorkflowID == "" {
		t.Fatalf("an accepted campaign created no workflow: %+v", result)
	}
	if count := probeWorkflowCount(t, store); count != 1 {
		t.Fatalf("workflows = %d, want 1", count)
	}
}

// TestSubmissionProceedsWhenReachabilityIsUnobserved states the degradation at
// the acceptance gate: with no worker to ask, the campaign is not refused. An
// unasked question must never read as a failed one.
func TestSubmissionProceedsWhenReachabilityIsUnobserved(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{})
	service, store := probeReadinessService(t, observer, "homelab")
	submissions := probeSubmissions(t, service, store)

	if _, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "campaign-3",
		BundleDir:      probeCampaignFixture(t),
		Principal:      "local:1000",
	}); err != nil {
		t.Fatalf("an unobserved reachability refused a submission: %v", err)
	}
	if count := probeWorkflowCount(t, store); count != 1 {
		t.Fatalf("workflows = %d, want 1", count)
	}
}
