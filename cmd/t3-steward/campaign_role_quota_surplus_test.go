package main

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignRoleQuotaSurplusFallthrough(t *testing.T) {
	for _, state := range []domain.AdmissionState{domain.AdmissionConstrained, domain.AdmissionRecovering} {
		t.Run(string(state), func(t *testing.T) {
			admin, store, gate, setUsed := campaignRankedQuotaFixture(t)
			setUsed("claude", 10)
			err := store.CommitQuotaAdmissionTransitions(context.Background(), []domain.QuotaAdmissionTransition{{
				ExpectedRevision: 2,
				Record: domain.QuotaAdmissionRecord{QuotaPoolID: "claude-pool", Revision: 3, Admission: state,
					ObservedAt: probeNow, AppliedAt: probeNow, Reason: "fixture surplus admission gate"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			bundle := campaignRankedQuotaBundle(t, false)
			name := filepath.Join(bundle, "workflow.yaml")
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(name, []byte(strings.Replace(string(data), "class: required", "class: surplus", 1)), 0600); err != nil {
				t.Fatal(err)
			}
			service := &backlog.SubmissionService{Store: store, StorageRoot: filepath.Join(t.TempDir(), "storage"),
				MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return probeNow },
				QuotaAdmission: gate, Roles: coordinatorManifestRoleResolver{admin: admin}}
			t.Cleanup(func() {
				if err := filepath.WalkDir(service.StorageRoot, func(path string, entry fs.DirEntry, err error) error {
					if os.IsNotExist(err) {
						return nil
					}
					if err != nil {
						return err
					}
					if entry.IsDir() {
						return os.Chmod(path, 0700)
					}
					return nil
				}); err != nil {
					t.Error(err)
				}
			})
			_, err = service.SubmitDirectory(context.Background(), backlog.DirectorySubmission{IdempotencyKey: "surplus-fallback", BundleDir: bundle})
			if err != nil {
				t.Fatalf("usable codex fallback refused with first pool %s: %v", state, err)
			}
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(records.Tasks) != 1 || records.Tasks[0].RoleSelection == nil {
				t.Fatalf("missing selection receipt: %+v", records.Tasks)
			}
			selected := records.Tasks[0].RoleSelection
			if selected.Route != "codex/model" {
				t.Fatalf("surplus chose class-gated %s instead of usable codex/model: route=%s reason=%s", state, selected.Route, selected.Reason)
			}
			if len(selected.Candidates) != 2 || !strings.Contains(selected.Candidates[0].Reason, string(state)) {
				t.Fatalf("class admission rejection missing from candidate receipt: %+v", selected)
			}
		})
	}
}
