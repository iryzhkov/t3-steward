package backlog

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"path/filepath"
	"testing"
)

type changingManifestRoles struct {
	calls int
	err   error
}

func (r *changingManifestRoles) ResolveManifestRoles(_ context.Context, m Manifest) (map[string]domain.RoleSelection, error) {
	r.calls++
	if r.err != nil {
		return nil, r.err
	}
	route := "codex/chosen"
	if r.calls > 1 {
		route = "codex/changed"
	}
	return map[string]domain.RoleSelection{"inspect": {Role: "execute", Route: route, Effort: "low", PolicyDigest: "digest"}}, nil
}

type inspectRoleAdmission struct {
	t     *testing.T
	calls int
}

func (v *inspectRoleAdmission) ValidatePermanent(_ context.Context, m Manifest) error {
	v.calls++
	task := m.Tasks["inspect"]
	if task.Role != "" || len(task.Routes) != 1 || task.Routes[0].Model != "chosen" {
		v.t.Fatalf("unresolved admission: %#v", task)
	}
	return nil
}
func roleSubmissionBundle(t *testing.T) string {
	b := validBundle(t)
	rewriteBundleManifest(t, b, `version: 2
name: role-submission
environment: {project: t3-steward, ref: main}
role: execute
tasks:
  inspect:
    prompt_file: prompts/inspect.md
    outputs: [findings.md]
`)
	return b
}
func TestSubmissionRolesResolveOnceAndPersistValidatedSelection(t *testing.T) {
	for _, registered := range []bool{false, true} {
		t.Run(map[bool]string{false: "submit", true: "register"}[registered], func(t *testing.T) {
			ctx := context.Background()
			store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			resolver := &changingManifestRoles{}
			validation := &inspectRoleAdmission{t: t}
			s := SubmissionService{Store: store, StorageRoot: filepath.Join(t.TempDir(), "storage"), MaxBytes: 1 << 20, MaxFiles: 16, Roles: resolver, Permanent: validation}
			t.Cleanup(func() { _ = removeIngestedTree(s.StorageRoot) })
			_, err = s.SubmitDirectory(ctx, DirectorySubmission{BundleDir: roleSubmissionBundle(t), IdempotencyKey: "roles", RegisterOnly: registered})
			if err != nil {
				t.Fatal(err)
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if resolver.calls != 1 || validation.calls != 1 {
				t.Fatalf("calls resolve=%d validate=%d", resolver.calls, validation.calls)
			}
			task := records.Tasks[0]
			if task.Role != "execute" {
				t.Fatalf("role lost: %#v", task)
			}
			if registered {
				if len(task.Routes) != 0 || task.RoleSelection != nil || len(records.WorkflowRuns) != 0 {
					t.Fatalf("registration resolved: %#v", records)
				}
			} else if len(task.Routes) != 1 || task.Routes[0].Model != "chosen" || task.RoleSelection == nil || task.RoleSelection.PolicyDigest != "digest" {
				t.Fatalf("selection lost: %#v", task)
			}
		})
	}
}
func TestDirectIngesterResolvesBeforePermanentValidation(t *testing.T) {
	resolver := &changingManifestRoles{}
	validation := &inspectRoleAdmission{t: t}
	store := &ingestionStore{}
	ingester := BundleIngester{Store: store, StorageRoot: filepath.Join(t.TempDir(), "storage"), Roles: resolver, Permanent: validation}
	t.Cleanup(func() { _ = removeIngestedTree(ingester.StorageRoot) })
	got, err := ingester.Ingest(context.Background(), roleSubmissionBundle(t))
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 || validation.calls != 1 || got.Records.Tasks[0].Routes[0].Model != "chosen" {
		t.Fatalf("direct ingestion resolve=%d validate=%d records=%#v", resolver.calls, validation.calls, got.Records)
	}
}
func TestSubmissionRoleRefusalCreatesNoWorkflowRecords(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := SubmissionService{Store: store, StorageRoot: filepath.Join(t.TempDir(), "storage"), MaxBytes: 1 << 20, MaxFiles: 16, Roles: &changingManifestRoles{err: errors.New("role policy unavailable")}}
	_, err = s.SubmitDirectory(ctx, DirectorySubmission{BundleDir: roleSubmissionBundle(t), IdempotencyKey: "refused"})
	if err == nil {
		t.Fatal("accepted unavailable role")
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Tasks)+len(records.Artifacts)+len(records.WorkflowRuns)+len(records.Workflows) != 0 {
		t.Fatalf("refusal stored records: %#v", records)
	}
}
