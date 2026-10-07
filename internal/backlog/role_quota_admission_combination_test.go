package backlog

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fixedManifestRoles map[string]domain.RoleSelection

func (r fixedManifestRoles) ResolveManifestRoles(context.Context, Manifest) (map[string]domain.RoleSelection, error) {
	return r, nil
}

// A role task's route is chosen by the coordinator at submission (M17-3), and
// rc.117 (M17-4a) admits a submission against quota atomically. The route the
// role selected must pass that admission like an explicit route: a role must
// not be a way around it.
func TestRoleSelectedRoutePassesQuotaAdmission(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(map[bool]string{false: "admitted", true: "refused"}[exhausted], func(t *testing.T) {
			store, gate, now, state := quotaSubmissionIntegrationFixture(t)
			if exhausted {
				state.UsedPercent = 100
				if err := store.SaveBucket(context.Background(), state); err != nil {
					t.Fatal(err)
				}
			}
			service := quotaSubmissionService(t, store, gate, now)
			service.Roles = fixedManifestRoles{"inspect": {Role: "execute", Route: "codex/gpt-5.6-sol", Effort: "low", PolicyDigest: "digest"}}
			bundle := validBundle(t)
			rewriteBundleManifest(t, bundle, `version: 2
name: role-quota
class: required
environment: {project: t3-steward}
role: execute
tasks:
  inspect:
    prompt_file: prompts/inspect.md
    estimated_cost: 60
`)
			result, err := service.SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: "role-quota", BundleDir: bundle})
			if exhausted {
				if err == nil || !strings.Contains(err.Error(), "quota admission refused") || !strings.Contains(err.Error(), "openai") {
					t.Fatalf("role-selected route bypassed quota admission: %+v %v", result, err)
				}
				assertQuotaSubmissionEmpty(t, store, service.StorageRoot)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			receipt := records.WorkflowRuns[0].QuotaAdmission
			if receipt == nil || len(receipt.Roots) != 1 || receipt.Roots[0].Route.ProviderInstanceID != "codex" || receipt.Roots[0].Route.QuotaPoolID != "openai" {
				t.Fatalf("role-selected route was not admitted against its pool: %+v", receipt)
			}
			if task := records.Tasks[0]; task.RoleSelection == nil || len(task.Routes) != 1 || task.Routes[0].Model != "gpt-5.6-sol" {
				t.Fatalf("role selection not applied: %+v", task)
			}
		})
	}
}
