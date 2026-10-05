package workerruntime

import (
	"context"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
)

func TestIndependentDeclaredOpaqueModelsAndCopies(t *testing.T) {
	ctx := context.Background()
	cfg := configuredReviewSettings()
	m := configuredReviewManifest(t)
	task := m.Tasks["run"]
	task.ReviewRequirements.Members[0].Route = "codex/org/team/model:version"
	m.Tasks["run"] = task
	cfg.Workers["w"].Providers["codex"] = config.V2Provider{Models: []string{"org/team/model:version"}, QuotaPool: "p"}
	delete(cfg.ReviewRoutes, "codex/org/sol")
	cfg.ReviewRoutes["codex/org/team/model:version"] = config.ReviewRouteMetadata{ProviderFamily: "openai", Tier: "executor"}
	catalog, err := NewConfiguredAdmissionCatalog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = backlog.ValidateTaskReviewAdmission(ctx, m, catalog); err != nil {
		t.Fatal(err)
	}
	first, err := catalog.ReviewAdmissionCatalog(ctx, "repo")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Projects["repo"].Workers[0] = "forged"
	cfg.SetupProfiles["go"].Commands[0] = "forged"
	first.AuthoredWorkers[0].Projects[0].Name = "forged"
	first.Classifications[0].ProviderFamily = "forged"
	second, err := catalog.ReviewAdmissionCatalog(ctx, "repo")
	if err != nil {
		t.Fatal(err)
	}
	third, err := catalog.ReviewAdmissionCatalog(ctx, "repo")
	if err != nil || !reflect.DeepEqual(second, third) {
		t.Fatal("snapshot alias", err)
	}
	if err = backlog.ValidateTaskReviewAdmission(ctx, m, catalog); err != nil {
		t.Fatal("caller alias changed admission", err)
	}
	task = m.Tasks["run"]
	task.ReviewRequirements.Members[0].Route = "codex/model:version"
	m.Tasks["run"] = task
	if err = backlog.ValidateTaskReviewAdmission(ctx, m, catalog); err == nil {
		t.Fatal("opaque model suffix became authorized alias")
	}
}
