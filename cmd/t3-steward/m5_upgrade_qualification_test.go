package main

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// The supported upgrade path first drains admission without replacing the
// worker's execution identity, waits for retained custody to settle, changes the
// catalog, and only then resumes admission. Every configuration and store path
// in this fixture is rooted in t.TempDir.
func TestM5SupportedIsolatedUpgradeDrainsSettlesAndResumes(t *testing.T) {
	ctx := context.Background()
	fixture := newReloadFixture(t)
	workerID := qualificationWorkerID()
	connectedWorker := fixture.cfg.BacklogV2.Workers[workerID]
	connectedWorker.Connection = "persistent-ssh"
	fixture.cfg.BacklogV2.Workers[workerID] = connectedWorker
	fixture.cfg.BacklogV2.MessageLimits.MaxArtifactBytes = 16 << 20
	fixture.cfg.BacklogV2.Transport.RequestTimeout = config.Duration(time.Minute)
	fixture.cfg.BacklogV2.Freshness.WorkerMaxAge = config.Duration(time.Minute)
	writeReloadConfig(t, fixture.cfg.Path, fixture.cfg)
	assignment := fixture.retainAssignment(t, workerID)

	initial, err := workerruntime.BuildWorkerBinding(fixture.cfg.BacklogV2, workerID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !initial.Inventory.AcceptBacklog {
		t.Fatal("fixture must begin with admission open")
	}

	drainedConfig := cloneReloadConfig(t, fixture.cfg)
	drainedWorker := drainedConfig.BacklogV2.Workers[workerID]
	drainedWorker.AcceptBacklog = false
	drainedConfig.BacklogV2.Workers[workerID] = drainedWorker
	writeReloadConfig(t, fixture.cfg.Path, drainedConfig)

	drain := evaluateCoordinatorReload(ctx, fixture.cfg, fixture.logger, fixture.store, fixture.receipts)
	if !drain.proceed {
		t.Fatalf("admission drain was refused with retained work: %+v", drain.receipt)
	}
	drained, err := workerruntime.BuildWorkerBinding(drain.next.BacklogV2, workerID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if drained.Inventory.AcceptBacklog || drained.CatalogRevision != initial.CatalogRevision {
		t.Fatalf("drain replaced execution identity or left admission open: before=%+v after=%+v", initial, drained)
	}
	fixture.cfg = drain.next

	upgradedConfig := cloneReloadConfig(t, fixture.cfg)
	project := upgradedConfig.BacklogV2.Projects["steward"]
	project.DefaultRef = "release-candidate"
	upgradedConfig.BacklogV2.Projects["steward"] = project
	writeReloadConfig(t, fixture.cfg.Path, upgradedConfig)

	blocked := evaluateCoordinatorReload(ctx, fixture.cfg, fixture.logger, fixture.store, fixture.receipts)
	if blocked.proceed || len(blocked.receipt.Blockers) != 1 ||
		blocked.receipt.Blockers[0].AssignmentID != assignment.ID {
		t.Fatalf("catalog upgrade did not remain blocked by retained custody: %+v", blocked.receipt)
	}

	assignment.State = domain.AssignmentReleased
	assignment.LeaseExpiresAt = time.Time{}
	assignment.UpdatedAt = time.Now().UTC()
	if err := fixture.store.SaveCoordinatorRecords(ctx, coordinatorRecordsWithAssignment(assignment)); err != nil {
		t.Fatal(err)
	}

	upgrade := evaluateCoordinatorReload(ctx, fixture.cfg, fixture.logger, fixture.store, fixture.receipts)
	if !upgrade.proceed {
		t.Fatalf("settled catalog upgrade was refused: %+v", upgrade.receipt)
	}
	upgraded, err := workerruntime.BuildWorkerBinding(upgrade.next.BacklogV2, workerID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.Inventory.AcceptBacklog || upgraded.CatalogRevision == drained.CatalogRevision {
		t.Fatalf("upgrade did not retain the drain and replace only the catalog: drained=%+v upgraded=%+v", drained, upgraded)
	}
	fixture.cfg = upgrade.next

	resumedConfig := cloneReloadConfig(t, fixture.cfg)
	resumedWorker := resumedConfig.BacklogV2.Workers[workerID]
	resumedWorker.AcceptBacklog = true
	resumedConfig.BacklogV2.Workers[workerID] = resumedWorker
	writeReloadConfig(t, fixture.cfg.Path, resumedConfig)

	resume := evaluateCoordinatorReload(ctx, fixture.cfg, fixture.logger, fixture.store, fixture.receipts)
	if !resume.proceed {
		t.Fatalf("post-upgrade admission resume was refused: %+v", resume.receipt)
	}
	resumed, err := workerruntime.BuildWorkerBinding(resume.next.BacklogV2, workerID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.Inventory.AcceptBacklog || resumed.CatalogRevision != upgraded.CatalogRevision {
		t.Fatalf("resume replaced upgraded execution identity or left admission closed: upgraded=%+v resumed=%+v", upgraded, resumed)
	}
}

func coordinatorRecordsWithAssignment(assignment domain.Assignment) sqlite.CoordinatorRecords {
	return sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}}
}
