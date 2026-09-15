package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// This subprocess supplies the authenticated coordinator-exchange framing;
// the shell carrier is replaced, not the CLI client or coordinator service.
func TestTaskWaitRemoteExchangeHelper(t *testing.T) {
	if os.Getenv("T3_TASK_WAIT_TEST_HELPER") != "1" {
		return
	}
	credentials := completeAdminCredentials()
	server, err := backlogadmin.NewRemoteServer(backlogadmin.RemoteServerConfig{
		CoordinatorID:   "test-coordinator",
		Clients:         map[string]backlogadmin.AdminCredentials{credentials.ClientPrincipal: credentials},
		Relay:           backlogadmin.LocalClient{Path: os.Getenv("T3_TASK_WAIT_TEST_SOCKET"), CoordinatorID: "test-coordinator", MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, RequestTimeout: 10 * time.Second},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err == nil {
		err = server.Serve(context.Background(), "node-wait", os.Stdin, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	os.Exit(0)
}

func remoteTaskWaitFixture(t *testing.T) (config.Config, *sqlite.Store, *sqlite.Store) {
	t.Helper()
	cfg, coordinator := taskWaitCLIFixture(t)
	previousHome := taskWaitWorkerHome
	isolatedHome := t.TempDir()
	taskWaitWorkerHome = func() (string, error) { return isolatedHome, nil }
	t.Cleanup(func() { taskWaitWorkerHome = previousHome })
	t.Setenv("T3_TASK_WAIT_TEST_SOCKET", cfg.StatePath+".admin.sock")
	t.Setenv("T3_TASK_WAIT_TEST_HELPER", "1")
	withAdminCredentials(t, fixedAdminCredentials{credentials: completeAdminCredentials()})
	bin := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nexec '" + strings.ReplaceAll(executable, "'", "'\\''") + "' -test.run=^TestTaskWaitRemoteExchangeHelper$\n"
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	cfg.StatePath = filepath.Join(t.TempDir(), "worker.db")
	cfg.BacklogV2.LocalWorker.ID = "worker"
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{
		CoordinatorID: "test-coordinator", Address: "disposable.invalid", Connection: "ssh", RemoteCommand: "t3-steward", Credential: "secretref:f03-admin/test",
		RequestTimeout: config.Duration(10 * time.Second), MessageLimits: config.V2MessageLimits{MaxBytes: 1 << 20, MaxFiles: 100, MaxArtifactBytes: 1 << 20},
	}
	worker, err := sqlite.OpenMigrated(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { worker.Close() })
	return cfg, coordinator, worker
}

func TestRemoteTaskWaitRegistrationSettlementAndOwnerDelivery(t *testing.T) {
	ctx := context.Background()
	cfg, coordinator, worker := remoteTaskWaitFixture(t)
	args := []string{"--task", "current", "--request-id", "remote-proof", "--every", "30s", "--max-every", "30s", "--timeout", "5m", "--", "false"}
	if err := cmdTaskWaitAdd(ctx, cfg, args); err != nil {
		t.Fatal(err)
	}
	if err := cmdTaskWaitAdd(ctx, cfg, args); err != nil {
		t.Fatal("registration replay", err)
	}
	checks, err := worker.ListWaits(ctx, "")
	if err != nil || len(checks) != 1 {
		t.Fatalf("duplicate local polls: %v %v", checks, err)
	}
	waits, err := coordinator.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 {
		t.Fatalf("coordinator waits: %v %v", waits, err)
	}
	if _, err := os.Stat(cfg.StatePath + ".admin.sock"); !os.IsNotExist(err) {
		t.Fatal("worker unexpectedly has coordinator socket")
	}
	wrong := &waitTestControl{threads: map[string]*domain.Thread{}}
	coordinatorRunner := wait.New(coordinator, wrong, nil)
	coordinatorRunner.AssignedTaskWakesOnly = true
	coordinatorRunner.TaskWorkerID = "coordinator-worker"
	coordinatorRunner.DisableQuotaChecks = true
	owner := &waitTestControl{threads: map[string]*domain.Thread{"thread-1": {ID: "thread-1", ProviderInstanceID: "codex"}}}
	runner := wait.New(worker, owner, nil)
	if err := configureTaskWaitTransport(runner, cfg); err != nil {
		t.Fatal(err)
	}
	runner.DisableQuotaChecks = true
	runner.SetClock(func() time.Time { return time.Now().Add(time.Minute) })
	runner.Exec = func(context.Context, wait.Wait) (string, int, error) { return "done", 0, nil }
	// Settle locally first, then let the wrong daemon race for the pending wake.
	check := checks[0]
	check.Status = wait.StatusMet
	settled := time.Now()
	check.SettledAt = &settled
	if err := worker.SaveWait(ctx, check); err != nil {
		t.Fatal(err)
	}
	remote := remoteTaskWaitStore{cfg: cfg}
	if _, err := remote.SettleTaskWait(ctx, waits[0].ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet, ObservedAt: settled}, settled); err != nil {
		t.Fatal(err)
	}
	coordinatorRunner.Tick(ctx, nil, nil)
	pending, err := coordinator.TaskWakesAwaitingDelivery(ctx, time.Now())
	if err != nil || len(pending) != 1 || pending[0].WorkerID != "worker" {
		t.Fatalf("wrong host consumed wake: %v %v", pending, err)
	}
	runner.Tick(ctx, nil, nil)
	runner.Tick(ctx, nil, nil)
	if len(owner.sends) != 1 || len(wrong.sends) != 0 {
		t.Fatalf("owner sends=%v wrong=%v", owner.sends, wrong.sends)
	}
	records, err := coordinator.LoadCoordinatorRecords(ctx)
	if err != nil || records.Attempts[0].Control != domain.ControlResuming {
		t.Fatalf("not resumed: %v %v", records.Attempts, err)
	}
	if err := cmdNodeWait(ctx, cfg, []string{"list", "--native", "--json"}); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteTaskWaitCancellationAndNoPollTimeout(t *testing.T) {
	for _, mode := range []string{"cancel", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cfg, coordinator, worker := remoteTaskWaitFixture(t)
			if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", mode, "--", "false"}); err != nil {
				t.Fatal(err)
			}
			waits, _ := coordinator.ListTaskWaits(ctx)
			id := waits[0].ID
			if !nativeWaitArgs([]string{"cancel", id}) {
				t.Fatal("task wait ID routed to interactive store")
			}
			if mode == "cancel" {
				if err := cmdNodeWait(ctx, cfg, []string{"cancel", id, "--json"}); err != nil {
					t.Fatal(err)
				}
				if _, err := (remoteTaskWaitStore{cfg: cfg}).SettleTaskWait(ctx, id, domain.TaskWaitResult{Outcome: domain.TaskWaitMet, ObservedAt: time.Now()}, time.Now()); err != nil {
					t.Fatal(err)
				}
			} else {
				// Model registration committed but local save failed: coordinator expiry
				// and assignment-derived ownership must still deliver the timeout.
				empty, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "no-poll.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer empty.Close()
				worker = empty
				if _, err := coordinator.ExpireTaskWaits(ctx, time.Now().Add(48*time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			control := &waitTestControl{threads: map[string]*domain.Thread{"thread-1": {ID: "thread-1", ProviderInstanceID: "codex"}}}
			runner := wait.New(worker, control, nil)
			if err := configureTaskWaitTransport(runner, cfg); err != nil {
				t.Fatal(err)
			}
			runner.DisableQuotaChecks = true
			runner.Tick(ctx, nil, nil)
			if len(control.sends) != 1 {
				t.Fatalf("no %s wake without polling: %v", mode, control.sends)
			}
			result, _ := coordinator.ListTaskWaits(ctx)
			want := domain.TaskWaitCancelled
			if mode == "timeout" {
				want = domain.TaskWaitTimedOut
			}
			if result[0].Result == nil || result[0].Result.Outcome != want {
				t.Fatalf("first outcome overwritten: %+v", result)
			}
		})
	}
}
