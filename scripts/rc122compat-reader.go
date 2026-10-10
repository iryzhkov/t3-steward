//go:build ignore

// Compiled unchanged against rc121 by rc122-downgrade-compat.sh.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type compatCollectionT3 struct{ workerruntime.T3Control }

func (compatCollectionT3) GetThread(context.Context, string) (*domain.Thread, error) {
	return &domain.Thread{ID: "sidecar-thread", TurnID: "sidecar-turn", TurnState: "completed"}, nil
}
func (compatCollectionT3) LastAssistantMessage(context.Context, string) (string, error) {
	return "BACKLOG STATUS: done", nil
}
func (compatCollectionT3) ExportThread(context.Context, string) ([]byte, error) {
	return []byte(`{"thread":{"id":"sidecar-thread","latestTurn":{"turnId":"sidecar-turn","state":"completed","startedAt":"2026-10-10T00:00:00Z","completedAt":"2026-10-10T00:01:00Z"},"session":{"threadId":"sidecar-thread","status":"ready","activeTurnId":null,"lastError":null},"messages":[{"role":"assistant","text":"BACKLOG STATUS: done"}]}}`), nil
}
func (compatCollectionT3) SettleThread(context.Context, string, string) error { return nil }

type compatCollectionPublisher struct {
	workerruntime.ArtifactPublisher
	root      string
	published bool
}

func (p *compatCollectionPublisher) PublishResult(_ context.Context, _ workerproto.ExecutionPackage, result workerruntime.PublishedResult) error {
	if result.Finalized.Completion.Failure != "" {
		return fmt.Errorf("old collection failed: %s", result.Finalized.Completion.Failure)
	}
	for _, artifact := range result.Finalized.Artifacts {
		if artifact.Name != "answer.txt" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(p.root, filepath.FromSlash(artifact.StoragePath)))
		if err != nil {
			return err
		}
		if string(raw) != "sidecar-compatible output\n" {
			return fmt.Errorf("old collection changed output: %q", raw)
		}
		p.published = true
	}
	if !p.published {
		return fmt.Errorf("old collection did not publish answer.txt")
	}
	return nil
}

func collectSidecar(root string) error {
	var pkg workerproto.ExecutionPackage
	if err := json.NewDecoder(os.Stdin).Decode(&pkg); err != nil {
		return err
	}
	artifactRoot := filepath.Join(root, "artifacts")
	publisher := &compatCollectionPublisher{root: artifactRoot}
	driver := &workerruntime.LocalDriver{
		Config:    workerruntime.LocalDriverConfig{RunsRoot: filepath.Join(root, "runs"), ArtifactRoot: artifactRoot},
		Finalizer: backlog.AttemptFinalizer{StorageRoot: artifactRoot},
		Publisher: publisher, T3: compatCollectionT3{}, Now: time.Now,
	}
	attempt := filepath.Join(root, "runs", pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, pkg.Identity.AttemptID)
	sidecar := filepath.Join(attempt, "preserved-result.json")
	original, err := os.ReadFile(sidecar)
	if err != nil {
		return err
	}
	if err := driver.Collect(context.Background(), pkg, filepath.Join(attempt, "workspace")); err != nil {
		return err
	}
	after, err := os.ReadFile(sidecar)
	if err != nil {
		return err
	}
	if !bytes.Equal(original, after) {
		return fmt.Errorf("rc121 modified preserved-result sidecar")
	}
	if !publisher.published {
		return fmt.Errorf("rc121 did not publish output")
	}
	fmt.Println("rc121 LocalDriver.Collect captured and published output with candidate sidecar present")
	return nil
}

func run() error {
	switch os.Args[1] {
	case "collect-sidecar":
		return collectSidecar(os.Args[2])
	case "store":
		s, err := sqlite.Open(os.Args[2])
		if err != nil {
			return err
		}
		defer s.Close()
		r, err := s.LoadCoordinatorRecords(context.Background())
		if err != nil {
			return err
		}
		if len(r.Attempts) != 2 || len(r.Tasks) != 1 || len(r.Artifacts) != 1 || len(r.AdminCommands) != 1 {
			return fmt.Errorf("fixture incomplete: attempts=%d tasks=%d artifacts=%d", len(r.Attempts), len(r.Tasks), len(r.Artifacts))
		}
		if r.Attempts[0].Failure != "workspace is missing" {
			return fmt.Errorf("failure lost: %+v", r.Attempts[0])
		}
		waits, err := s.ListTaskWaits(context.Background())
		if err != nil || len(waits) != 1 {
			return fmt.Errorf("rc121 wait read: count=%d err=%v", len(waits), err)
		}
		localWaits, err := s.ListWaits(context.Background(), "thread-2")
		if err != nil || len(localWaits) != 2 {
			return fmt.Errorf("rc121 local wait read: count=%d err=%v", len(localWaits), err)
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	case "config":
		_, err := config.LoadFile(os.Args[2])
		return err
	case "encode-snapshot":
		return (workerproto.Codec{MaxBytes: 1 << 20}).Encode(os.Stdout, workerproto.Observations{Snapshot: domain.WorkerSnapshot{WorkerID: "compat-worker", WorkerEpoch: "compat-epoch", CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC), ValidUntil: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC), Inventory: domain.WorkerInventory{ID: "compat-worker", Epoch: "compat-epoch", Sequence: 1, AcceptBacklog: true, Health: domain.WorkerHealthReady, Capabilities: []string{"provider-resume-v1"}, ObservedAt: time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)}}})
	case "decode-snapshot":
		var r workerproto.Observations
		if err := (workerproto.Codec{MaxBytes: 1 << 20}).Decode(os.Stdin, &r); err != nil {
			return err
		}
		if r.Snapshot.Inventory.ID != "compat-worker" {
			return fmt.Errorf("worker identity lost")
		}
		store, err := sqlite.OpenMigrated(":memory:")
		if err != nil {
			return err
		}
		defer store.Close()
		if err := store.SaveWorkerSnapshot(context.Background(), r.Snapshot); err != nil {
			return err
		}
		snapshots, err := store.LoadWorkerSnapshots(context.Background())
		if err != nil || len(snapshots) != 1 {
			return fmt.Errorf("rc121 coordinator snapshot persistence: %v", err)
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	case "encode-request":
		return (workerproto.Codec{MaxBytes: 1 << 20}).Encode(os.Stdout, workerproto.SnapshotRequest{ParkedReported: true})
	case "decode-request":
		var r workerproto.SnapshotRequest
		if err := (workerproto.Codec{MaxBytes: 1 << 20}).Decode(os.Stdin, &r); err != nil {
			return err
		}
		if !r.ParkedReported {
			return fmt.Errorf("parked evidence lost")
		}
		return nil
	}
	return fmt.Errorf("unknown mode")
}
