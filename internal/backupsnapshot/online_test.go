package backupsnapshot

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	storesqlite "github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func TestCreateOnlineWhileCoordinatorWritesAndRestoreDrillUsesBackupIdentity(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	database := filepath.Join(root, "state.db")
	artifacts := filepath.Join(root, "artifacts")
	submissions := filepath.Join(root, "submissions")
	store, err := sqlitetest.OpenMigrated(database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.AcquireCoordinator(ctx, "backup-origin"); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", database+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	writer.SetMaxOpenConns(1)
	a := onlineTestArtifact(t, writer, artifacts, "artifact-1", "worker", []byte("committed output"))
	b := onlineTestArtifact(t, writer, submissions, "artifact-2", "submission", []byte("committed input"))
	if err := os.MkdirAll(filepath.Join(artifacts, ".uploads"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(artifacts, ".uploads", "partial"), []byte("in-flight bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	stop := make(chan struct{})
	writerErr := make(chan error, 1)
	var once sync.Once
	go func() {
		for n := 0; ; n++ {
			select {
			case <-stop:
				writerErr <- nil
				return
			default:
			}
			tx, err := writer.BeginTx(ctx, nil)
			if err != nil {
				writerErr <- err
				once.Do(func() { close(started) })
				return
			}
			for _, key := range []string{"online-left", "online-right"} {
				if _, err = tx.ExecContext(ctx, "INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, strconv.Itoa(n)); err != nil {
					break
				}
			}
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
			if err != nil {
				writerErr <- err
				once.Do(func() { close(started) })
				return
			}
			once.Do(func() { close(started) })
		}
	}()
	<-started
	manager := Manager{Limits: Limits{MaxFiles: 100, MaxBytes: 16 << 20}, SubmissionRoot: submissions}
	snapshot := filepath.Join(root, "online-snapshot")
	manifest, backupErr := manager.CreateOnline(ctx, database, artifacts, snapshot)
	close(stop)
	if err := <-writerErr; err != nil {
		t.Fatal(err)
	}
	if backupErr != nil {
		t.Fatal(backupErr)
	}
	if len(manifest.Files) != 3 {
		t.Fatalf("manifest files = %#v", manifest.Files)
	}
	if _, err := manager.Verify(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	image, err := storesqlite.OpenReadOnly(filepath.Join(snapshot, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	left, leftOK, leftErr := image.GetKV(ctx, "online-left")
	right, rightOK, rightErr := image.GetKV(ctx, "online-right")
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}
	if leftErr != nil || rightErr != nil || !leftOK || !rightOK || left != right {
		t.Fatalf("torn online transaction: left=%q right=%q errors=%v,%v", left, right, leftErr, rightErr)
	}
	for _, artifact := range []domain.Artifact{a, b} {
		if _, err := os.Stat(filepath.Join(snapshot, "artifacts", filepath.FromSlash(artifact.StoragePath))); err != nil {
			t.Fatal(err)
		}
	}
	// The live coordinator and configured roots now describe another installation.
	// A drill must read the backup's identity and objects from scratch exclusively.
	if err := store.SetKV(ctx, "backlog_v2_coordinator_identity", "different-production"); err != nil {
		t.Fatal(err)
	}
	manager.SubmissionRoot = filepath.Join(root, "unavailable-production-submissions")
	report, err := manager.RestoreDrill(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if report.CoordinatorID != "backup-origin" || report.SchemaVersion != storesqlite.CurrentSchemaVersion() || report.Counts["artifacts"] != 2 {
		t.Fatalf("drill report=%#v", report)
	}
	identity, ok, err := store.GetKV(ctx, "backlog_v2_coordinator_identity")
	if err != nil || !ok || identity != "different-production" {
		t.Fatalf("drill touched live identity: %q, %v", identity, err)
	}
	// The live ownership lock remains held, proving the online path did not take
	// ownership or stop the coordinator.
	if _, err := manager.Create(ctx, database, artifacts, filepath.Join(root, "stopped-snapshot")); err == nil {
		t.Fatal("live coordinator unexpectedly stopped owning its state")
	}
}

func TestCreateOnlineFailsClosedOnMissingOrCorruptReferencedObjects(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			database := filepath.Join(root, "state.db")
			artifacts := filepath.Join(root, "artifacts")
			store, err := sqlitetest.OpenMigrated(database)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			writer, err := sql.Open("sqlite", database)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			artifact := onlineTestArtifact(t, writer, artifacts, "test", "worker", []byte("retained"))
			object := filepath.Join(artifacts, filepath.FromSlash(artifact.StoragePath))
			switch mode {
			case "missing":
				err = os.Remove(object)
			case "corrupt":
				err = os.WriteFile(object, []byte("modified"), 0o600)
			case "symlink":
				err = os.Remove(object)
				if err == nil {
					err = os.Symlink(database, object)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			snapshot := filepath.Join(root, "backup")
			manager := Manager{Limits: Limits{MaxFiles: 100, MaxBytes: 16 << 20}}
			if _, err := manager.CreateOnline(context.Background(), database, artifacts, snapshot); err == nil {
				t.Fatal("published incomplete backup")
			}
			if _, err := os.Lstat(snapshot); !os.IsNotExist(err) {
				t.Fatalf("failed backup published destination: %v", err)
			}
			stages, err := filepath.Glob(filepath.Join(root, ".coordinator-online-backup-*"))
			if err != nil || len(stages) != 0 {
				t.Fatalf("staging not cleaned: %v,%v", stages, err)
			}
		})
	}
}

func onlineTestArtifact(t *testing.T, db *sql.DB, root, id, producer string, content []byte) domain.Artifact {
	t.Helper()
	digest := sha256.Sum256(content)
	hash := hex.EncodeToString(digest[:])
	relative := filepath.ToSlash(filepath.Join("objects", hash[:2], hash))
	object := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(object), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(object, content, 0o600); err != nil {
		t.Fatal(err)
	}
	artifact := domain.Artifact{ID: id, WorkflowRunID: "run-test", TaskID: "task-test", AttemptID: "attempt-test", Kind: domain.ArtifactOutput, Name: id, MediaType: "text/plain", Size: int64(len(content)), SHA256: hash, StoragePath: relative, Producer: producer}
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO coordinator_artifacts(id,workflow_run_id,task_id,attempt_id,sha256,record) VALUES(?,?,?,?,?,?)", id, artifact.WorkflowRunID, artifact.TaskID, artifact.AttemptID, hash, raw); err != nil {
		t.Fatal(err)
	}
	return artifact
}
