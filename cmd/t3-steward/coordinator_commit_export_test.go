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
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// Exercise the same wrapper and dispatcher used by the coordinator, rather
// than the admin service directly: optional capabilities must survive wiring.
func TestCoordinatorLocalServiceDispatchesCommitExport(t *testing.T) {
	root, err := os.MkdirTemp("", "t3-export-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	store, err := sqlitetest.OpenMigrated(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	admin, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "admin.sock")
	listener, err := backlogadmin.ListenLocal(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	timeout := testtiming.Bound(10 * time.Second)
	server := &backlogadmin.LocalServer{
		Listener: listener, Service: coordinatorLocalService{admin: admin},
		AllowedUID: uint32(os.Getuid()), MaxRequestBytes: 1 << 20,
		MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		RequestTimeout: timeout, MaxConcurrent: 1,
	}
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("stop admin server: %v", err)
		}
	})
	client := backlogadmin.LocalClient{
		Path: path, RequestTimeout: timeout, MaxResponseBytes: 1 << 20,
		MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	}
	_, err = client.ExportCommit(ctx, backlogadmin.CommitExportRequest{
		RunID: "missing-run", TaskID: "task", Name: "implementation", Branch: "review",
	})
	// An empty store deliberately stops ExportCommit at run lookup. Its precise
	// error proves the request reached the service instead of failing the
	// dispatcher's optional-interface assertion as a malformed request.
	want := fmt.Sprintf("%v: run missing-run", backlogadmin.ErrNotFound)
	if err == nil || !strings.HasSuffix(err.Error(), ": "+want) {
		t.Fatalf("export error = %v, want %q", err, want)
	}
}
