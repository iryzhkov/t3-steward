package backupsnapshot

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestInspectDatabaseRefusesPendingMigrations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE schema_version (version INTEGER NOT NULL); INSERT INTO schema_version VALUES (1);"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = inspectDatabase(context.Background(), path)
	if !errors.Is(err, ErrInvalidSnapshot) || !strings.Contains(err.Error(), "pending schema migrations") {
		t.Fatalf("error=%v", err)
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM schema_version").Scan(&count); err != nil || count != 1 {
		t.Fatalf("inspection mutated rows: %d %v", count, err)
	}
}
