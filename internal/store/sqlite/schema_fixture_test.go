package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

// Only immutable bytes survive initialization. The migrated database is
// checkpointed and closed before reading, then removed before publishing bytes.
// Every caller gets a new private file and a new connection, never a shared Store.
var emptySchemaFixture struct {
	once  sync.Once
	image string
	err   error
}

func checkpointFixture(s *Store) error {
	var busy, log, checkpointed int
	if err := s.db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &checkpointed); err != nil {
		return err
	}
	if busy != 0 || log != checkpointed {
		return fmt.Errorf("incomplete fixture checkpoint: %d/%d/%d", busy, log, checkpointed)
	}
	return nil
}

func fixtureSchema(t testing.TB) string {
	t.Helper()
	image, err := migratedSchemaImage()
	if err != nil {
		t.Fatal(err)
	}
	return image
}

// migratedSchemaImage builds the template once per test process. It does not
// take a testing.TB so that openMigratedFixture keeps OpenMigrated's signature.
func migratedSchemaImage() (string, error) {
	emptySchemaFixture.once.Do(func() {
		dir, err := os.MkdirTemp("", "t3-steward-schema-template-")
		if err != nil {
			emptySchemaFixture.err = err
			return
		}
		defer os.RemoveAll(dir)
		path := filepath.Join(dir, "template.db")
		s, err := OpenMigrated(path)
		if err != nil {
			emptySchemaFixture.err = err
			return
		}
		checkpointErr := checkpointFixture(s)
		closeErr := s.Close()
		if checkpointErr != nil {
			emptySchemaFixture.err = checkpointErr
			return
		}
		if closeErr != nil {
			emptySchemaFixture.err = closeErr
			return
		}
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
				emptySchemaFixture.err = fmt.Errorf("template sidecar %s: %v", suffix, err)
				return
			}
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			emptySchemaFixture.err = err
			return
		}
		if err := os.Remove(path); err != nil {
			emptySchemaFixture.err = err
			return
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			emptySchemaFixture.err = fmt.Errorf("template cleanup: %v", err)
			return
		}
		emptySchemaFixture.image = string(raw)
	})
	return emptySchemaFixture.image, emptySchemaFixture.err
}

// openMigratedFixture has OpenMigrated's signature and result for tests whose
// subject is not migration or database creation. A missing file is first
// written from the migrated template, about thirty times cheaper than migrating
// under the race detector; OpenMigrated then opens it with the production
// connection settings and its idempotent migration pass. An existing file and
// ":memory:" go straight to OpenMigrated, so reopening behaves as before.
func openMigratedFixture(path string) (*Store, error) {
	if path != ":memory:" {
		if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
			image, err := migratedSchemaImage()
			if err != nil {
				return nil, err
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return nil, err
			}
			if err := os.WriteFile(path, []byte(image), 0o600); err != nil {
				return nil, err
			}
		}
	}
	return OpenMigrated(path)
}

func copyFixtureSchema(t testing.TB) string {
	t.Helper()
	image := fixtureSchema(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "state.db")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.WriteString(image)
	closeErr := f.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return path
}

func openSchemaFixture(t testing.TB) *Store {
	t.Helper()
	s, err := Open(copyFixtureSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	// Match OpenMigrated's connection setting without replaying migrations.
	if _, err := s.db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		t.Fatal(err)
	}
	return s
}

// Real migrations remain measured and are not replaced in migration tests.
func BenchmarkEmptySchemaMigration(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s, err := OpenMigrated(filepath.Join(b.TempDir(), "state.db"))
		if err != nil {
			b.Fatal(err)
		}
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEmptySchemaFixture(b *testing.B) {
	fixtureSchema(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := Open(copyFixtureSchema(b))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := s.db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
			b.Fatal(err)
		}
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func fixtureSchemaRows(t *testing.T, s *Store) []string {
	t.Helper()
	rows, err := s.db.Query("SELECT type,name,tbl_name,coalesce(sql,'') FROM sqlite_schema ORDER BY type,name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var kind, name, table, sql string
		if err := rows.Scan(&kind, &name, &table, &sql); err != nil {
			t.Fatal(err)
		}
		result = append(result, kind+"|"+name+"|"+table+"|"+sql)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestEmptySchemaFixtureParityIsolationAndCleanup(t *testing.T) {
	image := fixtureSchema(t)
	digest := sha256.Sum256([]byte(image))
	fresh, err := OpenMigrated(filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Error(err)
		}
	})
	schema := fixtureSchemaRows(t, fresh)
	version, err := fresh.SchemaVersion(context.Background())
	if err != nil || version != currentSchemaVersion {
		t.Fatalf("fresh version=%d error=%v", version, err)
	}
	if err := checkpointFixture(fresh); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(fresh.path)
	if err != nil || !bytes.Equal(raw, []byte(image)) {
		t.Fatalf("fresh/template byte parity: %v", err)
	}
	var mu sync.Mutex
	var dirs []string
	// Parent cleanup runs after every child's Store close and TempDir removal.
	t.Cleanup(func() {
		for _, dir := range dirs {
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Errorf("fixture cleanup %s: %v", dir, err)
			}
		}
		if sha256.Sum256([]byte(fixtureSchema(t))) != digest {
			t.Error("immutable template changed")
		}
	})
	for i := range 8 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			path := copyFixtureSchema(t)
			mu.Lock()
			dirs = append(dirs, filepath.Dir(path))
			mu.Unlock()
			raw, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(raw, []byte(image)) {
				t.Fatalf("copy byte parity: %v", err)
			}
			for _, entry := range []struct {
				path string
				mode os.FileMode
			}{{filepath.Dir(path), 0o700}, {path, 0o600}} {
				info, err := os.Stat(entry.path)
				if err != nil || info.Mode().Perm() != entry.mode {
					t.Fatalf("private mode %s: %v", entry.path, err)
				}
			}
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if got, err := s.SchemaVersion(context.Background()); err != nil || got != version {
				t.Fatalf("copy version=%d error=%v", got, err)
			}
			if !reflect.DeepEqual(fixtureSchemaRows(t, s), schema) {
				t.Fatal("copy schema differs")
			}
			var journal string
			if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
				t.Fatalf("journal=%s error=%v", journal, err)
			}
			if _, err := s.db.Exec("INSERT INTO kv(key,value) VALUES('isolated',?)", fmt.Sprint(i)); err != nil {
				t.Fatal(err)
			}
			for _, suffix := range []string{"-wal", "-shm"} {
				info, err := os.Stat(path + suffix)
				if err != nil || info.Mode().Perm() != 0o600 {
					t.Fatalf("private sidecar %s: %v", suffix, err)
				}
			}
			if _, err := os.Stat(path + "-journal"); !os.IsNotExist(err) {
				t.Fatalf("unexpected rollback journal: %v", err)
			}
			var count int
			var value string
			if err := s.db.QueryRow("SELECT count(*),value FROM kv WHERE key='isolated'").Scan(&count, &value); err != nil || count != 1 || value != fmt.Sprint(i) {
				t.Fatalf("independent mutation count=%d value=%s error=%v", count, value, err)
			}
			if err := s.IntegrityCheck(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
