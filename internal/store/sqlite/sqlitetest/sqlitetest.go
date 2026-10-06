// Package sqlitetest gives tests outside the sqlite package a migrated state
// database without paying for the migrations every time.
//
// Migrating an empty database runs every versioned schema migration through
// the pure Go SQLite engine, which costs about a second per database under the
// race detector. Most tests only need a migrated database as a fixture, so this
// package migrates one database per test process, keeps its bytes, and writes
// those bytes wherever a test asks for a new database. The copy is then opened
// by sqlite.OpenMigrated exactly as before, so the connection settings, the
// file mode and the idempotent migration pass are the production ones.
//
// Tests whose subject is migration itself, or the creation of a database file,
// keep calling sqlite.OpenMigrated directly.
package sqlitetest

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var template struct {
	once  sync.Once
	image []byte
	err   error
}

// Image returns the bytes of an empty database migrated by sqlite.OpenMigrated
// and closed, which checkpoints and removes its write-ahead log. The image is
// built once per process and must not be modified.
func Image() ([]byte, error) {
	template.once.Do(func() {
		template.image, template.err = buildImage()
	})
	return template.image, template.err
}

func buildImage() ([]byte, error) {
	dir, err := os.MkdirTemp("", "t3-steward-schema-template-")
	if err != nil {
		return nil, fmt.Errorf("schema template directory: %w", err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "template.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		return nil, fmt.Errorf("migrate schema template: %w", err)
	}
	if err := store.Close(); err != nil {
		return nil, fmt.Errorf("close schema template: %w", err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("schema template left %s behind after close: %v", suffix, err)
		}
	}
	image, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema template: %w", err)
	}
	return image, nil
}

// OpenMigrated has the signature and the result of sqlite.OpenMigrated. When
// path names no file yet it first writes the migrated template there, creating
// the parent directory as sqlite.OpenMigrated would; when path already exists,
// or is ":memory:", it only delegates. Reopening a test's database therefore
// behaves exactly as before.
func OpenMigrated(path string) (*sqlite.Store, error) {
	if err := Seed(path); err != nil {
		return nil, err
	}
	return sqlite.OpenMigrated(path)
}

// Seed writes the migrated template to path when no file exists there, so that
// code under test that opens path with sqlite.OpenMigrated finds an up-to-date
// schema. It leaves an existing file and ":memory:" untouched.
func Seed(path string) error {
	if path == "" || path == ":memory:" {
		return nil
	}
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	image, err := Image()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("seed migrated database: %w", err)
	}
	_, writeErr := f.Write(image)
	closeErr := f.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return fmt.Errorf("seed migrated database: %w", err)
	}
	return nil
}
