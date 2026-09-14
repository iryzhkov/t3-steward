package campaign

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// The packed bytes must be exactly what the coordinator accepts, and the
// digest reported before submission must be the digest the submission records.
// Predicting it here is only safe while it keeps matching the real one, which
// is what this test is for.
func TestPackedCampaignSubmitsAndReplays(t *testing.T) {
	root := campaignDir(t)
	bundle, err := Prepare(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	service := newSubmissionService(t)
	ctx := context.Background()
	accepted, err := service.SubmitArchive(ctx, backlog.ArchiveSubmission{
		IdempotencyKey: "campaign-submission",
		Archive:        bytes.NewReader(bundle.Archive),
	})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Record.Digest != bundle.ContentDigest {
		t.Fatalf("coordinator digest %q, campaign predicted %q", accepted.Record.Digest, bundle.ContentDigest)
	}
	if accepted.Record.WorkflowID == "" || accepted.Record.RunID == "" {
		t.Fatalf("submission record = %#v", accepted.Record)
	}

	// The same key with the same bytes is the replay the deterministic
	// packaging exists to guarantee.
	repeat, err := Prepare(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := service.SubmitArchive(ctx, backlog.ArchiveSubmission{
		IdempotencyKey: "campaign-submission",
		Archive:        bytes.NewReader(repeat.Archive),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replay || replayed.Record.RunID != accepted.Record.RunID ||
		replayed.Record.Digest != accepted.Record.Digest {
		t.Fatalf("replay = %#v, first = %#v", replayed.Record, accepted.Record)
	}
}

// Validation may be stricter than ingestion but never weaker. Every campaign
// the loader refuses, and that a plain tar can represent at all, is refused by
// the coordinator too.
func TestLoaderRefusesEverythingIngestionRefuses(t *testing.T) {
	service := newSubmissionService(t)
	for _, test := range negativeCases() {
		if !test.ingestionRefuses {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			reference := test.build(t, root)
			if _, err := Load(reference, DefaultLimits); err == nil {
				t.Fatal("the loader accepted the campaign")
			}
			_, err := service.SubmitArchive(context.Background(), backlog.ArchiveSubmission{
				IdempotencyKey: "negative-" + test.name,
				Archive:        bytes.NewReader(plainTar(t, root)),
			})
			if err == nil {
				t.Fatal("the coordinator accepted what the loader refused, so validation is weaker than ingestion")
			}
		})
	}
}

// plainTar packages a directory the way an author would before this package
// existed: every regular file, no links, no directory entries.
func plainTar(t *testing.T, root string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		header := &tar.Header{
			Name: filepath.ToSlash(relative),
			Mode: 0o600,
			Size: int64(len(content)),
		}
		if err := writer.WriteHeader(header); err != nil {
			return err
		}
		_, err = writer.Write(content)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func newSubmissionService(t *testing.T) *backlog.SubmissionService {
	t.Helper()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	storage := filepath.Join(t.TempDir(), "storage")
	// Ingestion makes what it stores read-only, so the tree has to be made
	// writable again before the test framework can remove it.
	t.Cleanup(func() {
		_ = filepath.WalkDir(storage, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
		_ = os.RemoveAll(storage)
	})
	return &backlog.SubmissionService{
		StorageRoot: storage,
		Store:       store,
		MaxBytes:    DefaultLimits.MaxBytes,
		MaxFiles:    DefaultLimits.MaxFiles,
	}
}
