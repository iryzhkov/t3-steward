package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"os"
	"path/filepath"
	"testing"
)

func TestTaskInputDigestMatchesIngestion(t *testing.T) {
	for _, withInput := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "input"}[withInput], func(t *testing.T) {
			var paths []string
			if withInput {
				source := filepath.Join(t.TempDir(), "evidence.md")
				if err := os.WriteFile(source, []byte("evidence"), 0600); err != nil {
					t.Fatal(err)
				}
				paths = []string{source}
			}
			snapshot, err := pinnedinput.SnapshotFiles(paths)
			if err != nil {
				t.Fatal(err)
			}
			bundle, err := writeTaskRunCampaign(taskRunCampaign{name: "digest-test", project: "test", fresh: true, route: taskRunRoute{Instance: "codex", Model: "sol"}, prompts: []taskRunPrompt{{name: "task", body: "review"}}, class: domain.TaskClassSurplus, maxTurns: 3, inputs: snapshot})
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(bundle)
			root := t.TempDir()
			defer func() {
				_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
					if err == nil && info.IsDir() {
						return os.Chmod(path, 0700)
					}
					return err
				})
				_ = os.RemoveAll(root)
			}()
			db, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			got, err := (backlog.BundleIngester{StorageRoot: root, Store: db}).Ingest(context.Background(), bundle)
			if err != nil {
				t.Fatal(err)
			}
			manifest := got.Records.Workflows[0].InputManifest
			if !withInput {
				if manifest != nil {
					t.Fatal("empty inputs should omit manifest")
				}
				return
			}
			if manifest == nil || manifest.Digest != snapshot.Manifest.Digest {
				t.Fatalf("digest mismatch: %+v versus %+v", manifest, snapshot.Manifest)
			}
		})
	}
}
