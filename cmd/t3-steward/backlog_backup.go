package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/iryzhkov/t3-steward/internal/backupsnapshot"
	"github.com/iryzhkov/t3-steward/internal/config"
)

// A backup is not a message. `backlog_v2.message_limits` bounds one submission
// or one artifact transfer, and deriving the snapshot bound from it made
// `backlog backup create` refuse on any coordinator that had done real work:
// the retained artifact tree grows with every run, nothing prunes it yet, and a
// thousand files is a busy week. The supported way to back the coordinator up
// therefore stopped working exactly on the installations that needed it, and
// the only remaining option was to copy the database by hand.
//
// These bounds exist to catch a runaway tree or a mis-pointed root, not to size
// the store, so they sit far above any plausible fleet.
const (
	backupSnapshotMaxFiles = 1 << 20
	backupSnapshotMaxBytes = 1 << 40
)

func backupSnapshotLimits() backupsnapshot.Limits {
	return backupsnapshot.Limits{MaxFiles: backupSnapshotMaxFiles, MaxBytes: backupSnapshotMaxBytes}
}

func runBacklogBackup(ctx context.Context, cfg config.Config, args []string) error {
	if len(args) != 2 {
		return errors.New("backup usage: backlog backup create|verify|restore <snapshot-directory>")
	}
	if cfg.BacklogV2.Mode != "coordinator" {
		return errors.New("backlog-v2 backup requires coordinator mode configuration")
	}
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	manager := backupsnapshot.Manager{Limits: backupSnapshotLimits()}
	var manifest backupsnapshot.Manifest
	switch args[0] {
	case "create":
		manifest, err = manager.Create(ctx, statePath, cfg.BacklogV2.Storage.Artifacts, args[1])
	case "verify":
		manifest, err = manager.Verify(ctx, args[1])
	case "restore":
		manifest, err = manager.Restore(ctx, args[1], statePath, cfg.BacklogV2.Storage.Artifacts)
	default:
		return fmt.Errorf("unknown backup command %q", args[0])
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(manifest)
}
