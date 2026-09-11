package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/iryzhkov/t3-steward/internal/backupsnapshot"
	"github.com/iryzhkov/t3-steward/internal/config"
)

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
	limits := cfg.BacklogV2.MessageLimits
	maxBytes := limits.MaxArtifactBytes
	if limits.MaxFiles > 0 && maxBytes <= math.MaxInt64/int64(limits.MaxFiles) {
		maxBytes *= int64(limits.MaxFiles)
	} else {
		maxBytes = math.MaxInt64
	}
	manager := backupsnapshot.Manager{Limits: backupsnapshot.Limits{MaxFiles: limits.MaxFiles + 1, MaxBytes: maxBytes}}
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
