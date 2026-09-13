package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/sessionarchive"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// This command is deliberately read-only; the daemon performs bounded effects.
func cmdUIArchive(g globalFlags, args []string) error {
	if len(args) != 1 || args[0] != "candidates" {
		return fmt.Errorf("usage: t3-steward ui-archive candidates [--config PATH]")
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	path, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(path)
	if err != nil {
		return err
	}
	defer store.Close()
	logger := newLogger(cfg.LogLevel)
	client, _, err := connect(cfg, logger)
	if err != nil {
		return err
	}
	control := t3control.New(client, logger, true)
	archiver := newUIArchiver(cfg, store, control, logger)
	ctx := context.Background()
	threads, err := control.ListThreads(ctx)
	if err != nil {
		return err
	}
	states, err := archiver.States(ctx)
	if err != nil {
		return err
	}
	candidates := []map[string]any{}
	now := time.Now()
	archived := 0
	for _, thread := range threads {
		if thread.ArchivedAt != nil {
			archived++
		}
		if !sessionarchive.Eligible(thread, states[thread.ID], now, archiver.Options) {
			continue
		}
		signature := thread.SettledAt.UTC().Format(time.RFC3339Nano)
		previous, _, err := store.GetKV(ctx, "ui_archive.settlement."+thread.ID)
		if err != nil {
			return err
		}
		if previous == signature {
			continue
		}
		candidates = append(candidates, map[string]any{"threadId": thread.ID, "settledAt": signature, "background": states[thread.ID].Background})
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"enabled": cfg.UIArchive.Enabled, "dryRun": cfg.UIArchive.DryRun, "threads": len(threads), "archived": archived, "candidates": candidates})
}
