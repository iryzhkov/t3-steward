package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/archive"
	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const archiveUsage = `Usage: t3-steward archive <command>

Cold storage for finished threads (see archive: in the configuration).

Commands:
  candidates        Threads that would be archived now, and why others are not.
  run [--dry-run]   Archive the candidates now.
  list              Archived threads recorded on this host.
  restore <id> [DIR] Fetch a thread's bundle back and unpack it (default: ./<id>).
`

func newArchiver(cfg config.Config, store *sqlite.Store, control *t3control.Control, logger *slog.Logger, dataDir string) *archive.Archiver {
	return archive.New(archive.Options{
		After:           cfg.Archive.After.D(),
		Destination:     cfg.Archive.Destination,
		HostName:        localHostName(cfg),
		DataDir:         dataDir,
		TranscriptDirs:  cfg.Archive.TranscriptDirs,
		DeleteFromT3:    cfg.Archive.DeleteFromT3,
		RemoveLocal:     cfg.Archive.RemoveLocal,
		KeepTranscripts: cfg.Archive.KeepTranscripts.D(),
		At:              cfg.Archive.At,
		MaxPerRun:       cfg.Archive.MaxPerRun,
		DryRun:          cfg.Policy.DryRun,
		Logger:          logger,
	}, store, control)
}

func cmdArchive(g globalFlags, args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Print(archiveUsage)
		return nil
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	if cfg.Archive.Destination == "" && args[0] != "list" {
		return errors.New("archive.destination is not configured")
	}
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx := context.Background()
	logger := newLogger(cfg.LogLevel)
	switch args[0] {
	case "list":
		recs, err := store.ListArchives(ctx)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			fmt.Println("No archived threads recorded on this host.")
			return nil
		}
		for _, r := range recs {
			status := "ok"
			if r.Error != "" {
				status = "FAILED: " + r.Error
			}
			fmt.Printf("%s  %s  %-40.40s  %6.1f MB  t3-deleted=%v  %s\n", r.ArchivedAt.Local().Format("2006-01-02"), r.ThreadID, r.Title, float64(r.Bytes)/1e6, r.DeletedFromT3, status)
		}
		return nil
	case "candidates", "run":
		client, dataDir, err := connect(cfg, logger)
		if err != nil {
			return err
		}
		control := t3control.New(client, logger, cfg.Policy.DryRun)
		a := newArchiver(cfg, store, control, logger, dataDir)
		if args[0] == "candidates" {
			cands, skipped, err := a.Candidates(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("%d thread(s) would be archived (idle for more than %s):\n", len(cands), cfg.Archive.After.D())
			for _, t := range cands {
				fmt.Printf("  %s  %-50.50s  updated %s\n", t.ID, t.Title, t.UpdatedAt.Local().Format("2006-01-02 15:04"))
			}
			if len(skipped) > 0 {
				fmt.Printf("%d kept:\n", len(skipped))
				n := 0
				for id, why := range skipped {
					if n++; n > 15 {
						fmt.Printf("  ... and %d more\n", len(skipped)-15)
						break
					}
					fmt.Printf("  %s  %s\n", id, why)
				}
			}
			return nil
		}
		dry := len(args) > 1 && args[1] == "--dry-run"
		n, err := a.Run(ctx, dry)
		if err != nil {
			return err
		}
		fmt.Printf("archived %d thread(s)\n", n)
		if !dry {
			_ = store.SetKV(ctx, "archive.last_run", time.Now().UTC().Format(time.RFC3339))
		}
		return nil
	case "restore":
		if len(args) < 2 {
			return errors.New("restore needs a thread id")
		}
		return restoreBundle(ctx, cfg, store, args[1], argOr(args, 2, ""))
	default:
		return fmt.Errorf("unknown archive command %q", args[0])
	}
}

func argOr(args []string, i int, def string) string {
	if len(args) > i {
		return args[i]
	}
	return def
}

// restoreBundle fetches an archived bundle and unpacks it locally.
func restoreBundle(ctx context.Context, cfg config.Config, store *sqlite.Store, threadID, dir string) error {
	recs, err := store.ListArchives(ctx)
	if err != nil {
		return err
	}
	var rec *archive.Record
	for i := range recs {
		if recs[i].ThreadID == threadID && recs[i].Error == "" {
			rec = &recs[i]
		}
	}
	if rec == nil {
		return fmt.Errorf("no archive record for %s on this host", threadID)
	}
	if dir == "" {
		dir = threadID
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	local := filepath.Join(dir, threadID+".tar.gz")
	if i := strings.Index(rec.Destination, ":"); i > 0 && !strings.Contains(rec.Destination[:i], "/") {
		cmd := exec.CommandContext(ctx, "scp", "-q", "-o", "BatchMode=yes", rec.Destination, local)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("scp: %v: %s", err, strings.TrimSpace(string(out)))
		}
	} else {
		if out, err := exec.CommandContext(ctx, "cp", rec.Destination, local).CombinedOutput(); err != nil {
			return fmt.Errorf("copy: %v: %s", err, strings.TrimSpace(string(out)))
		}
	}
	if out, err := exec.CommandContext(ctx, "tar", "-xzf", local, "-C", dir).CombinedOutput(); err != nil {
		return fmt.Errorf("unpack: %v: %s", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("restored %s into %s (thread.json, provider-logs/, transcripts/)\n", threadID, dir)
	return nil
}
