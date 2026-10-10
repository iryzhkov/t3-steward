package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backupsnapshot"
	"github.com/iryzhkov/t3-steward/internal/config"
)

const coordinatorBackupUsage = `Usage:
  t3-steward coordinator backup --config PATH --out DIR
  t3-steward coordinator backup verify DIR [--restore-drill]

Operator-only local maintenance. Creation requires an explicit operator-controlled
configuration in coordinator mode. It reads the live SQLite database consistently
without stopping the coordinator, copies retained artifacts, and atomically
publishes a checksummed manifest into an absent destination. Relative paths are
resolved against the current directory; ~ and ~/ expand to the current home.
No remote backup operation is exposed through coordinator-exchange.

verify checks digests, schema and integrity without reading production configuration.
--restore-drill additionally restores into a temporary scratch directory, accepts
the snapshot's own coordinator identity, opens it read-only and reports counts.
Scratch state is removed on completion. Commands always print JSON.
`

func cmdCoordinatorBackup(g globalFlags, args []string) error {
	manager := backupsnapshot.Manager{Limits: backupSnapshotLimits()}
	// Verification intentionally runs before configuration loading: a scratch
	// drill must not inherit or validate this host's production fleet identity.
	if len(args) > 0 && args[0] == "verify" {
		drill := false
		path := ""
		for _, arg := range args[1:] {
			if arg == "--restore-drill" {
				if drill {
					return errors.New("duplicate --restore-drill")
				}
				drill = true
			} else if strings.HasPrefix(arg, "-") || path != "" {
				return errors.New("usage: coordinator backup verify DIR [--restore-drill]")
			} else {
				path = arg
			}
		}
		if path == "" {
			return errors.New("usage: coordinator backup verify DIR [--restore-drill]")
		}
		path, err := maintenancePath(path)
		if err != nil {
			return err
		}
		if drill {
			report, err := manager.RestoreDrill(context.Background(), path)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(report)
		}
		manifest, err := manager.Verify(context.Background(), path)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(manifest)
	}
	fs := flag.NewFlagSet("coordinator backup", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	out := fs.String("out", "", "snapshot destination")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *out == "" {
		return errors.New("usage: coordinator backup --config PATH --out DIR")
	}
	if !g.configExplicit {
		return errors.New("coordinator backup requires an explicit operator-controlled --config PATH")
	}
	if g.dryRun || g.noDryRun {
		return errors.New("coordinator backup does not accept dry-run overrides")
	}
	cfg, err := config.LoadFile(g.configPath)
	if err != nil {
		return err
	}
	return runCoordinatorBackup(context.Background(), cfg, os.Stdout, *out)
}

func runCoordinatorBackup(ctx context.Context, cfg config.Config, out io.Writer, destination string) error {
	if cfg.BacklogV2.Mode != "coordinator" || cfg.BacklogV2.CoordinatorClient.Configured() {
		return errors.New("coordinator backup requires local coordinator mode without a remote coordinator client")
	}
	destination, err := maintenancePath(destination)
	if err != nil {
		return err
	}
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	statePath, err = maintenancePath(statePath)
	if err != nil {
		return err
	}
	artifacts, err := maintenancePath(cfg.BacklogV2.Storage.Artifacts)
	if err != nil {
		return err
	}
	bundles := ""
	if cfg.BacklogV2.Storage.Bundles != "" {
		bundles, err = maintenancePath(cfg.BacklogV2.Storage.Bundles)
		if err != nil {
			return err
		}
	}
	manager := backupsnapshot.Manager{Limits: backupSnapshotLimits(), SubmissionRoot: bundles}
	manifest, err := manager.CreateOnline(ctx, statePath, artifacts, destination)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(manifest)
}

func maintenancePath(path string) (string, error) {
	if path == "" {
		return "", errors.New("maintenance path is empty; supply a directory or an absolute path")
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = filepath.Join(home, strings.TrimPrefix(strings.TrimPrefix(path, "~"), "/"))
	} else if strings.HasPrefix(path, "~") {
		return "", fmt.Errorf("cannot expand %q; use ~/ for your home or supply an absolute path", path)
	}
	return filepath.Abs(path)
}
