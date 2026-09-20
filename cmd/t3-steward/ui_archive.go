package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/sessionarchive"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

func newUIArchiver(cfg config.Config, store *sqlite.Store, control *t3control.Control, logger *slog.Logger) *sessionarchive.Archiver {
	return &sessionarchive.Archiver{
		Options: sessionarchive.Options{BackgroundAfter: cfg.UIArchive.BackgroundAfter.D(), UserAfter: cfg.UIArchive.UserAfter.D(), MaxPerPass: cfg.UIArchive.MaxPerPass, DryRun: cfg.UIArchive.DryRun},
		Store:   store, Control: control, Logger: logger,
		States: func(ctx context.Context) (map[string]sessionarchive.State, error) {
			states, err := store.SessionArchiveStates(ctx)
			if err != nil {
				return nil, err
			}
			settings := []config.BacklogV2{cfg.BacklogV2}
			// UpKeeper's persistent worker has a separate bootstrap from the host
			// watchdog. Reload it read-only so its custody is never omitted.
			path := filepath.Join(filepath.Dir(cfg.Path), "persistent-worker.yaml")
			if _, err := os.Stat(path); err == nil {
				worker, err := config.LoadFile(path)
				if err != nil {
					return nil, err
				}
				settings = append(settings, worker.BacklogV2)
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			roots := map[string]bool{}
			// The journals of this host's own worker storage, which no
			// configuration file names when the worker runs from the private
			// bootstrap. Without them every thread the steward dispatched here
			// is classified as a person's session and waits a day to be hidden
			// rather than two hours.
			if home, err := os.UserHomeDir(); err == nil {
				for _, root := range workerruntime.HostJournalRoots(home) {
					if roots[root] {
						continue
					}
					roots[root] = true
					local, err := workerruntime.JournalArchiveStates(root)
					if err != nil {
						return nil, err
					}
					for thread, state := range local {
						combined := states[thread]
						combined.Background = combined.Background || state.Background
						if state.Busy != "" {
							combined.Busy = state.Busy
						}
						states[thread] = combined
					}
				}
			}
			for _, b := range settings {
				ids := map[string]bool{}
				for id := range b.Workers {
					ids[id] = true
				}
				if b.LocalWorker.ID != "" {
					ids[b.LocalWorker.ID] = true
				}
				for id := range ids {
					if b.Storage.Workspaces == "" {
						continue
					}
					_, _, root := workerruntime.WorkerRoots(b, id)
					if roots[root] {
						continue
					}
					roots[root] = true
					local, err := workerruntime.JournalArchiveStates(root)
					if err != nil {
						return nil, err
					}
					for thread, state := range local {
						combined := states[thread]
						combined.Background = combined.Background || state.Background
						if state.Busy != "" {
							combined.Busy = state.Busy
						}
						states[thread] = combined
					}
				}
			}
			return states, nil
		},
	}
}
