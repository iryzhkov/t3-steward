package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// A reload replaces configuration-bound services under the same acquired
// coordinator authority. Existing assignment records and packages remain immutable.
func coordinatorConfigLoop(ctx context.Context, cfg config.Config, logger *slog.Logger, store *sqlite.Store, epoch int64) error {
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	defer signal.Stop(reload)
	watchdogCtx, stopWatchdog := context.WithCancel(ctx)
	watchdogDone := make(chan struct{})
	go func() { defer close(watchdogDone); runWatchdogAlongside(watchdogCtx, cfg, logger, store) }()
	defer func() { stopWatchdog(); <-watchdogDone }()
	var fallback *config.Config
	for {
		instanceCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		ready := make(chan struct{})
		go func() {
			done <- runCoordinatorConfiguration(instanceCtx, cfg, logger, store, epoch, func() { close(ready) })
		}()
		select {
		case <-ready:
			fallback = nil
		case err := <-done:
			stop()
			if fallback != nil {
				logger.Error("configuration activation failed; restoring prior configuration", "error", err)
				cfg = *fallback
				fallback = nil
				continue
			}
			return err
		case <-ctx.Done():
			stop()
			return <-done
		}
	selectLoop:
		for {
			select {
			case <-ctx.Done():
				stop()
				return <-done
			case err := <-done:
				stop()
				return err
			case <-reload:
				next, err := loadCoordinatorReload(ctx, cfg, store)
				if err != nil {
					logger.Warn("configuration reload rejected; retaining effective configuration", "error", err)
					continue
				}
				stop()
				if err := <-done; err != nil {
					logger.Warn("configuration services stopped for reload", "error", err)
				}
				prior := cfg
				fallback = &prior
				cfg = next
				logger.Info("activating coordinator configuration", "epoch", epoch)
				break selectLoop
			}
		}
	}
}

func coordinatorConfigurationDigest(settings config.BacklogV2) (string, error) {
	raw, err := json.Marshal(settings)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func loadCoordinatorReload(ctx context.Context, current config.Config, store *sqlite.Store) (config.Config, error) {
	if current.Path == "" {
		return current, errors.New("reload requires a configuration file")
	}
	if _, err := os.Stat(current.Path); err != nil {
		return current, err
	}
	next, err := config.LoadFile(current.Path)
	if err != nil {
		return current, err
	}
	if err = validateCoordinatorReload(current, next); err != nil {
		return current, err
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return current, err
	}
	for _, assignment := range records.Assignments {
		if assignment.State == domain.AssignmentCompleted || assignment.State == domain.AssignmentReleased {
			continue
		}
		before, err := workerruntime.BuildWorkerBinding(current.BacklogV2, assignment.WorkerID, time.Now())
		if err != nil {
			return current, err
		}
		after, err := workerruntime.BuildWorkerBinding(next.BacklogV2, assignment.WorkerID, time.Now())
		if err != nil || before.CatalogRevision != after.CatalogRevision {
			return current, fmt.Errorf("worker %s has retained assignment %s; drain and settle before changing its execution catalog", assignment.WorkerID, assignment.ID)
		}
	}
	return next, nil
}

func validateCoordinatorReload(current, next config.Config) error {
	oldOuter, newOuter := current, next
	oldOuter.BacklogV2 = config.BacklogV2{}
	newOuter.BacklogV2 = config.BacklogV2{}
	if !reflect.DeepEqual(oldOuter, newOuter) {
		return errors.New("reload only accepts backlog_v2 catalog and policy; host lifecycle settings require restart")
	}
	a, b := current.BacklogV2, next.BacklogV2
	if a.Mode != b.Mode || !reflect.DeepEqual(a.Coordinator, b.Coordinator) || !reflect.DeepEqual(a.LocalWorker, b.LocalWorker) || !reflect.DeepEqual(a.Storage, b.Storage) {
		return errors.New("coordinator identity, epochs and storage are lifecycle operations")
	}
	for id, old := range a.Workers {
		if newer, ok := b.Workers[id]; ok && old.Epoch != newer.Epoch {
			return errors.New("worker epoch changes require explicit custody recovery")
		}
	}
	targets := map[string]string{}
	for name, project := range b.Projects {
		targets[name] = project.T3Project
	}
	if _, err := backlog.LegacyProjectAliases(targets); err != nil {
		return err
	}
	for id := range b.Workers {
		if _, err := workerruntime.BuildWorkerBinding(b, id, time.Now()); err != nil {
			return err
		}
	}
	return nil
}
