package main

import (
	"log/slog"
	"os"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/t3api"
	"github.com/iryzhkov/t3-steward/internal/t3projects"
)

// projectCleanupHome is the home directory the derived worker storage root is
// built from; a test replaces it.
var projectCleanupHome = os.UserHomeDir

// newProjectSweeper builds the pass that removes this host's own empty T3
// projects, or returns nil with the reason when it cannot be trusted to.
//
// The ownership rule is the whole safety of this feature, so it is decided
// here, where the configuration is: the roots are the worker workspaces root
// this configuration names, the one a worker running from the private
// bootstrap derives from the home directory, and whatever project_cleanup.roots
// adds for a host that moved its storage. A configuration that yields no usable
// root disables the sweep rather than widening it.
func newProjectSweeper(cfg config.Config, store *sqlite.Store, client *t3api.Client, logger *slog.Logger) *t3projects.Sweeper {
	options := t3projects.Options{
		OwnedRoots: managedProjectRoots(cfg, logger),
		After:      cfg.ProjectCleanup.After.D(),
		Every:      cfg.ProjectCleanup.Every.D(),
		MaxPerPass: cfg.ProjectCleanup.MaxPerPass,
		// A watchdog in dry-run mode deletes nothing at all, and the sweep has
		// its own switch for the same reason the archive does: an operator who
		// wants to see the candidates first should not have to stop enforcing
		// quota to see them.
		DryRun: cfg.ProjectCleanup.DryRun || cfg.Policy.DryRun,
	}
	if err := options.Validate(); err != nil {
		logger.Warn("project cleanup is disabled", "err", err)
		return nil
	}
	return &t3projects.Sweeper{
		Options: options,
		// The adapter carries the same policy, so a dry run cannot delete a
		// project even if something asks it to.
		Control: t3control.New(client, logger, options.DryRun),
		Store:   store,
		Logger:  logger,
	}
}

// managedProjectRoots are the directories this host provisions steward
// projects under: the worker workspaces root the configuration names, the one
// a worker running from the private bootstrap derives from the home directory,
// and project_cleanup.roots. The project cleanup and the archive's shorter
// retention for steward threads both decide ownership from them.
func managedProjectRoots(cfg config.Config, logger *slog.Logger) []string {
	home, err := projectCleanupHome()
	if err != nil {
		// One of two sources of roots is missing, which is not fatal: a
		// configuration that names the workspaces root still has one.
		logger.Warn("cannot derive the worker storage root from the home directory", "err", err)
	}
	roots := append([]string{cfg.BacklogV2.Storage.Workspaces, config.WorkerWorkspacesRoot(home)}, cfg.ProjectCleanup.Roots...)
	return t3projects.OwnedRoots(roots...)
}
