// Package t3projects removes the T3 projects the steward itself created once
// nothing is left in them.
//
// A backlog task and a supervision activation each need a T3 project to open
// their thread in, and the worker provisions one whose workspace root is a
// directory the worker owns. Nothing removed them, so every project the fleet
// ever ran -- one per catalog project per host, one per supervision activation,
// and one for every identity a renamed project or a moved coordinator left
// behind -- stayed in the T3 sidebar for good, next to the handful of projects
// a person actually opens.
//
// The sweep deletes such a project when it holds no thread and its record has
// been untouched for the configured period. It owns no other decision: threads
// are the archive's, workspaces are the worker's, and this package never
// removes a directory.
package t3projects

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ActionSweep is the audit kind of one deleted project.
const ActionSweep = domain.ActionKind("project-cleanup")

// lastPassKey records when the last pass ran, so the cadence survives a
// restart and a pass is not repeated on every poll.
const lastPassKey = "project_cleanup.last_pass"

type Control interface {
	ListProjects(context.Context) ([]t3control.Project, error)
	DeleteProject(context.Context, string) error
}

type Store interface {
	GetKV(context.Context, string) (string, bool, error)
	SetKV(context.Context, string, string) error
	RecordAction(context.Context, domain.ActionRecord) error
}

type Options struct {
	// OwnedRoots are the directories whose projects this host created. A
	// project is a candidate only if its workspace root is inside one of them,
	// which is the whole of the ownership rule: these are the worker's own
	// state directories, and no project a person opened lives under them.
	OwnedRoots []string
	// After is how long a project's record must have been untouched.
	After time.Duration
	// Every is the shortest interval between passes.
	Every time.Duration
	// MaxPerPass bounds the deletions of one pass.
	MaxPerPass int
	DryRun     bool
}

type Sweeper struct {
	Options Options
	Control Control
	Store   Store
	Now     func() time.Time
	Logger  *slog.Logger
}

func (s *Sweeper) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Sweeper) log() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// Validate refuses an ownership rule that would select projects this host did
// not create. An empty root, a relative one or the filesystem root would make
// every project in T3 a candidate, which is the one mistake this package must
// not be able to make.
func (o Options) Validate() error {
	if o.After <= 0 || o.Every <= 0 || o.MaxPerPass < 1 {
		return errors.New("project cleanup: after, every and max per pass must be positive")
	}
	if len(o.OwnedRoots) == 0 {
		return errors.New("project cleanup: at least one owned root is required")
	}
	for _, root := range o.OwnedRoots {
		clean := filepath.Clean(root)
		if strings.TrimSpace(root) == "" || !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
			return fmt.Errorf("project cleanup: owned root %q must be an absolute path below the filesystem root", root)
		}
	}
	return nil
}

// Owned reports whether a project's workspace root is inside one of the roots
// this host provisions managed projects under.
//
// Containment is strict: a project whose root is exactly an owned root is not
// one of ours, because the worker always provisions below it. Paths are
// compared after cleaning and never resolved through symlinks -- this decides
// whether to delete a T3 record, not whether to touch a file, and a link that
// changes under it must not change which projects are ours.
func Owned(workspaceRoot string, ownedRoots []string) bool {
	if strings.TrimSpace(workspaceRoot) == "" {
		return false
	}
	candidate := filepath.Clean(workspaceRoot)
	for _, root := range ownedRoots {
		clean := filepath.Clean(root)
		if clean == "" || clean == "." || clean == string(filepath.Separator) {
			continue
		}
		if strings.HasPrefix(candidate, clean+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// Idle is when a project's record was last touched, and whether that is known.
//
// T3 updates a project only when its own metadata changes, so this is the age
// of the project record rather than of the work that ran in it. A record with
// no time at all is no evidence and is never swept.
func Idle(project t3control.Project) (time.Time, bool) {
	switch {
	case !project.UpdatedAt.IsZero():
		return project.UpdatedAt, true
	case !project.CreatedAt.IsZero():
		return project.CreatedAt, true
	default:
		return time.Time{}, false
	}
}

// Candidates are the projects this pass would delete, oldest record first.
//
// threads is every thread T3 still holds, archived ones included: a project
// that holds an archived thread is not empty, T3 refuses to delete it without
// force, and this package never sends force.
func Candidates(projects []t3control.Project, threads []domain.Thread, now time.Time, opts Options) []t3control.Project {
	occupied := map[string]bool{}
	for _, thread := range threads {
		occupied[thread.ProjectID] = true
	}
	var candidates []t3control.Project
	for _, project := range projects {
		if !Owned(project.WorkspaceRoot, opts.OwnedRoots) || occupied[project.ID] {
			continue
		}
		idle, known := Idle(project)
		if !known || now.Sub(idle) < opts.After {
			continue
		}
		candidates = append(candidates, project)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, _ := Idle(candidates[i])
		right, _ := Idle(candidates[j])
		if left.Equal(right) {
			return candidates[i].ID < candidates[j].ID
		}
		return left.Before(right)
	})
	return candidates
}

// Tick runs one pass when the cadence allows it.
func (s *Sweeper) Tick(ctx context.Context, threads []domain.Thread, _ []domain.BucketState) {
	due, err := s.due(ctx)
	if err != nil {
		s.log().Warn("project cleanup cannot read when it last ran", "err", err)
		return
	}
	if !due {
		return
	}
	deleted, err := s.Run(ctx, threads)
	// The pass is recorded as having run whatever it found, so a T3 that is
	// unreachable on this poll is retried on the next cadence rather than on
	// every poll until it answers.
	if setErr := s.Store.SetKV(ctx, lastPassKey, s.now().UTC().Format(time.RFC3339Nano)); setErr != nil {
		s.log().Warn("project cleanup cannot record that it ran", "err", setErr)
	}
	if err != nil {
		s.log().Warn("project cleanup pass incomplete", "deleted", deleted, "err", err)
		return
	}
	if deleted != 0 {
		s.log().Info("project cleanup finished", "deleted", deleted, "dry_run", s.Options.DryRun)
	}
}

func (s *Sweeper) due(ctx context.Context) (bool, error) {
	last, ok, err := s.Store.GetKV(ctx, lastPassKey)
	if err != nil || !ok {
		return err == nil, err
	}
	at, parseErr := time.Parse(time.RFC3339Nano, last)
	if parseErr != nil {
		return true, nil
	}
	return s.now().Sub(at) >= s.Options.Every, nil
}

// Run deletes this pass's candidates and returns how many it deleted.
//
// A refusal from T3 is reported against the project it belongs to and does not
// end the pass: the usual cause is a thread that appeared after the snapshot
// this pass read, which is one project's news and not the fleet's.
func (s *Sweeper) Run(ctx context.Context, threads []domain.Thread) (int, error) {
	if err := s.Options.Validate(); err != nil {
		return 0, err
	}
	projects, err := s.Control.ListProjects(ctx)
	if err != nil {
		return 0, fmt.Errorf("project cleanup: read the projects: %w", err)
	}
	now := s.now()
	candidates := Candidates(projects, threads, now, s.Options)
	if len(candidates) > s.Options.MaxPerPass {
		candidates = candidates[:s.Options.MaxPerPass]
	}
	deleted := 0
	var failures error
	for _, project := range candidates {
		idle, _ := Idle(project)
		detail := fmt.Sprintf("project=%s title=%q root=%s idle=%s",
			project.ID, project.Title, project.WorkspaceRoot, now.Sub(idle).Round(time.Second))
		record := domain.ActionRecord{At: now, Kind: ActionSweep, DryRun: s.Options.DryRun, Detail: detail}
		if !s.Options.DryRun {
			if err := s.Control.DeleteProject(ctx, project.ID); err != nil {
				failures = errors.Join(failures, fmt.Errorf("project %s: %w", project.ID, err))
				record.Err = err.Error()
				if recordErr := s.Store.RecordAction(ctx, record); recordErr != nil {
					return deleted, errors.Join(failures, recordErr)
				}
				continue
			}
		}
		if err := s.Store.RecordAction(ctx, record); err != nil {
			return deleted, errors.Join(failures, err)
		}
		s.log().Info("managed project swept", "project", project.ID, "title", project.Title,
			"root", project.WorkspaceRoot, "dry_run", s.Options.DryRun)
		deleted++
	}
	return deleted, failures
}

// OwnedRoots keeps the roots a host provisions managed projects under: every
// absolute path below the filesystem root, once each, in the order given.
//
// The caller decides what those roots are, because the watchdog and the worker
// are separate processes that learn their storage differently: one reads it
// from the configuration file and the other derives it from the home directory
// when its catalog comes from the private bootstrap. A project provisioned
// under either is this host's to clean up, and neither process can ask the
// other, so both are listed and anything unusable is dropped here rather than
// silently making every project in T3 a candidate.
func OwnedRoots(roots ...string) []string {
	var owned []string
	for _, root := range roots {
		if strings.TrimSpace(root) == "" {
			continue
		}
		clean := filepath.Clean(root)
		if !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
			continue
		}
		if !slices.Contains(owned, clean) {
			owned = append(owned, clean)
		}
	}
	return owned
}
