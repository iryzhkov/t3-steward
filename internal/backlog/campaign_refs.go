package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

// CampaignCommitRecordVersion identifies the provenance document a downstream
// task reads to resolve a commit by reference.
const CampaignCommitRecordVersion = "campaign-commit/v1"

// CommitProvenance is the durable answer to "where did this commit come from".
// It names the producing task, the base it started from and the repository the
// commit belongs to, so that a downstream task never has to search for it.
type CommitProvenance struct {
	Version       string    `json:"version"`
	WorkflowRunID string    `json:"workflowRunId"`
	TaskID        string    `json:"taskId"`
	Name          string    `json:"name"`
	Repository    string    `json:"repository"`
	Base          string    `json:"base"`
	Commit        string    `json:"commit"`
	Ref           string    `json:"ref"`
	CreatedAt     time.Time `json:"createdAt"`
	// Bundle binds the record to the Git bundle of Base..Commit the producer
	// retained, which is how a consumer on another worker obtains the commit.
	// It is absent for a commit equal to its base, which needs no bundle, for a
	// producer whose build could not make one, and when BundleOmitted says why
	// none was retained.
	Bundle        *CommitBundleRecord `json:"bundle,omitempty"`
	BundleOmitted string              `json:"bundleOmitted,omitempty"`
}

// Report renders the provenance for an operator or a log line.
func (p CommitProvenance) Report() string {
	return fmt.Sprintf(
		"commit %s at %s produced by task %s from base %s in repository %s",
		p.Commit, p.Ref, p.TaskID, p.Base, p.Repository,
	)
}

// PublishCommitRequest asks for one declared commit to be kept reachable.
type PublishCommitRequest struct {
	WorkflowRunID string
	TaskID        string
	Name          string
	Repository    string
	// WorkspaceDir is the producing task's own checkout, which is the only
	// place the commit is known to exist when it is published.
	WorkspaceDir string
	// Revision is resolved in that workspace; it defaults to HEAD.
	Revision string
	// Base is the commit the workspace was pinned to before the task ran.
	Base      string
	CreatedAt time.Time
}

// CampaignRefStore keeps campaign-scoped commits reachable for the campaign's
// lifetime. It is deliberately not the repository cache: the cache is refreshed
// with "remote update --prune", which deletes any ref the origin does not have,
// so a commit parked there survives only until the next task refreshes it. This
// store is never pruned and holds exactly what a downstream task depends on.
type CampaignRefStore struct {
	Root      string
	GitBinary string
	// MaxBundleBytes bounds a commit bundle this store makes or imports. Zero
	// is DefaultCommitBundleMaxBytes.
	MaxBundleBytes int64
}

// CampaignRef names the durable ref of one declared commit.
func CampaignRef(workflowRunID, taskID, name string) string {
	return "refs/campaigns/" + workflowRunID + "/" + taskID + "/" + name
}

// StagedCampaignRef names where one attempt's declared commit waits for the
// coordinator's review gate. It is never handed to a downstream task.
func StagedCampaignRef(workflowRunID, taskID, attemptID, name string) string {
	return "refs/campaign-staged/" + workflowRunID + "/" + taskID + "/" + attemptID + "/" + name
}

// resolveDeclaredCommit checks a publication request and resolves the commit
// it names in the producing workspace.
func (s CampaignRefStore) resolveDeclaredCommit(ctx context.Context, request PublishCommitRequest, log io.Writer) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if err := validateCommitTarget(request.WorkflowRunID, request.TaskID, request.Name); err != nil {
		return "", err
	}
	if request.WorkspaceDir == "" {
		return "", errors.New("publish campaign commit: producing workspace is required")
	}
	if request.Repository == "" {
		return "", errors.New("publish campaign commit: repository is required")
	}
	if !validGitObjectID(request.Base) {
		return "", fmt.Errorf("publish campaign commit: base %q is not a commit ID", request.Base)
	}
	revision := request.Revision
	if revision == "" {
		revision = "HEAD"
	}
	if err := validateGitRef(revision); revision != "HEAD" && err != nil {
		return "", fmt.Errorf("publish campaign commit revision: %w", err)
	}
	raw, err := runLoggedCommandOutputEnv(ctx, log, "", workspaceGitNoTransport, s.git(), "-C", request.WorkspaceDir,
		"rev-parse", "--verify", revision+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve declared commit %q: %w", request.Name, err)
	}
	commit := strings.TrimSpace(string(raw))
	if !validGitObjectID(commit) {
		return "", fmt.Errorf("resolve declared commit %q: Git returned invalid commit %q", request.Name, commit)
	}
	return commit, nil
}

// Stage keeps one attempt's declared commit reachable without making it the
// task's campaign output, and returns the provenance it will have once it is.
// A review-declared task's commit is staged: the worker cannot know whether
// the coordinator's review gate will accept it, and a published ref is
// permanent. Each attempt stages under its own ref, so a retry that produces a
// different commit is never refused by an earlier attempt's work.
func (s CampaignRefStore) Stage(ctx context.Context, request PublishCommitRequest, attemptID string, log io.Writer) (CommitProvenance, error) {
	if !safePathComponent(attemptID) {
		return CommitProvenance{}, fmt.Errorf("stage campaign commit: attempt ID %q is not a safe path component", attemptID)
	}
	commit, err := s.resolveDeclaredCommit(ctx, request, log)
	if err != nil {
		return CommitProvenance{}, err
	}
	staged := StagedCampaignRef(request.WorkflowRunID, request.TaskID, attemptID, request.Name)
	if err := validateGitRef(staged); err != nil {
		return CommitProvenance{}, fmt.Errorf("stage campaign commit: %w", err)
	}
	gitDir, err := s.open(ctx, log)
	if err != nil {
		return CommitProvenance{}, err
	}
	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return CommitProvenance{}, fmt.Errorf("lock campaign refs: %w", err)
	}
	defer lock.Close()
	createdAt := request.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	provenance := CommitProvenance{
		Version: CampaignCommitRecordVersion, WorkflowRunID: request.WorkflowRunID,
		TaskID: request.TaskID, Name: request.Name, Repository: request.Repository,
		Base: request.Base, Commit: commit, Ref: CampaignRef(request.WorkflowRunID, request.TaskID, request.Name),
		CreatedAt: createdAt.UTC(),
	}
	// The record is written first: release finds staged work by its records,
	// so a ref without one would never be released, while a record without
	// its ref is released cleanly and never promoted.
	if err := writeCommitRecord(s.stagedPath(request.WorkflowRunID, request.TaskID, attemptID, request.Name), provenance); err != nil {
		return CommitProvenance{}, err
	}
	// The staged ref belongs to this attempt alone and nobody has been told
	// what it means, so finalizing the attempt again replaces it.
	if err := s.copyCommit(ctx, gitDir, request.WorkspaceDir, commit, staged, true, log); err != nil {
		return CommitProvenance{}, fmt.Errorf("stage campaign ref %s: %w", staged, err)
	}
	return provenance, nil
}

// Publish makes the declared commit reachable under its campaign ref and
// returns its provenance. Publishing the same commit again is idempotent;
// publishing a different commit under a ref that already exists is refused,
// because a downstream task has already been told what that ref means.
func (s CampaignRefStore) Publish(ctx context.Context, request PublishCommitRequest, log io.Writer) (CommitProvenance, error) {
	commit, err := s.resolveDeclaredCommit(ctx, request, log)
	if err != nil {
		return CommitProvenance{}, err
	}
	gitDir, err := s.open(ctx, log)
	if err != nil {
		return CommitProvenance{}, err
	}
	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return CommitProvenance{}, fmt.Errorf("lock campaign refs: %w", err)
	}
	defer lock.Close()

	ref := CampaignRef(request.WorkflowRunID, request.TaskID, request.Name)
	if existing, found, err := s.head(ctx, gitDir, ref, log); err != nil {
		return CommitProvenance{}, err
	} else if found && existing != commit {
		return CommitProvenance{}, fmt.Errorf("campaign ref %s already names commit %s", ref, existing)
	}
	if err := s.copyCommit(ctx, gitDir, request.WorkspaceDir, commit, ref, false, log); err != nil {
		return CommitProvenance{}, fmt.Errorf("publish campaign ref %s: %w", ref, err)
	}
	createdAt := request.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	provenance := CommitProvenance{
		Version: CampaignCommitRecordVersion, WorkflowRunID: request.WorkflowRunID,
		TaskID: request.TaskID, Name: request.Name, Repository: request.Repository,
		Base: request.Base, Commit: commit, Ref: ref, CreatedAt: createdAt.UTC(),
	}
	if err := s.writeProvenance(provenance); err != nil {
		return CommitProvenance{}, err
	}
	return provenance, nil
}

// copyCommit copies a declared commit from the producing workspace into the
// store under ref, replacing what ref names only when force is set. The store
// fetches it, rather than the workspace pushing it, because a push runs in the
// workspace's repository: its hooks, its remote and URL configuration and any
// receive-pack command it names belong to the executor, and they would run
// during collection, after verification. A fetch runs the store's own
// configuration and only an upload-pack in the workspace's repository, which
// runs no hook and no command that repository configures, and fetches no
// missing object itself. Git can decline a ref update and still exit
// successfully, as it does for a shallow source, so the ref is read back.
func (s CampaignRefStore) copyCommit(ctx context.Context, gitDir, workspaceDir, commit, ref string, force bool, log io.Writer) error {
	raw, err := runLoggedCommandOutputEnv(ctx, log, "", workspaceGitNoTransport, s.git(), "-C", workspaceDir,
		"rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return fmt.Errorf("locate producing repository: %w", err)
	}
	source := strings.TrimSuffix(string(raw), "\n")
	if !filepath.IsAbs(source) {
		return fmt.Errorf("locate producing repository: Git returned %q", source)
	}
	refspec := commit + ":" + ref
	if force {
		refspec = "+" + refspec
	}
	if err := runLoggedCommand(ctx, log, "", s.git(), "--git-dir", gitDir,
		"fetch", "--no-tags", "--", source, refspec); err != nil {
		return err
	}
	if got, found, err := s.head(ctx, gitDir, ref, log); err != nil {
		return err
	} else if !found || got != commit {
		return fmt.Errorf("fetch left %s at %q, want %s", ref, got, commit)
	}
	return nil
}

// Resolve returns the provenance recorded for one declared commit.
func (s CampaignRefStore) Resolve(workflowRunID, taskID, name string) (CommitProvenance, error) {
	if err := s.validate(); err != nil {
		return CommitProvenance{}, err
	}
	if err := validateCommitTarget(workflowRunID, taskID, name); err != nil {
		return CommitProvenance{}, err
	}
	return s.readProvenance(s.provenancePath(workflowRunID, taskID, name))
}

// FetchInto makes a declared commit reachable in a consuming workspace under
// the same campaign ref. The consumer resolves it by that reference and never
// searches a repository cache for it.
//
// FetchInto never decides what the producer's campaign output is. A commit that
// is only staged, because the coordinator has not accepted the result that
// declared it, is fetched from its staging for the consumer to inspect, and the
// store's campaign ref is left absent. FetchAcceptedInto is the one path that
// publishes a staged commit.
func (s CampaignRefStore) FetchInto(ctx context.Context, workspaceDir string, provenance CommitProvenance, log io.Writer) error {
	return s.fetchInto(ctx, workspaceDir, provenance, false, log)
}

// FetchAcceptedInto is FetchInto for a consumer whose execution package says
// the coordinator accepted the producing result. A staged commit becomes the
// producer's campaign output here, and only here.
func (s CampaignRefStore) FetchAcceptedInto(ctx context.Context, workspaceDir string, provenance CommitProvenance, log io.Writer) error {
	return s.fetchInto(ctx, workspaceDir, provenance, true, log)
}

func (s CampaignRefStore) fetchInto(ctx context.Context, workspaceDir string, provenance CommitProvenance, accepted bool, log io.Writer) error {
	if err := s.validate(); err != nil {
		return err
	}
	if workspaceDir == "" {
		return errors.New("fetch campaign commit: consuming workspace is required")
	}
	if err := validateCommitTarget(provenance.WorkflowRunID, provenance.TaskID, provenance.Name); err != nil {
		return err
	}
	if !validGitObjectID(provenance.Commit) {
		return fmt.Errorf("fetch campaign commit: %q is not a commit ID", provenance.Commit)
	}
	ref := CampaignRef(provenance.WorkflowRunID, provenance.TaskID, provenance.Name)
	if provenance.Ref != "" && provenance.Ref != ref {
		return fmt.Errorf("campaign commit record names ref %q, want %q", provenance.Ref, ref)
	}
	gitDir, err := s.open(ctx, log)
	if err != nil {
		return err
	}
	source := ref
	if accepted {
		if err := s.promote(ctx, gitDir, provenance, log); err != nil {
			return err
		}
	} else if source, err = s.inspectionSource(ctx, gitDir, provenance, log); err != nil {
		return err
	}
	if err := runLoggedCommand(ctx, log, "", s.git(), "-C", workspaceDir,
		"fetch", "--no-tags", "--", gitDir, "+"+source+":"+ref); err != nil {
		return fmt.Errorf("fetch campaign ref %s: %w", ref, err)
	}
	raw, err := runLoggedCommandOutput(ctx, log, "", s.git(), "-C", workspaceDir, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return fmt.Errorf("verify campaign ref %s: %w", ref, err)
	}
	if got := strings.TrimSpace(string(raw)); got != provenance.Commit {
		return fmt.Errorf("campaign ref %s resolved to %s, want %s", ref, got, provenance.Commit)
	}
	return nil
}

// promote publishes a staged commit under its campaign ref the first time an
// accepted consumer fetches it. The worker cannot see the coordinator's review
// gate at finalization; a consumer's execution package carries that decision,
// and the caller passes it here only when the package says the producing result
// was accepted. A ref that already exists is left alone; the fetch then checks
// it names the commit the consumer was told about. A ref whose record a failed
// promotion never wrote gets that record now, so Resolve and ReleaseRun see it.
func (s CampaignRefStore) promote(ctx context.Context, gitDir string, provenance CommitProvenance, log io.Writer) error {
	ref := CampaignRef(provenance.WorkflowRunID, provenance.TaskID, provenance.Name)
	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return fmt.Errorf("lock campaign refs: %w", err)
	}
	defer lock.Close()
	existing, found, err := s.head(ctx, gitDir, ref, log)
	if err != nil {
		return err
	}
	if found {
		return s.restorePromotedRecord(provenance, existing)
	}
	staged, _, found, err := s.findStaged(provenance)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("campaign commit %s at %s was neither published nor staged in this store", ref, provenance.Commit)
	}
	// The staged ref keeps the commit in this store; the campaign ref is
	// created only if it still does not exist.
	if err := runLoggedCommand(ctx, log, "", s.git(), "--git-dir", gitDir,
		"update-ref", ref, staged.Commit, ""); err != nil {
		return fmt.Errorf("publish staged campaign ref %s: %w", ref, err)
	}
	return s.writeProvenance(staged)
}

// restorePromotedRecord writes the record of a campaign ref that names the
// accepted commit but has none, which is what a promotion that failed between
// its ref and its record leaves. A ref naming another commit is left to the
// fetch, which refuses it.
func (s CampaignRefStore) restorePromotedRecord(provenance CommitProvenance, existing string) error {
	if existing != provenance.Commit {
		return nil
	}
	if _, err := os.Lstat(s.provenancePath(provenance.WorkflowRunID, provenance.TaskID, provenance.Name)); !errors.Is(err, os.ErrNotExist) {
		return nil
	}
	staged, _, found, err := s.findStaged(provenance)
	if err != nil || !found {
		return err
	}
	return s.writeProvenance(staged)
}

// inspectionSource names the ref a consumer without acceptance fetches: the
// campaign ref once it names this commit, otherwise the staged ref of the
// attempt that declared this commit. A judge inspecting a rejected commit
// still receives it after another attempt's commit was published. Nothing is
// written.
func (s CampaignRefStore) inspectionSource(ctx context.Context, gitDir string, provenance CommitProvenance, log io.Writer) (string, error) {
	ref := CampaignRef(provenance.WorkflowRunID, provenance.TaskID, provenance.Name)
	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return "", fmt.Errorf("lock campaign refs: %w", err)
	}
	defer lock.Close()
	if existing, found, err := s.head(ctx, gitDir, ref, log); err != nil || (found && existing == provenance.Commit) {
		return ref, err
	}
	_, attemptID, found, err := s.findStaged(provenance)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("campaign commit %s at %s was neither published nor staged in this store", ref, provenance.Commit)
	}
	return StagedCampaignRef(provenance.WorkflowRunID, provenance.TaskID, attemptID, provenance.Name), nil
}

// findStaged returns the staging record of the commit a provenance names, and
// the attempt that staged it.
func (s CampaignRefStore) findStaged(provenance CommitProvenance) (CommitProvenance, string, bool, error) {
	stagedRecords, err := filepath.Glob(filepath.Join(s.Root, "staged", provenance.WorkflowRunID, provenance.TaskID, "*", provenance.Name+".json"))
	if err != nil {
		return CommitProvenance{}, "", false, fmt.Errorf("find staged campaign commit: %w", err)
	}
	for _, path := range stagedRecords {
		attemptID := filepath.Base(filepath.Dir(path))
		if !safePathComponent(attemptID) {
			continue
		}
		staged, readErr := s.readProvenance(path)
		if readErr != nil {
			return CommitProvenance{}, "", false, readErr
		}
		if staged.Commit != provenance.Commit || staged.Base != provenance.Base || staged.Repository != provenance.Repository {
			continue
		}
		return staged, attemptID, true, nil
	}
	return CommitProvenance{}, "", false, nil
}

// List reports every commit published for one workflow run, oldest first.
func (s CampaignRefStore) List(workflowRunID string) ([]CommitProvenance, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if !safePathComponent(workflowRunID) {
		return nil, fmt.Errorf("campaign commits: workflow run ID %q is not a safe path component", workflowRunID)
	}
	return s.listRecords(filepath.Join(s.Root, "provenance", workflowRunID))
}

func (s CampaignRefStore) listRecords(root string) ([]CommitProvenance, error) {
	var records []CommitProvenance
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return nil
		}
		record, readErr := s.readProvenance(path)
		if readErr != nil {
			return readErr
		}
		records = append(records, record)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list campaign commits: %w", err)
	}
	sort.Slice(records, func(i, j int) bool {
		if !records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].CreatedAt.Before(records[j].CreatedAt)
		}
		return records[i].Ref < records[j].Ref
	})
	return records, nil
}

// Runs reports every workflow run this store currently holds commits for,
// sorted. It is what lets a worker act on the coordinator's keep list: the
// difference between what it holds and what it was told to keep is what it may
// release.
func (s CampaignRefStore) Runs() ([]string, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	// A run whose every declared commit is still staged holds commits too.
	var runs []string
	for _, kind := range []string{"provenance", "staged"} {
		entries, err := os.ReadDir(filepath.Join(s.Root, kind))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("list campaign commit runs: %w", err)
		}
		for _, entry := range entries {
			if entry.IsDir() && safePathComponent(entry.Name()) && !slices.Contains(runs, entry.Name()) {
				runs = append(runs, entry.Name())
			}
		}
	}
	sort.Strings(runs)
	return runs, nil
}

// ReleaseRun drops every campaign ref of one workflow run. It is the end of the
// declared campaign lifetime, and nothing else removes these refs.
//
// Releasing is idempotent, and a run that declared no commit costs nothing: the
// provenance records are the list of what this run pinned, so an empty list
// means there is nothing to delete and the bare repository is not even opened.
// A second release of the same run therefore reaches the same early return,
// which is what lets the caller retry a failed release on the next cycle
// without having to remember which runs it already released.
func (s CampaignRefStore) ReleaseRun(ctx context.Context, workflowRunID string, log io.Writer) error {
	if err := s.validate(); err != nil {
		return err
	}
	if !safePathComponent(workflowRunID) {
		return fmt.Errorf("release campaign commits: workflow run ID %q is not a safe path component", workflowRunID)
	}
	// The unlocked look is only an early exit for a run that pinned nothing. It
	// decides nothing: a publication that lands after it would have its record
	// destroyed by the wholesale removal below and its ref left behind forever,
	// so the list this acts on is read again under the lock that publication
	// also takes.
	if holds, err := s.holdsRun(workflowRunID); err != nil || !holds {
		return err
	}
	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return fmt.Errorf("lock campaign refs: %w", err)
	}
	defer lock.Close()
	if holds, err := s.holdsRun(workflowRunID); err != nil || !holds {
		return err
	}
	records, err := s.List(workflowRunID)
	if err != nil {
		return err
	}
	staged, err := s.listRecords(filepath.Join(s.Root, "staged", workflowRunID))
	if err != nil {
		return fmt.Errorf("list staged campaign commits: %w", err)
	}
	gitDir, err := s.open(ctx, log)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := runLoggedCommand(ctx, log, "", s.git(), "--git-dir", gitDir,
			"update-ref", "-d", record.Ref); err != nil {
			return fmt.Errorf("release campaign ref %s: %w", record.Ref, err)
		}
	}
	if len(staged) != 0 {
		// Staged refs are named by attempt, which the records do not repeat,
		// so they are found under the run's staging namespace.
		names, err := runLoggedCommandOutput(ctx, log, "", s.git(), "--git-dir", gitDir,
			"for-each-ref", "--format=%(refname)", "refs/campaign-staged/"+workflowRunID+"/")
		if err != nil {
			return fmt.Errorf("list staged campaign refs: %w", err)
		}
		for _, name := range strings.Fields(string(names)) {
			if err := runLoggedCommand(ctx, log, "", s.git(), "--git-dir", gitDir,
				"update-ref", "-d", name); err != nil {
				return fmt.Errorf("release staged campaign ref %s: %w", name, err)
			}
		}
	}
	for _, kind := range []string{"provenance", "staged"} {
		dir := filepath.Join(s.Root, kind, workflowRunID)
		if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := removeIngestedTree(dir); err != nil {
			return fmt.Errorf("release campaign commit records: %w", err)
		}
	}
	return nil
}

// holdsRun reports whether the store has published or staged anything for one
// workflow run.
func (s CampaignRefStore) holdsRun(workflowRunID string) (bool, error) {
	for _, kind := range []string{"provenance", "staged"} {
		records, err := s.listRecords(filepath.Join(s.Root, kind, workflowRunID))
		if err != nil {
			return false, err
		}
		if len(records) != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s CampaignRefStore) validate() error {
	if s.Root == "" {
		return errors.New("campaign ref store root is required")
	}
	return nil
}

func (s CampaignRefStore) git() string {
	if s.GitBinary != "" {
		return s.GitBinary
	}
	return "git"
}

// open returns the bare repository that holds the campaign namespace, creating
// it on first use.
func (s CampaignRefStore) open(ctx context.Context, log io.Writer) (string, error) {
	if err := os.MkdirAll(s.Root, 0o700); err != nil {
		return "", fmt.Errorf("create campaign ref store: %w", err)
	}
	gitDir := filepath.Join(s.Root, "campaigns.git")
	info, err := os.Lstat(gitDir)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", fmt.Errorf("campaign ref store path %q is not a directory", gitDir)
		}
		return gitDir, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect campaign ref store: %w", err)
	}
	if err := runLoggedCommand(ctx, log, "", s.git(), "init", "--bare", "--quiet", "--", gitDir); err != nil {
		return "", fmt.Errorf("create campaign ref store: %w", err)
	}
	return gitDir, nil
}

func (s CampaignRefStore) head(ctx context.Context, gitDir, ref string, log io.Writer) (string, bool, error) {
	raw, err := runLoggedCommandOutput(ctx, log, "", s.git(), "--git-dir", gitDir, "rev-parse", "--verify", "--quiet", ref+"^{commit}")
	if err != nil {
		// With --quiet, a missing ref is exit 1 and no output, which is the
		// ordinary case on first publication. Anything else is a store that
		// could not answer, and reading that as "the ref is absent" would let
		// a republication quietly redefine a ref a successor already resolved.
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 && len(bytes.TrimSpace(raw)) == 0 {
			return "", false, nil
		}
		return "", false, fmt.Errorf("read campaign ref %s: %w", ref, err)
	}
	commit := strings.TrimSpace(string(raw))
	if !validGitObjectID(commit) {
		return "", false, fmt.Errorf("campaign ref %s resolved to invalid commit %q", ref, commit)
	}
	return commit, true, nil
}

func (s CampaignRefStore) provenancePath(workflowRunID, taskID, name string) string {
	return filepath.Join(s.Root, "provenance", workflowRunID, taskID, name+".json")
}

func (s CampaignRefStore) stagedPath(workflowRunID, taskID, attemptID, name string) string {
	return filepath.Join(s.Root, "staged", workflowRunID, taskID, attemptID, name+".json")
}

func (s CampaignRefStore) writeProvenance(provenance CommitProvenance) error {
	return writeCommitRecord(s.provenancePath(provenance.WorkflowRunID, provenance.TaskID, provenance.Name), provenance)
}

func writeCommitRecord(path string, provenance CommitProvenance) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create campaign commit record directory: %w", err)
	}
	raw, err := MarshalCommitProvenance(provenance)
	if err != nil {
		return err
	}
	staged := path + ".next"
	if err := os.WriteFile(staged, raw, 0o600); err != nil {
		return fmt.Errorf("stage campaign commit record: %w", err)
	}
	if err := os.Rename(staged, path); err != nil {
		_ = os.Remove(staged)
		return fmt.Errorf("publish campaign commit record: %w", err)
	}
	return nil
}

func (s CampaignRefStore) readProvenance(path string) (CommitProvenance, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return CommitProvenance{}, fmt.Errorf("read campaign commit record: %w", err)
	}
	return ParseCommitProvenance(raw)
}

// MarshalCommitProvenance renders the provenance document a downstream task
// reads. It is the artifact content of a declared commit output.
func MarshalCommitProvenance(provenance CommitProvenance) ([]byte, error) {
	provenance.Version = CampaignCommitRecordVersion
	raw, err := json.MarshalIndent(provenance, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode campaign commit record: %w", err)
	}
	return append(raw, '\n'), nil
}

// ParseCommitProvenance reads a provenance document and refuses anything that
// is not one, so that an ordinary dependency file is never mistaken for a
// commit reference.
func ParseCommitProvenance(raw []byte) (CommitProvenance, error) {
	var provenance CommitProvenance
	if err := json.Unmarshal(raw, &provenance); err != nil {
		return CommitProvenance{}, fmt.Errorf("decode campaign commit record: %w", err)
	}
	if provenance.Version != CampaignCommitRecordVersion {
		return CommitProvenance{}, fmt.Errorf("campaign commit record version %q is not supported", provenance.Version)
	}
	if err := validateCommitTarget(provenance.WorkflowRunID, provenance.TaskID, provenance.Name); err != nil {
		return CommitProvenance{}, err
	}
	if !validGitObjectID(provenance.Commit) || !validGitObjectID(provenance.Base) {
		return CommitProvenance{}, errors.New("campaign commit record needs a commit and a base")
	}
	if provenance.Repository == "" {
		return CommitProvenance{}, errors.New("campaign commit record needs its repository")
	}
	return provenance, nil
}

func validateCommitTarget(workflowRunID, taskID, name string) error {
	if !safePathComponent(workflowRunID) || !safePathComponent(taskID) || !safePathComponent(name) {
		return fmt.Errorf("campaign commit %q/%q/%q is not a safe reference", workflowRunID, taskID, name)
	}
	if err := validateGitRef(CampaignRef(workflowRunID, taskID, name)); err != nil {
		return fmt.Errorf("campaign commit reference: %w", err)
	}
	return nil
}
