package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
}

// CampaignRef names the durable ref of one declared commit.
func CampaignRef(workflowRunID, taskID, name string) string {
	return "refs/campaigns/" + workflowRunID + "/" + taskID + "/" + name
}

// Publish makes the declared commit reachable under its campaign ref and
// returns its provenance. Publishing the same commit again is idempotent;
// publishing a different commit under a ref that already exists is refused,
// because a downstream task has already been told what that ref means.
func (s CampaignRefStore) Publish(ctx context.Context, request PublishCommitRequest, log io.Writer) (CommitProvenance, error) {
	if err := s.validate(); err != nil {
		return CommitProvenance{}, err
	}
	if err := validateCommitTarget(request.WorkflowRunID, request.TaskID, request.Name); err != nil {
		return CommitProvenance{}, err
	}
	if request.WorkspaceDir == "" {
		return CommitProvenance{}, errors.New("publish campaign commit: producing workspace is required")
	}
	if request.Repository == "" {
		return CommitProvenance{}, errors.New("publish campaign commit: repository is required")
	}
	if !validGitObjectID(request.Base) {
		return CommitProvenance{}, fmt.Errorf("publish campaign commit: base %q is not a commit ID", request.Base)
	}
	revision := request.Revision
	if revision == "" {
		revision = "HEAD"
	}
	if err := validateGitRef(revision); revision != "HEAD" && err != nil {
		return CommitProvenance{}, fmt.Errorf("publish campaign commit revision: %w", err)
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

	raw, err := runLoggedCommandOutput(ctx, log, "", s.git(), "-C", request.WorkspaceDir,
		"rev-parse", "--verify", revision+"^{commit}")
	if err != nil {
		return CommitProvenance{}, fmt.Errorf("resolve declared commit %q: %w", request.Name, err)
	}
	commit := strings.TrimSpace(string(raw))
	if !validGitObjectID(commit) {
		return CommitProvenance{}, fmt.Errorf("resolve declared commit %q: Git returned invalid commit %q", request.Name, commit)
	}
	ref := CampaignRef(request.WorkflowRunID, request.TaskID, request.Name)
	if existing, found, err := s.head(ctx, gitDir, ref, log); err != nil {
		return CommitProvenance{}, err
	} else if found && existing != commit {
		return CommitProvenance{}, fmt.Errorf("campaign ref %s already names commit %s", ref, existing)
	}
	if err := runLoggedCommand(ctx, log, "", s.git(), "-C", request.WorkspaceDir,
		"push", "--", gitDir, commit+":"+ref); err != nil {
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

// FetchInto makes a published commit reachable in a consuming workspace under
// the same campaign ref. The consumer resolves it by that reference and never
// searches a repository cache for it.
func (s CampaignRefStore) FetchInto(ctx context.Context, workspaceDir string, provenance CommitProvenance, log io.Writer) error {
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
	if err := runLoggedCommand(ctx, log, "", s.git(), "-C", workspaceDir,
		"fetch", "--no-tags", "--", gitDir, "+"+ref+":"+ref); err != nil {
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

// List reports every commit published for one workflow run, oldest first.
func (s CampaignRefStore) List(workflowRunID string) ([]CommitProvenance, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if !safePathComponent(workflowRunID) {
		return nil, fmt.Errorf("campaign commits: workflow run ID %q is not a safe path component", workflowRunID)
	}
	root := filepath.Join(s.Root, "provenance", workflowRunID)
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
	entries, err := os.ReadDir(filepath.Join(s.Root, "provenance"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("list campaign commit runs: %w", err)
	}
	var runs []string
	for _, entry := range entries {
		if entry.IsDir() && safePathComponent(entry.Name()) {
			runs = append(runs, entry.Name())
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
	records, err := s.List(workflowRunID)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	gitDir, err := s.open(ctx, log)
	if err != nil {
		return err
	}
	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return fmt.Errorf("lock campaign refs: %w", err)
	}
	defer lock.Close()
	for _, record := range records {
		if err := runLoggedCommand(ctx, log, "", s.git(), "--git-dir", gitDir,
			"update-ref", "-d", record.Ref); err != nil {
			return fmt.Errorf("release campaign ref %s: %w", record.Ref, err)
		}
	}
	if err := removeIngestedTree(filepath.Join(s.Root, "provenance", workflowRunID)); err != nil {
		return fmt.Errorf("release campaign commit records: %w", err)
	}
	return nil
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
		// A missing ref is the ordinary case on first publication.
		return "", false, nil
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

func (s CampaignRefStore) writeProvenance(provenance CommitProvenance) error {
	path := s.provenancePath(provenance.WorkflowRunID, provenance.TaskID, provenance.Name)
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
