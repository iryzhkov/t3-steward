package backlog

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A declared commit is published into the campaign ref store of the worker that
// produced it, and a consumer placed on another worker has its own store, which
// has never heard of it. The commit crosses between them as a Git bundle of
// base..commit that the producer retains as an ordinary artifact of its attempt,
// so it travels to the coordinator with the attempt's other results and from
// the coordinator to the consuming worker with the consumer's other inputs. No
// new channel is involved, and the bundle lives exactly as long as the
// provenance record it is bound to, because retention prunes them together and
// the release reconciler releases the refs when the record is gone.

// DefaultCommitBundleMaxBytes bounds a commit bundle when the worker's
// configuration does not. A declared commit is a review-sized change, and a
// bundle carries only what the consumer's repository cache does not already
// hold, so a bundle near this size is a mistake rather than a commit.
const DefaultCommitBundleMaxBytes int64 = 64 << 20

// CommitBundleMediaType is the media type a retained commit bundle carries.
const CommitBundleMediaType = "application/x-git-bundle"

// A provenance record whose producer retained no bundle says why with one of
// these fixed reason codes, never with free text. The record travels in the
// producer's result upload, which has one total limit, so what it adds must be
// small, bounded and known before the record is written; a consumer refused
// for it is told what the code means by describeBundleOmission.
const (
	// BundleOmittedSizeLimit: the bundle, or the provenance record with the
	// bundle bound to it, exceeded the largest commit bundle the producing
	// worker accepts or the largest artifact its result upload accepts.
	BundleOmittedSizeLimit = "bundle-omitted:size-limit"
	// BundleOmittedAggregateLimit: the bundle did not fit the result upload's
	// total limit together with the attempt's other results.
	BundleOmittedAggregateLimit = "bundle-omitted:aggregate-limit"
	// BundleOmittedNotDescendant: the commit does not descend from its base, so
	// there is no base..commit to bundle.
	BundleOmittedNotDescendant = "bundle-omitted:not-descendant"
	// BundleOmittedStaged: the commit is a review-gated task's staged work,
	// which an earlier rc.117 integration build kept only on the worker that
	// staged it. Staged work is bundled now, so no producer records it; a
	// consumer can still be handed such a record and is told what it means.
	BundleOmittedStaged = "bundle-omitted:staged"
)

// describeBundleOmission explains a recorded omission reason code.
func describeBundleOmission(code string) string {
	switch code {
	case BundleOmittedSizeLimit:
		return "its bundle exceeded the largest commit bundle the producing worker or the artifact transport accepts " +
			"(storage.campaign_commit_bundle_max_bytes, or the per-artifact limit of the package and of the producer's result upload)"
	case BundleOmittedAggregateLimit:
		return "its bundle did not fit within the total limit of its result upload together with the producing attempt's other results"
	case BundleOmittedNotDescendant:
		return "the commit does not descend from its base, so no bundle of base..commit exists"
	case BundleOmittedStaged:
		return "the commit was staged by a task that declares review, on a build that retained no bundle of staged work, " +
			"so it can be consumed only on the worker that staged it; run the consumer on that worker"
	default:
		const maxShown = 256
		if len(code) > maxShown {
			code = code[:maxShown] + "..."
		}
		return fmt.Sprintf("its producer recorded the unrecognized reason %q", code)
	}
}

// CommitBundleRecord binds a provenance record to the bundle retained with it.
type CommitBundleRecord struct {
	// Artifact is the bundle's artifact name within the producing attempt.
	Artifact string `json:"artifact"`
	SHA256   string `json:"sha256"`
	Size     int64  `json:"size"`
}

// CommitBundleArtifactName is the artifact name of the bundle retained for one
// declared commit. It is a git-state artifact, never an output, so it is never
// materialized into a consumer's dependency directory as if the task wrote it.
func CommitBundleArtifactName(name string) string {
	return "git/campaign-commits/" + name + ".bundle"
}

// IsCommitBundleOf reports whether an artifact name is the bundle of one of the
// commits the task declares.
func IsCommitBundleOf(task domain.Task, name string) bool {
	for _, output := range task.Outputs {
		if output.Commit != nil && CommitBundleArtifactName(output.Name) == name {
			return true
		}
	}
	return false
}

// CommitBundleDelivery is a commit bundle as an execution package delivered it
// to the consuming worker. Open is called only when the worker's own store does
// not already hold the commit. Omitted, when set, is the coordinator's reason
// for not delivering a bundle it retains, and nothing can be opened.
type CommitBundleDelivery struct {
	SHA256  string
	Size    int64
	Open    func(context.Context) (io.ReadCloser, error)
	Omitted string
}

func (s CampaignRefStore) maxBundleBytes() int64 {
	if s.MaxBundleBytes > 0 {
		return s.MaxBundleBytes
	}
	return DefaultCommitBundleMaxBytes
}

// commitBundleCandidate is a verified bundle made for one published commit and
// not yet captured. Whether it is retained depends on what else the attempt's
// result upload carries, so it waits in its private temporary directory until
// that is known.
type commitBundleCandidate struct {
	path   string
	size   int64
	sha256 string
}

// discard removes the candidate's temporary directory. It is safe on nil.
func (c *commitBundleCandidate) discard() {
	if c != nil {
		_ = os.RemoveAll(filepath.Dir(c.path))
	}
}

// publishedCommit is one declared commit the finalizer published, with the
// bundle made for it, if any, until the bundle is captured or left out.
type publishedCommit struct {
	declaration domain.ArtifactDeclaration
	provenance  CommitProvenance
	bundle      *commitBundleCandidate
	// bundleID and recordID are the artifact identities of the bundle, if it
	// is retained, and of the provenance record.
	bundleID, recordID string
}

// makeCommitBundle makes the bundle of one published commit and returns the
// provenance bound to it.
//
// A bundle that cannot be retained for a reason that is a property of the
// commit, such as its size, does not fail the producer: a consumer on the same
// worker never needs it. The reason is recorded in the provenance instead, so
// that a consumer elsewhere is refused with it rather than with a missing ref.
func (f AttemptFinalizer) makeCommitBundle(
	ctx context.Context,
	request AttemptFinalization,
	provenance CommitProvenance,
) (CommitProvenance, *commitBundleCandidate, error) {
	limit := f.CampaignRefs.maxBundleBytes()
	if request.CommitBundleLimit > 0 && request.CommitBundleLimit < limit {
		// The bundle travels as an artifact and cannot exceed what the
		// artifact transport accepts.
		limit = request.CommitBundleLimit
	}
	path, omitted, err := f.CampaignRefs.createBundle(ctx, provenance, limit, nil)
	if err != nil {
		return CommitProvenance{}, nil, err
	}
	if path == "" {
		provenance.BundleOmitted = omitted
		return provenance, nil, nil
	}
	candidate := &commitBundleCandidate{path: path}
	input, err := os.Open(path)
	if err != nil {
		candidate.discard()
		return CommitProvenance{}, nil, fmt.Errorf("open commit bundle: %w", err)
	}
	defer input.Close()
	hash := sha256.New()
	if candidate.size, err = io.Copy(hash, input); err != nil {
		candidate.discard()
		return CommitProvenance{}, nil, fmt.Errorf("read commit bundle: %w", err)
	}
	candidate.sha256 = fmt.Sprintf("%x", hash.Sum(nil))
	return candidate.bind(provenance), candidate, nil
}

// bind returns the provenance bound to this bundle.
func (c *commitBundleCandidate) bind(provenance CommitProvenance) CommitProvenance {
	provenance.Bundle = &CommitBundleRecord{Artifact: CommitBundleArtifactName(provenance.Name), SHA256: c.sha256, Size: c.size}
	provenance.BundleOmitted = ""
	return provenance
}

// admitCommitBundles decides which bundle metadata the attempt's result
// carries. candidate lists every artifact the result would capture with the
// records as they currently read, and admitResult is the upload's own
// validation of it, with every limit the upload enforces, per artifact and in
// total. Nothing here repeats those limits.
//
// Bundle bookkeeping must never fail a producer whose result is admitted
// without it, so only admitted results are kept, tried in a fixed order:
//
//  1. Every bundle, and the omission code of each commit without one.
//  2. Otherwise, starting from every record as it would be written with bundle
//     generation disabled, with no bundle and no omission: each bundle in
//     declaration order, then the omission code of each commit whose bundle
//     was not admitted, each kept only if the result is still admitted.
//
// A commit for which not even its omission code is admitted keeps the bare
// record, so its consumer on another worker is refused without a reason
// rather than its producer failing. When not even the bare result is admitted
// the bare result is what is captured, and the producer fails exactly as it
// would with bundle generation disabled.
func admitCommitBundles(published []publishedCommit, candidate func() ([]domain.Artifact, error), admitResult func([]domain.Artifact) error) error {
	var refusal error
	admitted := func() (bool, error) {
		artifacts, err := candidate()
		if err != nil {
			return false, err
		}
		refusal = admitResult(artifacts)
		return refusal == nil, nil
	}
	if ok, err := admitted(); err != nil || ok {
		return err
	}
	// The reason each commit would record if its bundle is not retained. A
	// commit with neither a bundle nor a reason, such as one equal to its base,
	// has no metadata to admit.
	reasons := make([]string, len(published))
	for index := range published {
		commit := &published[index]
		reasons[index] = commit.provenance.BundleOmitted
		commit.provenance.Bundle, commit.provenance.BundleOmitted = nil, ""
	}
	retained := make([]bool, len(published))
	for index := range published {
		commit := &published[index]
		if commit.bundle == nil {
			continue
		}
		before := commit.provenance
		commit.provenance = commit.bundle.bind(commit.provenance)
		ok, err := admitted()
		if err != nil {
			return err
		}
		retained[index] = ok
		if !ok {
			commit.provenance = before
			reasons[index] = bundleRefusalReason(refusal)
		}
	}
	for index := range published {
		commit := &published[index]
		if retained[index] || reasons[index] == "" {
			continue
		}
		commit.provenance.BundleOmitted = reasons[index]
		ok, err := admitted()
		if err != nil {
			return err
		}
		if !ok {
			commit.provenance.BundleOmitted = ""
		}
	}
	return nil
}

// bundleRefusalReason is the omission code of a bundle whose result upload
// refused it: the per-artifact limit when the refusal names an artifact over
// it, which is the bundle or its record with the bundle bound to it, and
// otherwise the upload's limits as a whole.
func bundleRefusalReason(refusal error) string {
	var size *workerproto.ArtifactSizeError
	if errors.As(refusal, &size) && size.Scope == "object" {
		return BundleOmittedSizeLimit
	}
	return BundleOmittedAggregateLimit
}

// captureCommitBundle captures a retained bundle in the attempt's staging
// directory as the artifact its provenance record is bound to.
func (f AttemptFinalizer) captureCommitBundle(
	request AttemptFinalization,
	stageDir string,
	provenance CommitProvenance,
	candidate commitBundleCandidate,
	id string,
	now time.Time,
) (domain.Artifact, error) {
	input, err := os.Open(candidate.path)
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("open commit bundle: %w", err)
	}
	defer input.Close()
	name := CommitBundleArtifactName(provenance.Name)
	storagePath := filepath.ToSlash(filepath.Join(
		"runs", request.Attempt.WorkflowRunID, request.Task.ID, request.Attempt.ID, "artifacts", name,
	))
	file, err := writeIngestedFile(input, filepath.Join(stageDir, "artifacts", filepath.FromSlash(name)), name, storagePath)
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("capture commit bundle: %w", err)
	}
	if file.size != candidate.size || !strings.EqualFold(file.sha256, candidate.sha256) {
		return domain.Artifact{}, fmt.Errorf("capture commit bundle: %s changed after it was bound to its provenance record", name)
	}
	return domain.Artifact{
		ID: id, WorkflowRunID: request.Attempt.WorkflowRunID,
		TaskID: request.Task.ID, AttemptID: request.Attempt.ID,
		Kind: domain.ArtifactGitState, Name: name, MediaType: CommitBundleMediaType,
		Size: file.size, SHA256: file.sha256, StoragePath: file.storagePath,
		Producer: request.Task.Name, CreatedAt: now,
	}, nil
}

// createBundle writes a verified bundle of base..commit for one published
// commit into a private temporary directory and returns its path. An empty path
// with no error means no bundle exists: either the commit is its base, which a
// consumer already holds, or omitted says why none could be made.
func (s CampaignRefStore) createBundle(ctx context.Context, provenance CommitProvenance, limit int64, log io.Writer) (string, string, error) {
	if provenance.Commit == provenance.Base {
		return "", "", nil
	}
	source, err := bundleRef(provenance)
	if err != nil {
		return "", "", err
	}
	gitDir, err := s.open(ctx, log)
	if err != nil {
		return "", "", err
	}
	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return "", "", fmt.Errorf("lock campaign refs: %w", err)
	}
	defer lock.Close()
	if head, found, err := s.head(ctx, gitDir, source, log); err != nil {
		return "", "", err
	} else if !found || head != provenance.Commit {
		return "", "", fmt.Errorf("bundle campaign commit: ref %s does not name commit %s", source, provenance.Commit)
	}
	if _, err := runLoggedCommandOutput(ctx, log, "", s.git(), "--git-dir", gitDir,
		"merge-base", "--is-ancestor", provenance.Base, provenance.Commit); err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			return "", "", fmt.Errorf("bundle campaign commit: %w", err)
		}
		// The base is not in the commit's history, so there is no base..commit
		// to carry; a whole-history bundle is not what a consumer was promised.
		return "", BundleOmittedNotDescendant, nil
	}
	directory, err := os.MkdirTemp(s.Root, ".bundle-")
	if err != nil {
		return "", "", fmt.Errorf("bundle campaign commit: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(directory)
		}
	}()
	path := filepath.Join(directory, "commit.bundle")
	if err := runLoggedCommand(ctx, log, "", s.git(), "--git-dir", gitDir,
		"bundle", "create", path, source, "^"+provenance.Base); err != nil {
		return "", "", fmt.Errorf("bundle campaign commit %s: %w", source, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", "", fmt.Errorf("bundle campaign commit: %w", err)
	}
	if info.Size() > limit {
		return "", BundleOmittedSizeLimit, nil
	}
	if err := runLoggedCommand(ctx, log, "", s.git(), "--git-dir", gitDir, "bundle", "verify", path); err != nil {
		return "", "", fmt.Errorf("verify commit bundle of %s: %w", source, err)
	}
	keep = true
	return path, "", nil
}

// bundleRef is the store ref a commit's bundle is made from and names: the
// campaign ref of a published commit, and the staged ref of the attempt that
// staged a review-declared task's commit. A bundle of staged work names its
// staging, so whoever imports it knows it as staged work of that attempt and
// never as the task's campaign output.
func bundleRef(provenance CommitProvenance) (string, error) {
	if provenance.StagedAttempt == "" {
		return CampaignRef(provenance.WorkflowRunID, provenance.TaskID, provenance.Name), nil
	}
	if !safePathComponent(provenance.StagedAttempt) {
		return "", fmt.Errorf("campaign commit record names staging attempt %q, which is not a safe path component", provenance.StagedAttempt)
	}
	ref := StagedCampaignRef(provenance.WorkflowRunID, provenance.TaskID, provenance.StagedAttempt, provenance.Name)
	if err := validateGitRef(ref); err != nil {
		return "", fmt.Errorf("campaign commit staging: %w", err)
	}
	return ref, nil
}

// Obtain makes a declared commit present in this worker's store under its
// campaign ref before a consumer fetches it.
//
// When the store already holds the ref, as it does on the producer's own
// worker or after an earlier import, nothing is read. Otherwise the commit is
// imported from the delivered bundle into the consuming workspace, checked to
// be exactly the declared commit descending from the declared base, and pushed
// into the store with its provenance, so that it is released with the run like
// any commit published here. Every failure is specific, and none falls back to
// any other commit.
//
// A review-declared task's staged commit is imported as staged work of the
// attempt that staged it, exactly as that attempt's worker holds it: the fetch
// that follows publishes it only for a consumer the coordinator accepted the
// producing result for, and otherwise reads it from the staging.
func (s CampaignRefStore) Obtain(ctx context.Context, workspaceDir string, provenance CommitProvenance, delivery *CommitBundleDelivery, log io.Writer) error {
	if err := s.validate(); err != nil {
		return err
	}
	if workspaceDir == "" {
		return errors.New("obtain campaign commit: consuming workspace is required")
	}
	if err := validateCommitTarget(provenance.WorkflowRunID, provenance.TaskID, provenance.Name); err != nil {
		return err
	}
	if !validGitObjectID(provenance.Commit) || !validGitObjectID(provenance.Base) {
		return fmt.Errorf("obtain campaign commit: %q from base %q is not a commit ID", provenance.Commit, provenance.Base)
	}
	ref := CampaignRef(provenance.WorkflowRunID, provenance.TaskID, provenance.Name)
	if provenance.Ref != "" && provenance.Ref != ref {
		return fmt.Errorf("campaign commit record names ref %q, want %q", provenance.Ref, ref)
	}
	provenance.Ref = ref
	source, err := bundleRef(provenance)
	if err != nil {
		return err
	}
	gitDir, err := s.open(ctx, log)
	if err != nil {
		return err
	}
	if held, err := s.heldCommit(ctx, gitDir, provenance, log); err != nil || held {
		return err
	}
	// A review-gated producer on this worker staged its commit rather than
	// publishing it, so the store holds it under the producing attempt's
	// staged ref. The fetch that follows publishes it for an accepted
	// consumer or reads it from staging for inspection; importing a bundle
	// here would publish it without the coordinator's acceptance.
	if staged, err := s.stagedHeld(ctx, gitDir, provenance, log); err != nil || staged {
		return err
	}
	if !s.hasCommit(ctx, workspaceDir, provenance.Base, log) {
		return fmt.Errorf("campaign commit %s: missing prerequisite: its base %s is not present in this worker's repository cache for %s, "+
			"and the commit can only be imported on top of it", ref, provenance.Base, provenance.Repository)
	}
	if provenance.Commit != provenance.Base {
		if err := s.importBundle(ctx, workspaceDir, provenance, source, delivery, log); err != nil {
			return err
		}
	}

	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return fmt.Errorf("lock campaign refs: %w", err)
	}
	defer lock.Close()
	// Another consumer on this worker may have imported it meanwhile.
	if held, err := s.heldCommit(ctx, gitDir, provenance, log); err != nil || held {
		return err
	}
	if provenance.StagedAttempt != "" {
		if staged, err := s.stagedHeld(ctx, gitDir, provenance, log); err != nil || staged {
			return err
		}
		// The record is written first, as Stage writes it, so that release
		// finds the staged ref. A record whose push failed is taken back, and
		// one that outlived a crash is not taken for the staging, because
		// stagedHeld also reads the ref.
		record := s.stagedPath(provenance.WorkflowRunID, provenance.TaskID, provenance.StagedAttempt, provenance.Name)
		if err := writeCommitRecord(record, provenance); err != nil {
			return err
		}
		if err := runLoggedCommand(ctx, log, "", s.git(), "-C", workspaceDir,
			"push", "--", gitDir, "+"+provenance.Commit+":"+source); err != nil {
			_ = os.Remove(record)
			return fmt.Errorf("import staged campaign ref %s: %w", source, err)
		}
		return nil
	}
	if err := runLoggedCommand(ctx, log, "", s.git(), "-C", workspaceDir,
		"push", "--", gitDir, provenance.Commit+":"+ref); err != nil {
		return fmt.Errorf("import campaign ref %s: %w", ref, err)
	}
	return s.writeProvenance(provenance)
}

// stagedHeld reports whether the store holds the declared commit as staged
// work, as findStaged resolves it. A record alone is what a staging or an
// import that stopped before its ref leaves, and the commit may then be
// missing from the store.
func (s CampaignRefStore) stagedHeld(ctx context.Context, gitDir string, provenance CommitProvenance, log io.Writer) (bool, error) {
	_, _, found, err := s.findStaged(ctx, gitDir, provenance, log)
	return found, err
}

// heldCommit reports whether the store already holds the declared commit. A
// ref that names anything else is refused, never replaced.
func (s CampaignRefStore) heldCommit(ctx context.Context, gitDir string, provenance CommitProvenance, log io.Writer) (bool, error) {
	existing, found, err := s.head(ctx, gitDir, provenance.Ref, log)
	if err != nil || !found {
		return false, err
	}
	if existing != provenance.Commit {
		return false, fmt.Errorf("campaign ref %s in this worker's store names commit %s, but its provenance record names %s",
			provenance.Ref, existing, provenance.Commit)
	}
	return true, nil
}

func (s CampaignRefStore) hasCommit(ctx context.Context, workspaceDir, commit string, log io.Writer) bool {
	_, err := runLoggedCommandOutput(ctx, log, "", s.git(), "-C", workspaceDir, "cat-file", "-e", commit+"^{commit}")
	return err == nil
}

// importBundle fetches the declared commit from its delivered bundle into the
// consuming workspace under its campaign ref. The bundle must name exactly
// source, the ref bundleRef gives for the provenance record.
func (s CampaignRefStore) importBundle(ctx context.Context, workspaceDir string, provenance CommitProvenance, source string, delivery *CommitBundleDelivery, log io.Writer) error {
	ref := provenance.Ref
	if delivery == nil {
		switch {
		case provenance.BundleOmitted != "":
			return fmt.Errorf("campaign commit %s is not in this worker's campaign ref store, and its producer retained no bundle (%s): %s",
				ref, provenance.BundleOmitted, describeBundleOmission(provenance.BundleOmitted))
		case provenance.Bundle != nil:
			return fmt.Errorf("campaign commit %s is not in this worker's campaign ref store, and its bundle %s was not delivered "+
				"with this task's execution package; the coordinator must support capability %q",
				ref, provenance.Bundle.Artifact, workerproto.PackageCapabilityCommitBundle)
		default:
			return fmt.Errorf("campaign commit %s is not in this worker's campaign ref store, and its producer retained no bundle and recorded no reason: "+
				"either the producing worker does not support capability %q, or not even the omission reason fit within the per-artifact "+
				"or total limit of the producer's result upload; the commit can be consumed only on the worker that produced it",
				ref, workerproto.PackageCapabilityCommitBundle)
		}
	}
	if delivery.Omitted != "" {
		return fmt.Errorf("campaign commit %s is not in this worker's campaign ref store, and the coordinator could not deliver its bundle: %s",
			ref, delivery.Omitted)
	}
	if provenance.Bundle == nil {
		return fmt.Errorf("campaign commit %s: a bundle was delivered, but its provenance record is bound to none", ref)
	}
	if !strings.EqualFold(delivery.SHA256, provenance.Bundle.SHA256) || delivery.Size != provenance.Bundle.Size {
		return fmt.Errorf("campaign commit %s: the delivered bundle (sha256 %s, %d bytes) is not the bundle its provenance record names (sha256 %s, %d bytes)",
			ref, delivery.SHA256, delivery.Size, provenance.Bundle.SHA256, provenance.Bundle.Size)
	}
	if limit := s.maxBundleBytes(); delivery.Size > limit {
		return fmt.Errorf("campaign commit %s: its bundle is %d bytes, which exceeds the limit of %d bytes (storage.campaign_commit_bundle_max_bytes)",
			ref, delivery.Size, limit)
	}
	if err := os.MkdirAll(s.Root, 0o700); err != nil {
		return fmt.Errorf("import campaign commit: %w", err)
	}
	directory, err := os.MkdirTemp(s.Root, ".import-")
	if err != nil {
		return fmt.Errorf("import campaign commit: %w", err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "commit.bundle")
	if err := receiveBundle(ctx, delivery, path, ref); err != nil {
		return err
	}
	heads, prerequisites, err := readBundleHeader(path)
	if err != nil {
		return fmt.Errorf("campaign commit %s: commit bundle is invalid: %w", ref, err)
	}
	if len(heads) != 1 || heads[0].ref != source {
		return fmt.Errorf("campaign commit %s: commit bundle names %d refs, want exactly %s", ref, len(heads), source)
	}
	if heads[0].commit != provenance.Commit {
		return fmt.Errorf("campaign commit %s: commit bundle names commit %s, but its provenance record names %s",
			ref, heads[0].commit, provenance.Commit)
	}
	for _, prerequisite := range prerequisites {
		if !s.hasCommit(ctx, workspaceDir, prerequisite, log) {
			return fmt.Errorf("campaign commit %s: missing prerequisite: the bundle requires %s, which is not present in this worker's repository cache",
				ref, prerequisite)
		}
		if _, err := runLoggedCommandOutput(ctx, log, "", s.git(), "-C", workspaceDir,
			"merge-base", "--is-ancestor", prerequisite, provenance.Base); err != nil {
			return fmt.Errorf("campaign commit %s: the bundle requires %s, which is neither the base %s its provenance record names nor an ancestor of it",
				ref, prerequisite, provenance.Base)
		}
	}
	if err := runLoggedCommand(ctx, log, "", s.git(), "-C", workspaceDir, "bundle", "verify", path); err != nil {
		return fmt.Errorf("campaign commit %s: commit bundle failed verification: %w", ref, err)
	}
	if err := runLoggedCommand(ctx, log, "", s.git(), "-C", workspaceDir,
		"fetch", "--no-tags", "--", path, "+"+source+":"+ref); err != nil {
		return fmt.Errorf("campaign commit %s: import commit bundle: %w", ref, err)
	}
	raw, err := runLoggedCommandOutput(ctx, log, "", s.git(), "-C", workspaceDir, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return fmt.Errorf("campaign commit %s: verify imported commit: %w", ref, err)
	}
	if got := strings.TrimSpace(string(raw)); got != provenance.Commit {
		return fmt.Errorf("campaign commit %s: imported commit %s, but its provenance record names %s", ref, got, provenance.Commit)
	}
	if _, err := runLoggedCommandOutput(ctx, log, "", s.git(), "-C", workspaceDir,
		"merge-base", "--is-ancestor", provenance.Base, provenance.Commit); err != nil {
		return fmt.Errorf("campaign commit %s: commit %s does not descend from the base %s its provenance record names",
			ref, provenance.Commit, provenance.Base)
	}
	return nil
}

// receiveBundle copies a delivered bundle to path and checks it is exactly the
// bytes the provenance record was bound to.
func receiveBundle(ctx context.Context, delivery *CommitBundleDelivery, path, ref string) error {
	if delivery.Open == nil {
		return fmt.Errorf("campaign commit %s: the delivered bundle cannot be read", ref)
	}
	source, err := delivery.Open(ctx)
	if err != nil {
		return fmt.Errorf("campaign commit %s: open delivered bundle: %w", ref, err)
	}
	defer source.Close()
	output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("campaign commit %s: stage delivered bundle: %w", ref, err)
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(output, hash), io.LimitReader(source, delivery.Size+1))
	if err := errors.Join(copyErr, output.Close()); err != nil {
		return fmt.Errorf("campaign commit %s: stage delivered bundle: %w", ref, err)
	}
	if digest := fmt.Sprintf("%x", hash.Sum(nil)); size != delivery.Size || !strings.EqualFold(digest, delivery.SHA256) {
		return fmt.Errorf("campaign commit %s: commit bundle is corrupt: received %d bytes with sha256 %s, want %d bytes with sha256 %s",
			ref, size, digest, delivery.Size, delivery.SHA256)
	}
	return nil
}

type bundleHead struct {
	commit string
	ref    string
}

// maxBundleHeaderBytes bounds the header read before Git sees the bundle. A
// commit bundle names one ref and a handful of prerequisites.
const maxBundleHeaderBytes = 1 << 20

// readBundleHeader reads the refs and prerequisites a v2 or v3 bundle declares.
func readBundleHeader(path string) ([]bundleHead, []string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(io.LimitReader(file, maxBundleHeaderBytes))
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, nil, errors.New("no bundle signature")
	}
	if line != "# v2 git bundle\n" && line != "# v3 git bundle\n" {
		return nil, nil, fmt.Errorf("unsupported bundle signature %q", strings.TrimSpace(line))
	}
	var heads []bundleHead
	var prerequisites []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, nil, errors.New("bundle header is not terminated")
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "":
			return heads, prerequisites, nil
		case strings.HasPrefix(line, "@"):
			// A v3 capability, such as the object format.
		case strings.HasPrefix(line, "-"):
			commit, _, _ := strings.Cut(strings.TrimPrefix(line, "-"), " ")
			if !validGitObjectID(commit) {
				return nil, nil, fmt.Errorf("bundle prerequisite %q is not a commit ID", commit)
			}
			prerequisites = append(prerequisites, commit)
		default:
			commit, ref, found := strings.Cut(line, " ")
			if !found || !validGitObjectID(commit) || validateGitRef(ref) != nil {
				return nil, nil, fmt.Errorf("bundle ref line %q is malformed", line)
			}
			heads = append(heads, bundleHead{commit: commit, ref: ref})
		}
	}
}

// placementCapabilities is what a worker must advertise to run a task: what the
// manifest requires, and the commit bundle capability for a task that consumes
// a declared commit. Without it a worker other than the producer's cannot
// obtain the commit, and the gate is placement's rather than the offer's so
// that such a worker is excluded with the reason named instead of being
// offered work it can only refuse.
func placementCapabilities(manifest Manifest, task ManifestTask) []string {
	capabilities := append([]string(nil), task.Placement.Requires...)
	if task.Gate != nil && !slices.Contains(capabilities, workerproto.PackageCapabilityWorkerOwnedGate) {
		capabilities = append(capabilities, workerproto.PackageCapabilityWorkerOwnedGate)
	}
	if ConsumesDeclaredCommit(manifest, task) && !slices.Contains(capabilities, workerproto.PackageCapabilityCommitBundle) {
		capabilities = append(capabilities, workerproto.PackageCapabilityCommitBundle)
	}
	// The offer requires these for a review-declared task and for a consumer
	// of a review-declared producer's commit (M16-3); placing such a task on
	// a worker without them would only have its offer withheld.
	if task.ReviewRequirements != nil && !slices.Contains(capabilities, workerproto.PackageCapabilityWorkspaceHead) {
		capabilities = append(capabilities, workerproto.PackageCapabilityWorkspaceHead)
	}
	if consumesReviewedCommit(manifest, task) && !slices.Contains(capabilities, workerproto.PackageCapabilityAcceptedDependencies) {
		capabilities = append(capabilities, workerproto.PackageCapabilityAcceptedDependencies)
	}
	return capabilities
}

// consumesReviewedCommit reports whether a task takes a declared commit from a
// producer that declares review requirements, whose commit is staged and
// reaches the consumer only as an accepted dependency.
func consumesReviewedCommit(manifest Manifest, task ManifestTask) bool {
	for producer, names := range task.InputsFrom {
		producerTask, ok := manifest.Tasks[producer]
		if !ok || producerTask.ReviewRequirements == nil {
			continue
		}
		for _, declaration := range producerTask.OutputDeclarations() {
			if declaration.Commit != nil && slices.ContainsFunc(names, func(name string) bool { return filepath.ToSlash(name) == declaration.Name }) {
				return true
			}
		}
	}
	return false
}

// RequireCommitBundleCapability adds the commit bundle capability to a task
// that consumes a declared commit outside manifest ingest: a rerun carrying one
// from its source run, or an external dependency on one. Placement then
// excludes a worker that could not obtain the commit, exactly as for a commit
// consumer placed at ingest.
func RequireCommitBundleCapability(task *domain.Task) {
	if !slices.Contains(task.Placement.Capabilities, workerproto.PackageCapabilityCommitBundle) {
		task.Placement.Capabilities = append(task.Placement.Capabilities, workerproto.PackageCapabilityCommitBundle)
	}
}

// RequireCarriedCommitCapabilities adds what a worker needs to obtain a
// declared commit of producer that a task carries outside manifest ingest: the
// commit bundle capability, and for a review-declared producer, whose record
// names staged work, the capability of a worker that holds staged work, which
// a consumer in the producer's own run is placed with too.
func RequireCarriedCommitCapabilities(task *domain.Task, producer domain.Task) {
	RequireCommitBundleCapability(task)
	if producer.ReviewRequirements != nil && !slices.Contains(task.Placement.Capabilities, workerproto.PackageCapabilityAcceptedDependencies) {
		task.Placement.Capabilities = append(task.Placement.Capabilities, workerproto.PackageCapabilityAcceptedDependencies)
	}
}

// DeclaresCommit reports whether a task declares name as a commit output.
func DeclaresCommit(task domain.Task, name string) bool {
	return slices.ContainsFunc(task.Outputs, func(output domain.ArtifactDeclaration) bool {
		return output.Commit != nil && output.Name == filepath.ToSlash(name)
	})
}

// ConsumesDeclaredCommit reports whether a manifest task takes a declared
// commit of another task as an input. Such a task may run on a worker other
// than the producer's, and only a worker that can import the commit bundle can
// run it there.
func ConsumesDeclaredCommit(manifest Manifest, task ManifestTask) bool {
	for producer, names := range task.InputsFrom {
		producerTask, ok := manifest.Tasks[producer]
		if !ok {
			continue
		}
		for _, declaration := range producerTask.OutputDeclarations() {
			if declaration.Commit == nil {
				continue
			}
			for _, name := range names {
				if filepath.ToSlash(name) == declaration.Name {
					return true
				}
			}
		}
	}
	return false
}
