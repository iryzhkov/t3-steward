package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const childStageNamespace = "review-child-stages"
const childStageVersion = "review-child-stage/v1"
const childStageMetadataLimit = 256 << 10
const childStageMaxMembers = 32
const childStageMaxBytes = pinnedinput.MaxTotalBytes + childStageMaxMembers*pinnedinput.MaxFileBytes

// DeclaredChildStageRequest is INTERNAL coordinator metadata, not physical Git
// evidence or an authenticated public request. No path or policy is accepted.
type DeclaredChildStageRequest struct {
	Parent                   DeclaredAdmissionRequest
	CheckpointID, HeadCommit string
	Deadline                 time.Time
}

// ChildStageReceipt is immutable filesystem custody evidence, not SQL artifact
// publication or permission to activate the pure builder's default routes.
type ChildStageReceipt struct {
	Version, PromptVersion, Owner, AuthorityDigest string
	Checkpoint                                     review.CheckpointAuthority
	Manifest                                       pinnedinput.Manifest
	Criteria                                       CriteriaProvenance
	CreatedAt, Deadline                            time.Time
	Artifacts                                      []domain.Artifact
}
type DeclaredChildStageResult struct {
	Admission   AdmissionSnapshot
	Checkpoint  review.CheckpointAuthority
	Receipt     ChildStageReceipt
	Preparation review.ChildPreparation
}

// DeclaredReviewStaging owns a private namespace in the configured submission
// root. Its adapters are constructor-only trusted coordinator dependencies.
type DeclaredReviewStaging struct {
	admission ReviewAdmissionService
	store     *sqlite.Store
	now       func() time.Time
	// Package-private fault hook exercises actual durability boundaries in tests.
	fault func(string) error
}

func NewDeclaredReviewStaging(admission ReviewAdmissionService, store *sqlite.Store, now func() time.Time) (*DeclaredReviewStaging, error) {
	if store == nil || admission.Store != store || admission.Artifacts.Catalog != store || admission.Projects == nil || admission.Catalog == nil || now == nil ||
		!filepath.IsAbs(admission.Artifacts.SubmissionRoot) || !filepath.IsAbs(admission.Artifacts.Root) {
		return nil, errors.New("review child staging: complete configured coordinator owner required")
	}
	if err := childStageRootFence(admission.Artifacts.SubmissionRoot); err != nil {
		return nil, errors.New("review child staging: unsafe submission root")
	}
	if err := childStageRootFence(admission.Artifacts.Root); err != nil {
		return nil, errors.New("review child staging: unsafe artifact root")
	}
	return &DeclaredReviewStaging{admission: admission, store: store, now: now}, nil
}
func (s *DeclaredReviewStaging) boundary(phase string) error {
	if s.fault != nil {
		return s.fault(phase)
	}
	return nil
}

func (s *DeclaredReviewStaging) StageDeclared(ctx context.Context, request DeclaredChildStageRequest) (result DeclaredChildStageResult, err error) {
	phase := "request"
	defer func() {
		if err != nil {
			err = fmt.Errorf("review child staging checkpoint %q phase %s: %w", request.CheckpointID, phase, err)
		}
	}()
	// Finite deadlines are checked before freezing or allocating. Expiration
	// never causes reissue, extension or cleanup of an existing stage.
	deadline := request.Deadline.UTC()
	if deadline.IsZero() || deadline.Year() < 1 || deadline.Year() > 9999 || !deadline.After(s.now()) {
		return result, errors.New("finite unexpired deadline required")
	}
	candidate := review.Checkpoint{ID: request.CheckpointID, HeadCommit: request.HeadCommit, InputDigest: strings.Repeat("0", 64)}
	if err = candidate.Validate(); err != nil {
		return result, err
	}
	root := s.admission.Artifacts.SubmissionRoot
	if err = childStageRootFence(root); err != nil {
		return result, errors.New("unsafe configured root")
	}
	namespace := filepath.Join(root, childStageNamespace)
	if err = childStageDirectory(namespace); err != nil {
		return result, err
	}
	phase = "freeze"
	snapshot, err := s.admission.FreezeDeclared(ctx, request.Parent)
	if err != nil {
		return result, err
	}
	manifest, err := pinnedinput.NewManifest(snapshot.Provenance.InputManifest.Entries)
	if err != nil || !reflect.DeepEqual(manifest, snapshot.Provenance.InputManifest) {
		return result, errors.New("retained whole canonical manifest mismatch")
	}
	candidate.InputDigest = manifest.Digest
	phase = "allocate"
	checkpoint, err := s.store.AllocateReviewCheckpoint(ctx, snapshot.Authority, candidate)
	if err != nil {
		return result, err
	}
	if err = s.boundary("allocated"); err != nil {
		return result, err
	}
	round, err := s.store.GetReviewRound(ctx, checkpoint.RoundID)
	if err != nil {
		return result, err
	}
	if round.CreatedAt.IsZero() || !deadline.After(round.CreatedAt) {
		return result, errors.New("deadline must follow durable allocation")
	}
	phase = "owner-lock"
	lock, err := childStageLock(ctx, namespace, checkpoint.Key())
	if err != nil {
		return result, err
	}
	defer lock.Close()
	stageDir := filepath.Join(namespace, checkpoint.Key())
	if err = childStageDirectory(stageDir); err != nil {
		return result, err
	}
	dir, err := os.OpenRoot(stageDir)
	if err != nil {
		return result, err
	}
	defer dir.Close()
	phase = "copy-inputs"
	files, err := s.childStageFiles(ctx, snapshot, checkpoint, round.CreatedAt)
	if err != nil {
		return result, err
	}
	receipt := ChildStageReceipt{Version: childStageVersion, PromptVersion: review.TemplateVersion, Owner: checkpoint.Key(), AuthorityDigest: admissionDigest(snapshot.Authority), Checkpoint: checkpoint, Manifest: manifest, Criteria: snapshot.Provenance.Criteria, CreatedAt: round.CreatedAt, Deadline: deadline}
	for _, f := range files {
		receipt.Artifacts = append(receipt.Artifacts, f.Artifact)
	}
	raw, err := json.Marshal(receipt)
	if err != nil || len(raw) > childStageMetadataLimit {
		return result, errors.New("stage receipt exceeds bound")
	}
	// Classify completed state under the owner lock BEFORE retaining anything.
	// A receipt of any type is completed evidence, never a partial-stage repair.
	phase = "inspect-stage"
	complete := false
	if _, e := dir.Lstat("receipt.json"); e == nil {
		complete = true
	} else if !errors.Is(e, os.ErrNotExist) {
		return result, e
	}
	if err = childStageInspect(dir, raw, files, complete); err != nil {
		return result, err
	}
	if err = childStageCustody(dir, stageDir, lock); err != nil {
		return result, err
	}
	// Only genuinely partial stages may retain the first immutable intent.
	// It binds the original deadline, including pending publication recovery.
	phase = "retain-request"
	if !complete {
		if err = childStageRetain(dir, stageDir, "request.json", raw); err != nil {
			return result, err
		}
	}
	if err = s.boundary("request-retained"); err != nil {
		return result, err
	}
	phase = "retain-files"
	for _, f := range files {
		name := f.Artifact.ID + ".blob"
		if complete {
			err = childStageVerify(dir, name, f.Bytes)
		} else {
			err = childStageRetain(dir, stageDir, name, f.Bytes)
		}
		if err != nil {
			return result, err
		}
		if err = s.boundary("file-retained:" + f.Artifact.ID); err != nil {
			return result, err
		}
	}
	// Verify current declared custody after all IO. This is a separate SQL
	// transaction; activation must validate again in its future owning writer.
	phase = "current-handoff"
	current, err := s.store.AllocateReviewCheckpoint(ctx, snapshot.Authority, candidate)
	if err != nil || current != checkpoint {
		return result, errors.Join(err, errors.New("current checkpoint handoff refused"))
	}
	if err = s.boundary("files-verified"); err != nil {
		return result, err
	}
	phase = "retain-receipt"
	if !complete {
		if err = childStageRetain(dir, stageDir, "receipt.json", raw); err != nil {
			return result, err
		}
	}
	if err = s.boundary("receipt-retained"); err != nil {
		return result, err
	}
	phase = "prepare"
	for _, f := range files {
		if err = childStageVerify(dir, f.Artifact.ID+".blob", f.Bytes); err != nil {
			return result, err
		}
	}
	if err = childStageRootFence(stageDir); err != nil {
		return result, errors.New("stage root changed")
	}
	staged := CoordinatorArtifactStore{Root: root, SubmissionRoot: root, Catalog: childStageCatalog{artifacts: receipt.Artifacts}}
	preparation, err := PrepareReviewChild(ctx, staged, snapshot.Authority, checkpoint, receipt.Criteria.Name, deadline, receipt.Artifacts)
	if err != nil {
		return result, err
	}
	if _, err = preparation.Build(snapshot.Authority, checkpoint, round.CreatedAt); err != nil {
		return result, err
	}
	if _, err = s.store.AllocateReviewCheckpoint(ctx, snapshot.Authority, candidate); err != nil {
		return result, err
	}
	if !deadline.After(s.now()) {
		return result, errors.New("deadline expired during staging")
	}
	// Last filesystem operation is read-only, after all preparation and callbacks.
	phase = "final-custody"
	if err = childStageInspect(dir, raw, files, true); err != nil {
		return result, err
	}
	if err = childStageCustody(dir, stageDir, lock); err != nil {
		return result, err
	}
	return DeclaredChildStageResult{snapshot, checkpoint, receipt, preparation}, nil
}

// childStageInspect never creates, publishes, removes or repairs evidence.
// Existing partial data and every pending publication must also match exactly.
func childStageInspect(root *os.Root, raw []byte, files []review.RetainedFile, complete bool) error {
	expected := map[string][]byte{"request.json": raw, "receipt.json": raw}
	for _, f := range files {
		expected[f.Artifact.ID+".blob"] = f.Bytes
	}
	listing, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, readErr := listing.ReadDir(2*len(expected) + 1)
	closeErr := listing.Close()
	if errors.Is(readErr, io.EOF) {
		readErr = nil
	}
	if err = errors.Join(readErr, closeErr); err != nil {
		return err
	}
	if len(entries) > 2*len(expected) {
		return errors.New("stage file count exceeds bound")
	}
	for _, entry := range entries {
		name := strings.TrimPrefix(entry.Name(), ".pending-")
		if _, ok := expected[name]; !ok || !entry.Type().IsRegular() {
			return errors.New("unexpected stage entry")
		}
	}
	for name, data := range expected {
		if _, err = root.Lstat(name); complete || !errors.Is(err, os.ErrNotExist) {
			if err = childStageVerify(root, name, data); err != nil {
				return fmt.Errorf("stage %s: %w", name, err)
			}
		}
		if err = childStageCheckPending(root, name, data); err != nil {
			return fmt.Errorf("stage pending %s: %w", name, err)
		}
	}
	return nil
}

// childStageCustody is a read-only check of the original opened owner/lock
// against real private paths. It must not call the creating directory helper.
func childStageCustody(root *os.Root, stageDir string, lock *fileLock) error {
	if err := childStageRootFence(stageDir); err != nil {
		return err
	}
	namespace := filepath.Dir(stageDir)
	for _, path := range []string{namespace, filepath.Join(namespace, ".locks"), stageDir} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode() != os.ModeDir|0700 {
			return errors.New("private stage directory type/mode refused")
		}
		fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if err != nil {
			return errors.New("private stage directory descriptor refused")
		}
		f := os.NewFile(uintptr(fd), path)
		opened, statErr := f.Stat()
		closeErr := f.Close()
		if statErr != nil || closeErr != nil || opened.Mode() != os.ModeDir|0700 || !os.SameFile(info, opened) {
			return errors.New("private stage directory descriptor changed")
		}
	}
	owner, err := root.Open(".")
	if err != nil {
		return err
	}
	opened, statErr := owner.Stat()
	closeErr := owner.Close()
	info, pathErr := os.Lstat(stageDir)
	if statErr != nil || closeErr != nil || pathErr != nil || opened.Mode() != os.ModeDir|0700 || !os.SameFile(info, opened) {
		return errors.New("opened stage owner changed")
	}
	name := filepath.Join(namespace, ".locks", filepath.Base(stageDir)+".lock")
	info, err = os.Lstat(name)
	if err != nil || info.Mode() != 0600 || info.Size() != 0 {
		return errors.New("owner lock path type/mode/size refused")
	}
	opened, err = lock.file.Stat()
	if err != nil || opened.Mode() != 0600 || opened.Size() != 0 || !os.SameFile(info, opened) {
		return errors.New("opened owner lock changed")
	}
	return nil
}

// childStageFiles selects the entire admitted manifest from current retained
// lineage; clone/rerun reference IDs are selected from their immutable task.
func (s *DeclaredReviewStaging) childStageFiles(ctx context.Context, snap AdmissionSnapshot, cp review.CheckpointAuthority, created time.Time) ([]review.RetainedFile, error) {
	records, err := s.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return nil, err
	}
	run, err := admissionOne(records.WorkflowRuns, func(r domain.WorkflowRun) bool { return r.ID == snap.Authority.Parent.RunID })
	if err != nil {
		return nil, err
	}
	task, err := admissionOne(domain.TasksForRun(run, records.Tasks), func(t domain.Task) bool { return t.ID == snap.Authority.Parent.TaskID })
	if err != nil {
		return nil, err
	}
	workflow, err := admissionOne(records.Workflows, func(w domain.Workflow) bool { return w.ID == run.WorkflowID })
	if err != nil {
		return nil, err
	}
	manifest, criteria, err := admissionCriteria(ctx, s.admission.Artifacts, records, workflow, run, task, snap.Provenance.Criteria.ArtifactID)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(manifest, snap.Provenance.InputManifest) || criteria != snap.Provenance.Criteria {
		return nil, errors.New("original input provenance changed")
	}
	ids := workflow.InputArtifactIDs
	if run.Graph != nil && run.Graph.ClonedFrom != nil {
		ids = task.InputArtifactIDs
	}
	if len(snap.Authority.Requirements.Members) > childStageMaxMembers {
		return nil, errors.New("stage reviewer count exceeds bound")
	}
	var files []review.RetainedFile
	seen := map[string]bool{}
	var total int64
	add := func(a domain.Artifact, raw []byte) error {
		if seen[a.Name] || !pinnedinput.ValidName(a.Name) || len(files) >= pinnedinput.MaxFiles+childStageMaxMembers || int64(len(raw)) > pinnedinput.MaxFileBytes || int64(len(raw)) > childStageMaxBytes-total {
			return errors.New("stage name collision or byte/file limit")
		}
		seen[a.Name] = true
		total += int64(len(raw))
		a.WorkflowRunID, a.AttemptID, a.Producer, a.Kind = cp.RoundID, "", "submission", domain.ArtifactInput
		a.CreatedAt = created
		a.Size, a.SHA256 = int64(len(raw)), childStageBytesDigest(raw)
		a.StoragePath = childStageNamespace + "/" + cp.Key() + "/" + a.ID + ".blob"
		files = append(files, review.RetainedFile{Artifact: a, Bytes: raw})
		return nil
	}
	for _, pin := range manifest.Entries {
		a, e := admissionOne(records.Artifacts, func(a domain.Artifact) bool { return slices.Contains(ids, a.ID) && a.Name == pin.Name })
		if e != nil {
			return nil, e
		}
		if a.Size != pin.Size || a.SHA256 != pin.SHA256 {
			return nil, errors.New("input pin mismatch")
		}
		opened, descriptor, e := s.admission.Artifacts.Open(ctx, a.ID)
		if e != nil {
			return nil, e
		}
		raw, readErr := io.ReadAll(io.LimitReader(descriptor, pinnedinput.MaxFileBytes+1))
		closeErr := descriptor.Close()
		if e = errors.Join(readErr, closeErr); e != nil {
			return nil, e
		}
		if !reflect.DeepEqual(a, opened) || int64(len(raw)) != pin.Size || childStageBytesDigest(raw) != pin.SHA256 {
			return nil, errors.New("retained descriptor bytes changed")
		}
		a.ID, a.TaskID = review.ChildInputID(cp, a.Name), ""
		if e = add(a, raw); e != nil {
			return nil, e
		}
	}
	for _, m := range snap.Authority.Requirements.Members {
		prompt, e := review.ChildPrompt(snap.Authority, cp, m, manifest, criteria.Name)
		if e != nil {
			return nil, e
		}
		a := domain.Artifact{ID: review.ChildPromptID(cp, m.ID), TaskID: cp.MemberTaskID(m.ID), Name: review.ChildPromptName(m.ID), MediaType: "text/markdown"}
		if e = add(a, []byte(prompt)); e != nil {
			return nil, e
		}
	}
	return files, nil
}
func childStageBytesDigest(raw []byte) string {
	// admissionDigest hashes JSON; retained descriptors hash their original bytes.
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

type childStageCatalog struct {
	ArtifactCatalog // non-read operations deliberately unavailable
	artifacts       []domain.Artifact
}

func (c childStageCatalog) LoadArtifacts(_ context.Context, ids []string) ([]domain.Artifact, error) {
	var out []domain.Artifact
	for _, id := range ids {
		for _, a := range c.artifacts {
			if a.ID == id {
				out = append(out, a)
			}
		}
	}
	return out, nil
}

// All configured root components and private owner components must be real
// directories. os.Root further fences operations to an opened owner directory.
func childStageRootFence(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("unsafe root")
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("root component is not a real directory")
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	return nil
}
func childStageDirectory(path string) error {
	if err := ensureRealDirectory(path, 0700); err != nil {
		return errors.New("private stage directory refused")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0700 {
		return errors.New("private stage directory mode refused")
	}
	if err = childStageRootFence(path); err != nil {
		return err
	}
	return childStageSync(filepath.Dir(path))
}
func childStageLock(ctx context.Context, namespace, key string) (*fileLock, error) {
	locks := filepath.Join(namespace, ".locks")
	if err := childStageDirectory(locks); err != nil {
		return nil, err
	}
	name := filepath.Join(locks, key+".lock")
	fd, err := syscall.Open(name, syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, errors.New("owner lock refused")
	}
	f := os.NewFile(uintptr(fd), name)
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		f.Close()
		return nil, errors.New("owner lock type/mode refused")
	}
	for {
		err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &fileLock{file: f}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			f.Close()
			return nil, err
		}
		timer := time.NewTimer(fileLockRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func childStageVerify(root *os.Root, name string, raw []byte) error {
	info, err := root.Lstat(name)
	if err != nil {
		return errors.New("retained stage file missing")
	}
	if info.Mode() != 0400 || info.Size() != int64(len(raw)) {
		return errors.New("retained stage file type/mode/size mismatch")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return errors.New("retained stage descriptor refused")
	}
	opened, statErr := f.Stat()
	if statErr != nil || opened.Mode() != 0400 || opened.Size() != int64(len(raw)) || !os.SameFile(info, opened) {
		f.Close()
		return errors.New("retained stage descriptor type/mode/size/identity mismatch")
	}
	got, readErr := io.ReadAll(io.LimitReader(f, int64(len(raw))+1))
	closeErr := f.Close()
	if err = errors.Join(readErr, closeErr); err != nil {
		return err
	}
	if !bytes.Equal(got, raw) {
		return errors.New("retained stage bytes conflict")
	}
	return nil
}
func childStageCheckPending(root *os.Root, name string, raw []byte) error {
	if _, err := root.Lstat(".pending-" + name); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return childStageVerify(root, ".pending-"+name, raw)
}
func childStageRetain(root *os.Root, dir, name string, raw []byte) error {
	if _, err := root.Lstat(name); err == nil {
		return childStageVerify(root, name, raw)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	pending := ".pending-" + name
	f, err := root.OpenFile(pending, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if errors.Is(err, os.ErrExist) {
		if err = childStageVerify(root, pending, raw); err != nil {
			return err
		}
	} else {
		if err != nil {
			return err
		}
		_, writeErr := f.Write(raw)
		chmodErr := f.Chmod(0400)
		syncErr := f.Sync()
		closeErr := f.Close()
		if err = errors.Join(writeErr, chmodErr, syncErr, closeErr); err != nil {
			return err
		}
	}
	// Hard-link is an atomic no-replace publication; rename could overwrite.
	if err = root.Link(pending, name); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err = childStageSync(dir); err != nil {
		return err
	}
	if err = childStageVerify(root, name, raw); err != nil {
		return err
	}
	if err = root.Remove(pending); err != nil {
		return err
	}
	return childStageSync(dir)
}
func childStageSync(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	return errors.Join(syncErr, dir.Close())
}
