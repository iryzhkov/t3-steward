package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// SubmissionStore owns both the immutable workflow records and the durable
// request/result idempotency decision.
type SubmissionStore interface {
	CoordinatorRecordStore
	ReserveSubmission(context.Context, domain.SubmissionRecord) (domain.SubmissionRecord, bool, error)
	CompleteSubmission(context.Context, string, string, time.Time) (domain.SubmissionRecord, bool, error)
}

// DirectorySubmission is a bounded request whose content is copied before the
// accepted result is returned.
type DirectorySubmission struct {
	IdempotencyKey string
	BundleDir      string
	// Principal, Unverified and UnverifiedReason record who submitted and
	// whether the client skipped its own live readiness check. They are audit
	// facts; they never change what the coordinator validates.
	Principal        string
	Unverified       bool
	UnverifiedReason string
}

// SubmissionAudit is one recorded submission decision. It exists so that a use
// of the client-side escape hatch is loud rather than invisible once the run
// exists.
type SubmissionAudit struct {
	Key              string
	Digest           string
	Principal        string
	Unverified       bool
	UnverifiedReason string
	At               time.Time
}

// SubmissionResult is immutable for an idempotency key.
type SubmissionResult struct {
	Record     domain.SubmissionRecord
	StorageDir string
	Replay     bool
}

// SubmissionService serializes local publication while SQLite supplies durable
// cross-restart idempotency.
type SubmissionService struct {
	DirectoryCatalogs map[string][]directoryresource.Binding
	StorageRoot       string
	Store             SubmissionStore
	MaxBytes          int64
	MaxFiles          int
	Now               func() time.Time
	NewKey            func() string
	// Permanent refuses a permanently impossible manifest during ingestion.
	Permanent PermanentValidator
	// Audit records every submission decision, including a skipped client check.
	Audit func(context.Context, SubmissionAudit)

	mu sync.Mutex
}

func (s *SubmissionService) SubmitDirectory(ctx context.Context, request DirectorySubmission) (SubmissionResult, error) {
	if s == nil || s.Store == nil {
		return SubmissionResult{}, errors.New("submission store is required")
	}
	if s.StorageRoot == "" {
		return SubmissionResult{}, errors.New("submission storage root is required")
	}
	if s.MaxBytes < 1 || s.MaxFiles < 1 {
		return SubmissionResult{}, errors.New("positive submission byte and file limits are required")
	}
	key := request.IdempotencyKey
	if key == "" {
		if s.NewKey != nil {
			key = s.NewKey()
		} else {
			key = uuid.NewString()
		}
	}
	digest, err := directorySubmissionDigest(ctx, request.BundleDir, s.MaxBytes, s.MaxFiles)
	if err != nil {
		return SubmissionResult{}, err
	}
	workflowID, runID := submissionResultIDs(key)
	createdAt := time.Now().UTC()
	if s.Now != nil {
		createdAt = s.Now().UTC()
	}
	proposed := domain.SubmissionRecord{
		Key: key, Digest: digest, WorkflowID: workflowID, RunID: runID,
		State: domain.SubmissionPending, CreatedAt: createdAt,
	}

	if s.Audit != nil {
		s.Audit(ctx, SubmissionAudit{
			Key: key, Digest: digest, Principal: request.Principal,
			Unverified: request.Unverified, UnverifiedReason: request.UnverifiedReason,
			At: createdAt,
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	record, replay, err := s.Store.ReserveSubmission(ctx, proposed)
	if err != nil {
		return SubmissionResult{}, err
	}
	finalDir := filepath.Join(s.StorageRoot, "workflows", record.WorkflowID)
	if record.State == domain.SubmissionAccepted {
		return SubmissionResult{Record: record, StorageDir: finalDir, Replay: true}, nil
	}
	if replay {
		if err := recoverPendingSubmission(ctx, request.BundleDir, finalDir, digest, s.MaxBytes, s.MaxFiles); err != nil {
			return SubmissionResult{}, err
		}
	}
	ingester := BundleIngester{
		DirectoryCatalogs: s.DirectoryCatalogs,
		StorageRoot:       s.StorageRoot,
		Store:             s.Store,
		Permanent:         s.Permanent,
		Now:               func() time.Time { return record.CreatedAt },
		NewTypedID:        submissionTypedIDGenerator(key),
	}
	ingested, err := ingester.Ingest(ctx, request.BundleDir)
	if err != nil {
		return SubmissionResult{}, err
	}
	if ingested.WorkflowID != record.WorkflowID || ingested.RunID != record.RunID {
		return SubmissionResult{}, errors.New("submission ingester returned unexpected identities")
	}
	acceptedAt := time.Now().UTC()
	if s.Now != nil {
		acceptedAt = s.Now().UTC()
	}
	record, completionReplay, err := s.Store.CompleteSubmission(ctx, key, digest, acceptedAt)
	if err != nil {
		return SubmissionResult{}, err
	}
	return SubmissionResult{
		Record: record, StorageDir: ingested.StorageDir,
		Replay: replay || completionReplay,
	}, nil
}

func recoverPendingSubmission(ctx context.Context, sourceDir, finalDir, digest string, maxBytes int64, maxFiles int) error {
	info, err := os.Stat(finalDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect pending submission files: %w", err)
	}
	if !info.IsDir() {
		return errors.New("pending submission path is not a directory")
	}
	source, err := filepath.Abs(sourceDir)
	if err != nil {
		return err
	}
	published, err := filepath.Abs(finalDir)
	if err != nil {
		return err
	}
	if source == published {
		return errors.New("pending submission source cannot be its published directory")
	}
	publishedDigest, err := directorySubmissionDigest(ctx, filepath.Join(finalDir, "files"), maxBytes, maxFiles)
	if err != nil {
		return fmt.Errorf("verify pending submission files: %w", err)
	}
	if publishedDigest != digest {
		return errors.New("pending submission files do not match reserved content")
	}
	if err := removeIngestedTree(finalDir); err != nil {
		return fmt.Errorf("remove recoverable pending submission files: %w", err)
	}
	return nil
}

func directorySubmissionDigest(ctx context.Context, bundleDir string, maxBytes int64, maxFiles int) (string, error) {
	root, sourceRoot, manifest, manifestBytes, err := openIngestionBundleBounded(bundleDir, maxBytes)
	if err != nil {
		return "", fmt.Errorf("validate submission bundle: %w", err)
	}
	defer sourceRoot.Close()
	paths, _, err := ingestionPaths(root, manifest)
	if err != nil {
		return "", fmt.Errorf("validate submission bundle: %w", err)
	}
	if len(paths) > maxFiles {
		return "", fmt.Errorf("submission has %d files, limit is %d", len(paths), maxFiles)
	}
	sort.Strings(paths)
	digest := NewSubmissionDigest()
	remaining := maxBytes
	for _, relative := range paths {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		var content []byte
		if relative == "workflow.yaml" {
			content = manifestBytes
		} else {
			resolved, err := safeBundleFile(root, relative)
			if err != nil {
				return "", err
			}
			sourceRelative, err := filepath.Rel(root, resolved)
			if err != nil {
				return "", err
			}
			file, err := sourceRoot.Open(sourceRelative)
			if err != nil {
				return "", err
			}
			content, err = io.ReadAll(io.LimitReader(file, remaining+1))
			closeErr := file.Close()
			if err != nil {
				return "", err
			}
			if closeErr != nil {
				return "", closeErr
			}
		}
		if int64(len(content)) > remaining {
			return "", fmt.Errorf("submission exceeds %d bytes", maxBytes)
		}
		remaining -= int64(len(content))
		digest.AddFile(relative, content)
	}
	return digest.Sum(), nil
}

// SubmissionDigest computes the digest the coordinator records for a bundle.
//
// An idempotency key is only meaningful against this digest: the same key with
// the same bytes returns the same run, and with different bytes is refused. A
// caller that wants to show an author what will be sent before sending it must
// therefore compute exactly this, over the same paths in the same order.
//
// It is exported so there is one implementation instead of two that agree until
// they quietly stop agreeing. A second copy's failure mode is a predicted digest
// that does not match the recorded one, which is discovered at submission.
type SubmissionDigest struct{ hash hash.Hash }

// NewSubmissionDigest starts an empty digest.
func NewSubmissionDigest() *SubmissionDigest {
	return &SubmissionDigest{hash: sha256.New()}
}

// File frames one bundle file and returns the writer its content must be
// streamed into, together with the function that closes the entry.
//
// Framing is separate from content because a caller may not hold the content in
// memory: the campaign packer streams each file into the archive and into this
// digest at once, rather than reading every file twice. Paths are slash-spelled
// and must be added in sorted order, and the separators keep a file's name from
// running into its content or into the next entry.
func (d *SubmissionDigest) File(relativePath string) (io.Writer, func()) {
	_, _ = d.hash.Write([]byte(relativePath))
	_, _ = d.hash.Write([]byte{0})
	return d.hash, func() { _, _ = d.hash.Write([]byte{0}) }
}

// AddFile frames one bundle file whose content is already in memory.
func (d *SubmissionDigest) AddFile(relativePath string, content []byte) {
	writer, done := d.File(relativePath)
	_, _ = writer.Write(content)
	done()
}

// Sum returns the hexadecimal digest of everything added so far.
func (d *SubmissionDigest) Sum() string { return hex.EncodeToString(d.hash.Sum(nil)) }

func openIngestionBundleBounded(bundleDir string, maxBytes int64) (string, *os.Root, Manifest, []byte, error) {
	var empty Manifest
	root, err := filepath.Abs(bundleDir)
	if err != nil {
		return "", nil, empty, nil, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return "", nil, empty, nil, err
	}
	sourceRoot, err := os.OpenRoot(root)
	if err != nil {
		return "", nil, empty, nil, err
	}
	info, err := sourceRoot.Stat("workflow.yaml")
	if err != nil {
		sourceRoot.Close()
		return "", nil, empty, nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxBytes {
		sourceRoot.Close()
		return "", nil, empty, nil, fmt.Errorf("workflow.yaml must be regular and no larger than %d bytes", maxBytes)
	}
	sourceRoot.Close()
	return openIngestionBundle(root)
}

func submissionResultIDs(key string) (string, string) {
	return submissionID(key, "workflow", 0), submissionID(key, "run", 0)
}

func submissionTypedIDGenerator(key string) func(string) string {
	counts := make(map[string]int)
	return func(kind string) string {
		index := counts[kind]
		counts[kind] = index + 1
		return submissionID(key, kind, index)
	}
}

func submissionID(key, kind string, index int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%d", kind, key, index))
	return kind + "-" + hex.EncodeToString(sum[:16])
}
