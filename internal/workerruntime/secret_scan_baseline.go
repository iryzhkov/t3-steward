package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// scanBaseline is the repository state an execution started from. It is
// recorded in worker custody before the provider's first turn, while the
// workspace still holds what preparation checked out. The task can rewrite
// its workspace afterwards, including .t3/base-commit and an allowlist, so
// result admission trusts only this record.
type scanBaseline struct {
	Version int `json:"version"`
	// Base is the pinned commit; empty for a workspace without a repository.
	Base string `json:"base,omitempty"`
	// Allow holds the fixture fingerprints committed at Base.
	Allow []string `json:"allow,omitempty"`
}

const secretAllowlistPath = ".t3/secret-scan-allow"

func (s *CustodyStore) scanBaselinePath(pkg workerproto.ExecutionPackage) string {
	return filepath.Join(s.config.Root, "secret-scans", secretSnapshotKey(pkg)+".baseline.json")
}

// RecordScanBaseline keeps the first record for an execution, so a thread
// created again after the task has run cannot replace it.
func (s *CustodyStore) RecordScanBaseline(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	failure := &SecretScanError{Object: "execution", Detector: "scan-baseline", Offset: 0}
	path := s.scanBaselinePath(pkg)
	if _, err := os.Lstat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return failure
	}
	baseline := scanBaseline{Version: 1}
	if workspace != "" {
		base, err := backlog.WorkspaceBaseCommit(workspace)
		if err != nil {
			return failure
		}
		baseline.Base = base
	}
	if baseline.Base != "" {
		raw, err := committedSecretAllowlist(ctx, workspace, baseline.Base)
		if err != nil {
			return &SecretScanError{Object: secretAllowlistPath, Detector: "allowlist-read", Offset: 0}
		}
		allow, err := parseSecretAllowlist(raw)
		if err != nil {
			return &SecretScanError{Object: secretAllowlistPath, Detector: "allowlist-read", Offset: 0}
		}
		for fingerprint := range allow {
			baseline.Allow = append(baseline.Allow, fingerprint)
		}
		sort.Strings(baseline.Allow)
	}
	if err := ensureRealDirectory(filepath.Dir(path)); err != nil {
		return failure
	}
	if err := writeJSONExclusive(path, baseline); err != nil && !errors.Is(err, os.ErrExist) {
		return failure
	}
	return nil
}

// loadScanBaseline returns nil when no baseline was recorded, as for an
// execution started before the worker recorded them.
func (s *CustodyStore) loadScanBaseline(pkg workerproto.ExecutionPackage) (*scanBaseline, error) {
	raw, err := readScanBounded(s.scanBaselinePath(pkg), 4<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New("scan baseline read failed")
	}
	var baseline scanBaseline
	if json.Unmarshal(raw, &baseline) != nil || baseline.Version != 1 || baseline.Base != "" && !gitObjectID.MatchString(baseline.Base) {
		return nil, errors.New("scan baseline invalid")
	}
	for _, fingerprint := range baseline.Allow {
		if !fixtureFingerprint.MatchString(fingerprint) {
			return nil, errors.New("scan baseline invalid")
		}
	}
	return &baseline, nil
}

func (b *scanBaseline) allowSet() map[string]bool {
	allow := map[string]bool{}
	if b != nil {
		for _, fingerprint := range b.Allow {
			allow[fingerprint] = true
		}
	}
	return allow
}

// committedSecretAllowlist reads the allowlist from the commit's tree, never
// from the working tree. A missing file is an empty allowlist; anything but a
// regular file is refused.
func committedSecretAllowlist(ctx context.Context, repo, commit string) ([]byte, error) {
	listing, err := scanGitOutput(ctx, repo, "ls-tree", "-z", commit, "--", secretAllowlistPath)
	if err != nil {
		return nil, err
	}
	if len(listing) == 0 {
		return nil, nil
	}
	entry, name, ok := bytes.Cut(bytes.TrimSuffix(listing, []byte{0}), []byte{'\t'})
	fields := strings.Fields(string(entry))
	if !ok || string(name) != secretAllowlistPath || len(fields) != 3 || fields[1] != "blob" || fields[0] != "100644" && fields[0] != "100755" || !gitObjectID.MatchString(fields[2]) {
		return nil, errors.New("allowlist is not a regular committed file")
	}
	raw, err := scanGitOutput(ctx, repo, "cat-file", "blob", fields[2])
	if err != nil || len(raw) > 1<<20 {
		return nil, errors.New("allowlist unreadable or too large")
	}
	return raw, nil
}
