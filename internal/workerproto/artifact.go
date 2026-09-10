package workerproto

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"
)

const ArtifactManifestVersion = 1

type ArtifactObject struct {
	ID            string `json:"id"`
	Path          string `json:"path"`
	Kind          string `json:"kind"`
	MediaType     string `json:"mediaType"`
	Size          int64  `json:"size"`
	SHA256        string `json:"sha256"`
	ArchiveFormat string `json:"archiveFormat,omitempty"`
}

type ArtifactTransferManifest struct {
	Version          int              `json:"version"`
	ID               string           `json:"id"`
	Direction        string           `json:"direction"`
	CoordinatorEpoch int64            `json:"coordinatorEpoch"`
	WorkerID         string           `json:"workerId"`
	WorkerEpoch      string           `json:"workerEpoch"`
	AssignmentID     string           `json:"assignmentId"`
	AssignmentEpoch  int64            `json:"assignmentEpoch"`
	Objects          []ArtifactObject `json:"objects"`
	TotalBytes       int64            `json:"totalBytes"`
	CreatedAt        time.Time        `json:"createdAt"`
	ExpiresAt        time.Time        `json:"expiresAt"`
}

type ArtifactUploadRequest struct {
	Manifest ArtifactTransferManifest `json:"manifest"`
	Custody  ArtifactCustodyRecord    `json:"custody"`
}

type ArtifactDownloadRequest struct {
	ManifestID string   `json:"manifestId"`
	ObjectIDs  []string `json:"objectIds"`
}

type ArtifactCustodyRecord struct {
	ManifestID     string    `json:"manifestId"`
	ObjectID       string    `json:"objectId"`
	From           string    `json:"from"`
	To             string    `json:"to"`
	Sequence       int64     `json:"sequence"`
	Size           int64     `json:"size"`
	SHA256         string    `json:"sha256"`
	VerifiedAt     time.Time `json:"verifiedAt"`
	PreviousSHA256 string    `json:"previousSha256,omitempty"`
	RecordSHA256   string    `json:"recordSha256"`
}

type ArchiveLimits struct {
	MaxEntries int
	MaxBytes   int64
}

func ValidateArtifactObject(object ArtifactObject, maxBytes int64) error {
	if !identityPattern.MatchString(object.ID) {
		return errors.New("artifact: invalid id")
	}
	if !safeRelativePath(object.Path) {
		return errors.New("artifact: unsafe path")
	}
	if object.Kind == "" || object.MediaType == "" {
		return errors.New("artifact: kind and media type are required")
	}
	if object.Size < 0 || maxBytes <= 0 || object.Size > maxBytes {
		return errors.New("artifact: invalid or excessive size")
	}
	if !validSHA256(object.SHA256) {
		return errors.New("artifact: invalid checksum")
	}
	if object.ArchiveFormat != "" && object.ArchiveFormat != "tar" {
		return errors.New("artifact: unsupported archive format")
	}
	return nil
}

func ValidateArtifactTransferManifest(manifest ArtifactTransferManifest, maxArtifactBytes, maxTotalBytes int64, now time.Time) error {
	if manifest.Version != ArtifactManifestVersion {
		return errors.New("artifact manifest: unsupported version")
	}
	if !identityPattern.MatchString(manifest.ID) || !identityPattern.MatchString(manifest.WorkerID) ||
		!identityPattern.MatchString(manifest.WorkerEpoch) || !identityPattern.MatchString(manifest.AssignmentID) {
		return errors.New("artifact manifest: invalid identity")
	}
	if manifest.Direction != "upload" && manifest.Direction != "download" {
		return errors.New("artifact manifest: invalid direction")
	}
	if manifest.CoordinatorEpoch < 1 || manifest.AssignmentEpoch < 1 || len(manifest.Objects) == 0 ||
		manifest.CreatedAt.IsZero() || !manifest.ExpiresAt.After(manifest.CreatedAt) || !now.Before(manifest.ExpiresAt) {
		return errors.New("artifact manifest: invalid epoch, contents, or lifetime")
	}
	ids := make(map[string]struct{}, len(manifest.Objects))
	paths := make(map[string]struct{}, len(manifest.Objects))
	var total int64
	for _, object := range manifest.Objects {
		if err := ValidateArtifactObject(object, maxArtifactBytes); err != nil {
			return fmt.Errorf("artifact manifest: %w", err)
		}
		if _, exists := ids[object.ID]; exists {
			return errors.New("artifact manifest: duplicate object id")
		}
		if _, exists := paths[object.Path]; exists {
			return errors.New("artifact manifest: duplicate object path")
		}
		ids[object.ID] = struct{}{}
		paths[object.Path] = struct{}{}
		if object.Size > maxTotalBytes-total {
			return errors.New("artifact manifest: total size exceeds limit")
		}
		total += object.Size
	}
	if manifest.TotalBytes != total {
		return errors.New("artifact manifest: total size mismatch")
	}
	return nil
}

func VerifyArtifact(reader io.Reader, object ArtifactObject, maxBytes int64) error {
	if err := ValidateArtifactObject(object, maxBytes); err != nil {
		return err
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return fmt.Errorf("artifact: read: %w", err)
	}
	if n != object.Size {
		return errors.New("artifact: size mismatch")
	}
	if !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), object.SHA256) {
		return errors.New("artifact: checksum mismatch")
	}
	return nil
}

func ValidateTarArchive(reader io.Reader, compressedSize int64, limits ArchiveLimits) error {
	if compressedSize < 0 || limits.MaxEntries < 1 || limits.MaxBytes < 1 {
		return errors.New("artifact archive: invalid limits")
	}
	tr := tar.NewReader(io.LimitReader(reader, compressedSize+1))
	seen := make(map[string]struct{})
	var entries int
	var total int64
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("artifact archive: malformed tar: %w", err)
		}
		entries++
		if entries > limits.MaxEntries {
			return errors.New("artifact archive: entry limit exceeded")
		}
		if !safeRelativePath(header.Name) {
			return errors.New("artifact archive: unsafe entry path")
		}
		if _, exists := seen[header.Name]; exists {
			return errors.New("artifact archive: duplicate entry path")
		}
		seen[header.Name] = struct{}{}
		switch header.Typeflag {
		case tar.TypeReg, byte(0):
			if header.Size < 0 || header.Size > limits.MaxBytes-total {
				return errors.New("artifact archive: expanded size limit exceeded")
			}
			total += header.Size
			n, err := io.Copy(io.Discard, io.LimitReader(tr, header.Size+1))
			if err != nil || n != header.Size {
				return errors.New("artifact archive: truncated entry")
			}
		case tar.TypeDir:
			if header.Size != 0 {
				return errors.New("artifact archive: directory has content")
			}
		default:
			return errors.New("artifact archive: links and special files are forbidden")
		}
	}
	if entries == 0 {
		return errors.New("artifact archive: empty archive")
	}
	return nil
}

func BuildCustodyRecord(record ArtifactCustodyRecord) (ArtifactCustodyRecord, error) {
	if !identityPattern.MatchString(record.ManifestID) || !identityPattern.MatchString(record.ObjectID) ||
		!identityPattern.MatchString(record.From) || !identityPattern.MatchString(record.To) ||
		record.From == record.To || record.Sequence < 1 || record.Size < 0 ||
		!validSHA256(record.SHA256) || record.VerifiedAt.IsZero() {
		return ArtifactCustodyRecord{}, errors.New("artifact custody: invalid record")
	}
	if record.PreviousSHA256 != "" && !validSHA256(record.PreviousSHA256) {
		return ArtifactCustodyRecord{}, errors.New("artifact custody: invalid previous record checksum")
	}
	record.RecordSHA256 = ""
	canonical := fmt.Appendf(nil, "%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%s\x00%s\x00%s",
		record.ManifestID, record.ObjectID, record.From, record.To, record.Sequence,
		record.Size, strings.ToLower(record.SHA256), record.VerifiedAt.UTC().Format(time.RFC3339Nano),
		strings.ToLower(record.PreviousSHA256))
	sum := sha256.Sum256(canonical)
	record.RecordSHA256 = hex.EncodeToString(sum[:])
	return record, nil
}

func ValidateCustodyRecord(record ArtifactCustodyRecord) error {
	want, err := BuildCustodyRecord(ArtifactCustodyRecord{
		ManifestID: record.ManifestID, ObjectID: record.ObjectID, From: record.From,
		To: record.To, Sequence: record.Sequence, Size: record.Size, SHA256: record.SHA256,
		VerifiedAt: record.VerifiedAt, PreviousSHA256: record.PreviousSHA256,
	})
	if err != nil {
		return err
	}
	if !strings.EqualFold(want.RecordSHA256, record.RecordSHA256) {
		return errors.New("artifact custody: record checksum mismatch")
	}
	return nil
}

func safeRelativePath(value string) bool {
	if value == "" || strings.Contains(value, "\\") || strings.ContainsRune(value, 0) ||
		strings.HasPrefix(value, "/") || path.Clean(value) != value {
		return false
	}
	for part := range strings.SplitSeq(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func safeOpaqueValue(value string, max int) bool {
	if value == "" || len(value) > max || strings.ContainsRune(value, 0) {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
