package backlog

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type ArchiveSubmission struct {
	IdempotencyKey string
	Archive        io.Reader
}

// SubmitArchive validates and expands one uncompressed tar bundle into a
// disposable root before using the same durable directory submission path.
func (s *SubmissionService) SubmitArchive(ctx context.Context, request ArchiveSubmission) (SubmissionResult, error) {
	if s == nil || s.MaxBytes < 1 || s.MaxFiles < 1 {
		return SubmissionResult{}, errors.New("positive submission byte and file limits are required")
	}
	if request.Archive == nil {
		return SubmissionResult{}, errors.New("submission archive is required")
	}
	raw, err := io.ReadAll(io.LimitReader(request.Archive, s.MaxBytes+1))
	if err != nil {
		return SubmissionResult{}, fmt.Errorf("read submission archive: %w", err)
	}
	if int64(len(raw)) > s.MaxBytes {
		return SubmissionResult{}, fmt.Errorf("submission archive exceeds %d bytes", s.MaxBytes)
	}
	limits := workerproto.ArchiveLimits{MaxEntries: s.MaxFiles, MaxBytes: s.MaxBytes}
	if err := workerproto.ValidateTarArchive(bytes.NewReader(raw), int64(len(raw)), limits); err != nil {
		return SubmissionResult{}, err
	}
	root, err := os.MkdirTemp("", "t3-steward-submission-")
	if err != nil {
		return SubmissionResult{}, fmt.Errorf("create submission extraction root: %w", err)
	}
	defer os.RemoveAll(root)
	if err := extractValidatedSubmissionTar(ctx, raw, root); err != nil {
		return SubmissionResult{}, err
	}
	return s.SubmitDirectory(ctx, DirectorySubmission{
		IdempotencyKey: request.IdempotencyKey,
		BundleDir:      root,
	})
}

func extractValidatedSubmissionTar(ctx context.Context, raw []byte, root string) error {
	reader := tar.NewReader(bytes.NewReader(raw))
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read validated submission archive: %w", err)
		}
		target := filepath.Join(root, filepath.FromSlash(header.Name))
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return fmt.Errorf("create submission directory %q: %w", header.Name, err)
			}
		case tar.TypeReg, byte(0):
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return fmt.Errorf("create submission parent %q: %w", header.Name, err)
			}
			file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				return fmt.Errorf("create submission file %q: %w", header.Name, err)
			}
			written, copyErr := io.Copy(file, io.LimitReader(reader, header.Size+1))
			closeErr := file.Close()
			if copyErr != nil {
				return fmt.Errorf("extract submission file %q: %w", header.Name, copyErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close submission file %q: %w", header.Name, closeErr)
			}
			if written != header.Size {
				return fmt.Errorf("extract submission file %q: size changed after validation", header.Name)
			}
		default:
			return fmt.Errorf("submission archive entry %q changed after validation", header.Name)
		}
	}
}
