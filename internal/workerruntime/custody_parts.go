package workerruntime

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"io"
	"os"
)

func (s *CustodyStore) splitResultObjects(pkg workerproto.ExecutionPackage, finalized backlog.FinalizedAttempt, planned []resultObject) ([]resultObject, error) {
	limits := s.resultLimits(pkg)
	planned = append([]resultObject(nil), planned...)
	var total int64
	for _, entry := range planned {
		if entry.object.Size > limits.total-total {
			return nil, workerproto.NewArtifactSizeError("aggregate", limits.total, uint64(total)+uint64(entry.object.Size), entry.object)
		}
		total += entry.object.Size
	}
	for index := range planned {
		entry := &planned[index]
		limit := min(limits.object, workerproto.StreamArtifactLimit)
		if entry.object.Size <= workerproto.StreamArtifactLimit || len(entry.object.Parts) > 0 {
			continue
		}
		// An optional finalizer metadata probe has no bytes to split yet.
		if entry.artifact != nil && finalized.StorageDir == "" {
			continue
		}
		var reader io.Reader = bytes.NewReader(entry.data)
		var file *os.File
		if entry.artifact != nil {
			path, err := finalizedArtifactPath(finalized, *entry.artifact)
			if err != nil {
				return nil, err
			}
			file, err = openRegular(path)
			if err != nil {
				return nil, err
			}
			reader = file
		}
		object, err := workerproto.SplitArtifact(reader, entry.object, limit)
		if file != nil {
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err != nil {
			return nil, fmt.Errorf("%s: split: %w", entry.failure, err)
		}
		entry.object = object
	}
	return planned, nil
}

type splitArtifactReader struct {
	io.Reader
	files []*os.File
}

func (r *splitArtifactReader) Close() error {
	var errs []error
	for _, file := range r.files {
		errs = append(errs, file.Close())
	}
	return errors.Join(errs...)
}
func (s *CustodyStore) openSplitArtifact(object workerproto.ArtifactObject) (io.ReadCloser, error) {
	if err := workerproto.ValidateArtifactObject(object, s.config.MaxArtifactBytes); err != nil {
		return nil, err
	}
	if object.Size > s.config.MaxTotalBytes {
		return nil, errors.New("artifact exceeds aggregate limit")
	}
	result := &splitArtifactReader{}
	fail := func(err error) (io.ReadCloser, error) { _ = result.Close(); return nil, err }
	var readers []io.Reader
	for index := range object.Parts {
		part := workerproto.ArtifactPartObject(object, index)
		file, err := openRegular(s.objectPath(part.SHA256))
		if err != nil {
			return fail(err)
		}
		result.files = append(result.files, file)
		readers = append(readers, file)
	}
	if err := workerproto.VerifyArtifact(io.MultiReader(readers...), object, s.config.MaxArtifactBytes); err != nil {
		return fail(err)
	}
	for _, file := range result.files {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return fail(err)
		}
	}
	result.Reader = io.MultiReader(readers...)
	return result, nil
}
func (s *CustodyStore) storeSplitArtifact(reader io.Reader, object workerproto.ArtifactObject) error {
	if err := workerproto.ValidateArtifactObject(object, s.config.MaxArtifactBytes); err != nil {
		return err
	}
	if object.Size > s.config.MaxTotalBytes {
		return errors.New("artifact exceeds aggregate limit")
	}
	for index := range object.Parts {
		part := workerproto.ArtifactPartObject(object, index)
		if err := s.storeObject(io.LimitReader(reader, part.Size), part); err != nil {
			return fmt.Errorf("store part %d: %w", index, err)
		}
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || err != io.EOF {
		return errors.New("split artifact: trailing bytes")
	}
	assembled, err := s.openSplitArtifact(object)
	if err != nil {
		return err
	}
	return assembled.Close()
}
