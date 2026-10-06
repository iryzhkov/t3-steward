package workerruntime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"io"
	"os"
	"path/filepath"
)

type publishedBundleExporter interface {
	ExportPublishedBundle(context.Context, backlog.CommitProvenance, int64) (string, error)
}

func (s *WorkerService) serveCommitBundle(ctx context.Context, envelope workerproto.Envelope, input *bufio.Reader, output io.Writer) error {
	if err := requireStreamEOF(input); err != nil {
		return err
	}
	var path string
	defer func() {
		if path != "" {
			_ = os.RemoveAll(filepath.Dir(path))
		}
	}()
	produce := func() (workerproto.CommitBundleResponse, error) {
		var r workerproto.CommitBundleRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageCommitBundle, &r); err != nil {
			return workerproto.CommitBundleResponse{}, err
		}
		if err := workerproto.ValidateCommitBundleRequest(r); err != nil {
			return workerproto.CommitBundleResponse{}, err
		}
		p, err := backlog.ParseCommitProvenance(r.Provenance)
		if err != nil {
			return workerproto.CommitBundleResponse{}, err
		}
		exporter, ok := s.Exchange.Runtime.config.CampaignRefs.(publishedBundleExporter)
		if !ok {
			return workerproto.CommitBundleResponse{}, errors.New("worker commit export: published source unavailable")
		}
		path, err = exporter.ExportPublishedBundle(ctx, p, r.MaxBytes)
		if err != nil {
			return workerproto.CommitBundleResponse{}, err
		}
		file, err := os.Open(path)
		if err != nil {
			return workerproto.CommitBundleResponse{}, err
		}
		defer file.Close()
		h := sha256.New()
		n, err := io.Copy(h, file)
		if err != nil {
			return workerproto.CommitBundleResponse{}, err
		}
		return workerproto.CommitBundleResponse{Size: n, SHA256: fmt.Sprintf("%x", h.Sum(nil))}, nil
	}
	response, err := s.Exchange.Server.Handle(ctx, envelope, func(context.Context, workerproto.Envelope) (workerproto.MessageType, any, error) {
		metadata, err := produce()
		return workerproto.MessageCommitBundle, metadata, err
	})
	if err != nil {
		return err
	}
	if response.Type == workerproto.MessageError {
		return s.Codec.Encode(output, response)
	}
	if path == "" {
		metadata, err := produce()
		if err != nil {
			return err
		}
		var recorded workerproto.CommitBundleResponse
		if err := workerproto.DecodePayload(response, workerproto.MessageCommitBundle, &recorded); err != nil {
			return err
		}
		if recorded != metadata {
			return errors.New("worker commit export: replayed source changed")
		}
	}
	if err := s.Codec.Encode(output, response); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(output, file)
	return err
}
