package workerruntime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ServeArtifactReceive authenticates a manifest envelope, consumes exactly its
// ordered raw object bytes, commits worker custody, and returns a signed receipt.
func (s *WorkerService) ServeArtifactReceive(ctx context.Context, input io.Reader, output io.Writer) error {
	if s == nil || s.Exchange.Server == nil || s.Exchange.Custody == nil {
		return errors.New("artifact receive: worker service is not initialized")
	}
	envelope, buffered, err := readStreamEnvelope(input, s.Codec)
	if err != nil {
		return err
	}
	var manifest workerproto.ArtifactTransferManifest
	if err := workerproto.DecodePayload(envelope, workerproto.MessageArtifactDownload, &manifest); err != nil {
		return err
	}
	consumed := false
	receive := func() ([]workerproto.ArtifactCustodyRecord, error) {
		readers := make(map[string]io.Reader, len(manifest.Objects))
		for _, object := range manifest.Objects {
			readers[object.ID] = io.LimitReader(buffered, object.Size)
		}
		custody, err := s.Exchange.Custody.ReceiveDownload(ctx, manifest, readers)
		if err != nil {
			return nil, err
		}
		if err := requireStreamEOF(buffered); err != nil {
			return nil, err
		}
		consumed = true
		return custody, nil
	}
	response, err := s.Exchange.Server.Handle(ctx, envelope, func(_ context.Context, _ workerproto.Envelope) (workerproto.MessageType, any, error) {
		custody, err := receive()
		return workerproto.MessageArtifactDownload, workerproto.ArtifactDownloadReceipt{
			ManifestID: manifest.ID,
			Custody:    custody,
		}, err
	})
	if err != nil {
		return err
	}
	if !consumed && response.Type != workerproto.MessageError {
		if _, err := receive(); err != nil {
			return err
		}
	}
	return s.Codec.Encode(output, response)
}

// ServeArtifactSend authenticates one complete outbox request, returns signed
// manifest/custody metadata, and then streams exact raw objects in manifest order.
func (s *WorkerService) ServeArtifactSend(ctx context.Context, input io.Reader, output io.Writer) error {
	if s == nil || s.Exchange.Server == nil || s.Exchange.Custody == nil {
		return errors.New("artifact send: worker service is not initialized")
	}
	envelope, buffered, err := readStreamEnvelope(input, s.Codec)
	if err != nil {
		return err
	}
	if err := requireStreamEOF(buffered); err != nil {
		return err
	}
	var request workerproto.ArtifactDownloadRequest
	response, err := s.Exchange.Server.Handle(ctx, envelope, func(_ context.Context, authenticated workerproto.Envelope) (workerproto.MessageType, any, error) {
		if err := workerproto.DecodePayload(authenticated, workerproto.MessageArtifactUpload, &request); err != nil {
			return "", nil, err
		}
		pending, err := s.Exchange.Custody.PendingUploadFor(request)
		if err != nil {
			return "", nil, err
		}
		return workerproto.MessageArtifactUpload, workerproto.ArtifactUploadResponse{
			Manifest: pending.Manifest,
			Custody:  pending.Custody,
		}, nil
	})
	if err != nil {
		return err
	}
	if err := s.Codec.Encode(output, response); err != nil {
		return err
	}
	if response.Type == workerproto.MessageError {
		return nil
	}
	if request.ManifestID == "" {
		if err := workerproto.DecodePayload(envelope, workerproto.MessageArtifactUpload, &request); err != nil {
			return err
		}
	}
	pending, err := s.Exchange.Custody.PendingUploadFor(request)
	if err != nil {
		return err
	}
	for _, object := range pending.Manifest.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader, err := s.Exchange.Custody.OpenArtifact(ctx, object)
		if err != nil {
			return err
		}
		written, copyErr := io.CopyN(output, reader, object.Size)
		closeErr := reader.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if written != object.Size {
			return errors.New("artifact send: short object")
		}
	}
	return nil
}

func readStreamEnvelope(input io.Reader, codec workerproto.Codec) (workerproto.Envelope, *bufio.Reader, error) {
	if input == nil {
		return workerproto.Envelope{}, nil, errors.New("artifact stream: input is required")
	}
	buffered := bufio.NewReader(input)
	var header bytes.Buffer
	for {
		fragment, err := buffered.ReadSlice('\n')
		if int64(header.Len()+len(fragment)) > codec.MaxBytes {
			return workerproto.Envelope{}, nil, errors.New("artifact stream: envelope exceeds limit")
		}
		header.Write(fragment)
		if err == nil {
			break
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return workerproto.Envelope{}, nil, errors.New("artifact stream: incomplete envelope")
		}
	}
	var envelope workerproto.Envelope
	if err := codec.Decode(bytes.NewReader(header.Bytes()), &envelope); err != nil {
		return workerproto.Envelope{}, nil, err
	}
	return envelope, buffered, nil
}

func requireStreamEOF(reader *bufio.Reader) error {
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("artifact stream: trailing bytes")
	}
	return nil
}
