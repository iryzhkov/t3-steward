package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type coordinatorArtifactDownloadTransport interface {
	RoundTripArtifactDownloadWithRetry(context.Context, workerproto.Envelope, workerproto.RetryPolicy, io.ReadSeeker, int64) (workerproto.Envelope, error)
}

type coordinatorDeliveringOfferBuilder struct {
	Base             backlog.AssignmentOfferBuilder
	Transport        coordinatorArtifactDownloadTransport
	Artifacts        backlog.CoordinatorArtifactStore
	CoordinatorID    string
	CoordinatorEpoch int64
	WorkerID         string
	WorkerEpoch      string
	SignerPrincipal  string
	SignerKeyID      string
	SignerSecret     []byte
	RequestTimeout   time.Duration
	RetryPolicy      workerproto.RetryPolicy
	MaxArtifactBytes int64
	MaxTotalBytes    int64
	Now              func() time.Time
}

func (b coordinatorDeliveringOfferBuilder) BuildAssignmentOffer(ctx context.Context, assignment domain.Assignment, expiresAt time.Time) (workerproto.AssignmentOffer, error) {
	if b.Base == nil || b.Transport == nil || b.Now == nil || b.RequestTimeout <= 0 ||
		b.CoordinatorID == "" || b.CoordinatorEpoch < 1 || b.WorkerID == "" || b.WorkerEpoch == "" ||
		b.SignerPrincipal == "" || b.SignerKeyID == "" || len(b.SignerSecret) < 16 ||
		b.MaxArtifactBytes < 1 || b.MaxTotalBytes < b.MaxArtifactBytes {
		return workerproto.AssignmentOffer{}, errors.New("artifact-delivering offer builder is not fully configured")
	}
	offer, err := b.Base.BuildAssignmentOffer(ctx, assignment, expiresAt)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	if err := b.deliver(ctx, offer); err != nil {
		return workerproto.AssignmentOffer{}, fmt.Errorf("deliver assignment artifacts: %w", err)
	}
	return offer, nil
}

func (b coordinatorDeliveringOfferBuilder) deliver(ctx context.Context, offer workerproto.AssignmentOffer) error {
	objects := executionPackageObjects(offer.Package.Package)
	now := b.Now().UTC()
	manifest := workerproto.ArtifactTransferManifest{
		Version: workerproto.ArtifactManifestVersion, Direction: "download",
		CoordinatorEpoch: b.CoordinatorEpoch, WorkerID: b.WorkerID, WorkerEpoch: b.WorkerEpoch,
		AssignmentID: offer.Assignment.ID, AssignmentEpoch: offer.Assignment.Epoch,
		Objects: objects, CreatedAt: now, ExpiresAt: offer.ExpiresAt,
	}
	for _, object := range objects {
		if object.Size > b.MaxTotalBytes-manifest.TotalBytes {
			return errors.New("artifact download exceeds aggregate limit")
		}
		manifest.TotalBytes += object.Size
	}
	identity, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(identity)
	manifest.ID = "download-" + hex.EncodeToString(sum[:16])
	if err := workerproto.ValidateArtifactTransferManifest(manifest, b.MaxArtifactBytes, b.MaxTotalBytes, now); err != nil {
		return err
	}

	stage, err := os.CreateTemp("", "t3-steward-artifact-download-*")
	if err != nil {
		return err
	}
	stagePath := stage.Name()
	defer func() {
		_ = stage.Close()
		_ = os.Remove(stagePath)
	}()
	for _, object := range objects {
		artifact, content, err := b.Artifacts.Open(ctx, object.ID)
		if err != nil {
			return err
		}
		if artifact.ID != object.ID || artifact.Size != object.Size || artifact.SHA256 != object.SHA256 {
			content.Close()
			return fmt.Errorf("artifact %q metadata changed before delivery", object.ID)
		}
		written, copyErr := io.CopyN(stage, content, object.Size)
		closeErr := content.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if written != object.Size {
			return fmt.Errorf("artifact %q was truncated", object.ID)
		}
	}
	if err := stage.Sync(); err != nil {
		return err
	}
	if _, err := stage.Seek(0, io.SeekStart); err != nil {
		return err
	}

	sessionID, err := newCoordinatorWorkerSessionID(b.CoordinatorID, b.WorkerID)
	if err != nil {
		return err
	}
	deadline := now.Add(b.RequestTimeout)
	if offer.ExpiresAt.Before(deadline) {
		deadline = offer.ExpiresAt
	}
	request, err := workerproto.NewEnvelope(
		workerproto.MessageArtifactDownload, sessionID, sessionID+"-1",
		b.CoordinatorID, b.WorkerID, b.CoordinatorEpoch, b.WorkerEpoch, 1,
		now, deadline, manifest,
	)
	if err != nil {
		return err
	}
	if err := workerproto.SignEnvelope(&request, b.SignerPrincipal, b.SignerKeyID, b.SignerSecret); err != nil {
		return err
	}
	response, err := b.Transport.RoundTripArtifactDownloadWithRetry(ctx, request, b.RetryPolicy, stage, manifest.TotalBytes)
	if err != nil {
		return err
	}
	if response.Type == workerproto.MessageError {
		var protocolErr workerproto.ProtocolError
		if err := workerproto.DecodePayload(response, workerproto.MessageError, &protocolErr); err != nil {
			return err
		}
		return &protocolErr
	}
	var receipt workerproto.ArtifactDownloadReceipt
	if err := workerproto.DecodePayload(response, workerproto.MessageArtifactDownload, &receipt); err != nil {
		return err
	}
	return validateCoordinatorDownloadReceipt(manifest, receipt, b.CoordinatorID)
}

func executionPackageObjects(pkg workerproto.ExecutionPackage) []workerproto.ArtifactObject {
	objects := []workerproto.ArtifactObject{pkg.Prompt}
	objects = append(objects, pkg.StaticInputs...)
	for _, dependency := range pkg.Dependencies {
		objects = append(objects, dependency.Artifacts...)
	}
	return objects
}

func validateCoordinatorDownloadReceipt(manifest workerproto.ArtifactTransferManifest, receipt workerproto.ArtifactDownloadReceipt, coordinatorID string) error {
	if receipt.ManifestID != manifest.ID || len(receipt.Custody) != len(manifest.Objects) {
		return errors.New("artifact download receipt does not match manifest")
	}
	previous := ""
	for index, record := range receipt.Custody {
		object := manifest.Objects[index]
		if err := workerproto.ValidateCustodyRecord(record); err != nil {
			return err
		}
		if record.ManifestID != manifest.ID || record.ObjectID != object.ID ||
			record.From != "coordinator:"+coordinatorID || record.To != "worker:"+manifest.WorkerID ||
			record.Sequence != int64(index+1) || record.Size != object.Size ||
			record.SHA256 != object.SHA256 || record.PreviousSHA256 != previous {
			return errors.New("artifact download custody chain changed")
		}
		previous = record.RecordSHA256
	}
	return nil
}

var _ backlog.AssignmentOfferBuilder = coordinatorDeliveringOfferBuilder{}
var _ coordinatorArtifactDownloadTransport = (*workerproto.SSHTransport)(nil)
