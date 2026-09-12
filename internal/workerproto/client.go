package workerproto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// RoundTripper is the authenticated request boundary used by a coordinator
// protocol client. Implementations must retry only the exact supplied envelope.
type RoundTripper interface {
	RoundTripWithRetry(context.Context, Envelope, RetryPolicy) (Envelope, error)
}

type ArtifactRoundTripper interface {
	RoundTripArtifactWithRetry(context.Context, Envelope, RetryPolicy, int64) (Envelope, []byte, error)
}

type fetchedArtifactObject struct {
	object ArtifactObject
	data   []byte
}

// FetchedArtifactUpload owns one fully buffered, bounded and verified raw
// transfer. It satisfies the coordinator result importer's opener boundary.
type FetchedArtifactUpload struct {
	Response ArtifactUploadResponse
	objects  map[string]fetchedArtifactObject
}

func (f FetchedArtifactUpload) OpenWorkerUpload(ctx context.Context, object ArtifactObject) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fetched, exists := f.objects[object.ID]
	if !exists || !reflect.DeepEqual(fetched.object, object) {
		return nil, errors.New("worker protocol client: fetched artifact identity mismatch")
	}
	return io.NopCloser(bytes.NewReader(fetched.data)), nil
}

// ClientConfig fixes one coordinator-to-worker protocol session.
type ClientConfig struct {
	CoordinatorID    string
	WorkerID         string
	CoordinatorEpoch int64
	WorkerEpoch      string
	SessionID        string
	RequestTimeout   time.Duration
	SignerPrincipal  string
	SignerKeyID      string
	SignerSecret     []byte
	RetryPolicy      RetryPolicy
	Transport        RoundTripper
	Now              func() time.Time
}

// Client serializes a single protocol session. An exhausted exchange makes the
// session unusable: whether the worker committed it is ambiguous, so callers
// must reconcile through a fresh session instead of risking a sequence gap.
type Client struct {
	config ClientConfig

	mu       sync.Mutex
	sequence int64
	unusable bool
}

func NewClient(config ClientConfig) (*Client, error) {
	if config.CoordinatorID == "" || config.WorkerID == "" || config.CoordinatorEpoch < 1 ||
		config.WorkerEpoch == "" || config.SessionID == "" {
		return nil, errors.New("worker protocol client: complete session identity is required")
	}
	if config.RequestTimeout <= 0 || config.SignerPrincipal == "" || config.SignerKeyID == "" ||
		len(config.SignerSecret) < 16 || config.RetryPolicy.MaxAttempts < 1 || config.Transport == nil {
		return nil, errors.New("worker protocol client: transport, authentication, timeout, and retry policy are required")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	config.SignerSecret = append([]byte(nil), config.SignerSecret...)
	return &Client{config: config}, nil
}

func (c *Client) Snapshot(ctx context.Context) (domain.WorkerSnapshot, error) {
	var observations Observations
	if err := c.exchange(ctx, MessageSnapshot, MessageObservations, SnapshotRequest{}, &observations); err != nil {
		return domain.WorkerSnapshot{}, err
	}
	return observations.Snapshot, nil
}

func (c *Client) DeliverOffers(ctx context.Context, offers []AssignmentOffer) ([]domain.AssignmentClaimRequest, error) {
	var claims AssignmentClaims
	if err := c.exchange(ctx, MessageOffers, MessageClaims, AssignmentOffers{Offers: offers}, &claims); err != nil {
		return nil, err
	}
	return claims.Claims, nil
}

func (c *Client) DeliverLeaseRenewals(ctx context.Context, renewals []domain.AssignmentLeaseRenewal) (domain.WorkerSnapshot, error) {
	var observations Observations
	if err := c.exchange(ctx, MessageLeaseRenewals, MessageObservations, LeaseRenewals{Renewals: renewals}, &observations); err != nil {
		return domain.WorkerSnapshot{}, err
	}
	return observations.Snapshot, nil
}

// DeliverWorkerCommands satisfies the backlog coordinator transport boundary.
func (c *Client) DeliverWorkerCommands(ctx context.Context, snapshot domain.WorkerSnapshot, commands []domain.WorkerCommand) ([]domain.WorkerAcknowledgement, error) {
	if err := c.requireSnapshot(snapshot); err != nil {
		return nil, err
	}
	var acknowledgements Acknowledgements
	if err := c.exchange(ctx, MessageCommands, MessageAcknowledgements, CommandDelivery{Commands: commands}, &acknowledgements); err != nil {
		return acknowledgements.Acknowledgements, err
	}
	return acknowledgements.Acknowledgements, nil
}

func (c *Client) DeliverThrottle(ctx context.Context, snapshot domain.WorkerSnapshot, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
	if err := c.requireSnapshot(snapshot); err != nil {
		return nil, err
	}
	var acknowledgements ThrottleAcknowledgements
	if err := c.exchange(ctx, MessageThrottleCommands, MessageThrottleAcknowledgements, ThrottleDelivery{Commands: commands}, &acknowledgements); err != nil {
		return acknowledgements.Acknowledgements, err
	}
	return acknowledgements.Acknowledgements, nil
}

// PollArtifact asks for one pending immutable upload. The worker advances to a
// later upload only after the coordinator acknowledges successful import.
func (c *Client) PollArtifact(ctx context.Context, purpose string, exclude ...string) (*ArtifactUploadResponse, error) {
	if purpose != "result" && purpose != "checkpoint" {
		return nil, errors.New("worker protocol client: unsupported artifact purpose")
	}
	var announcement ArtifactAnnouncement
	if err := c.exchange(ctx, MessageArtifactPoll, MessageArtifactAnnouncement, ArtifactPollRequest{Purpose: purpose, Exclude: exclude}, &announcement); err != nil {
		return nil, err
	}
	return announcement.Upload, nil
}

func (c *Client) AcknowledgeArtifact(ctx context.Context, manifestID string) error {
	if manifestID == "" {
		return errors.New("worker protocol client: artifact manifest ID is required")
	}
	var acknowledgement ArtifactAcknowledgement
	if err := c.exchange(ctx, MessageArtifactAcknowledge, MessageArtifactAcknowledged, ArtifactAcknowledgeRequest{ManifestID: manifestID}, &acknowledgement); err != nil {
		return err
	}
	if acknowledgement.ManifestID != manifestID {
		return errors.New("worker protocol client: artifact acknowledgement identity changed")
	}
	return nil
}

// FetchArtifact uses a dedicated raw-stream transport session to retrieve the
// exact upload previously announced over the control session.
func (c *Client) FetchArtifact(ctx context.Context, announced ArtifactUploadResponse, maxArtifactBytes, maxTotalBytes int64) (FetchedArtifactUpload, error) {
	manifest := announced.Manifest
	if maxArtifactBytes < 1 || maxTotalBytes < maxArtifactBytes {
		return FetchedArtifactUpload{}, errors.New("worker protocol client: positive artifact limits are required")
	}
	if err := ValidateArtifactTransferManifest(manifest, maxArtifactBytes, maxTotalBytes, c.config.Now().UTC()); err != nil {
		return FetchedArtifactUpload{}, err
	}
	if manifest.Direction != "upload" || manifest.CoordinatorEpoch < 1 || manifest.CoordinatorEpoch > c.config.CoordinatorEpoch ||
		manifest.WorkerID != c.config.WorkerID || manifest.WorkerEpoch != c.config.WorkerEpoch {
		return FetchedArtifactUpload{}, errors.New("worker protocol client: announced artifact authority mismatch")
	}
	transport, ok := c.config.Transport.(ArtifactRoundTripper)
	if !ok {
		return FetchedArtifactUpload{}, errors.New("worker protocol client: raw artifact transport is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unusable {
		return FetchedArtifactUpload{}, errors.New("worker protocol client: session is unusable after an ambiguous exchange")
	}
	if err := ctx.Err(); err != nil {
		return FetchedArtifactUpload{}, err
	}
	c.sequence++
	now := c.config.Now().UTC()
	requestID := c.config.SessionID + "-" + strconv.FormatInt(c.sequence, 10)
	objectIDs := make([]string, 0, len(manifest.Objects))
	for _, object := range manifest.Objects {
		objectIDs = append(objectIDs, object.ID)
	}
	request, err := NewEnvelope(
		MessageArtifactUpload, c.config.SessionID, requestID,
		c.config.CoordinatorID, c.config.WorkerID,
		c.config.CoordinatorEpoch, c.config.WorkerEpoch, c.sequence,
		now, now.Add(c.config.RequestTimeout),
		ArtifactDownloadRequest{ManifestID: manifest.ID, ObjectIDs: objectIDs},
	)
	if err != nil {
		return FetchedArtifactUpload{}, err
	}
	if err := SignEnvelope(&request, c.config.SignerPrincipal, c.config.SignerKeyID, c.config.SignerSecret); err != nil {
		return FetchedArtifactUpload{}, err
	}
	responseEnvelope, raw, err := transport.RoundTripArtifactWithRetry(ctx, request, c.config.RetryPolicy, maxTotalBytes)
	if err != nil {
		c.unusable = true
		return FetchedArtifactUpload{}, fmt.Errorf("worker protocol client: artifact exchange %s: %w", requestID, err)
	}
	if responseEnvelope.Type == MessageError {
		var protocolErr ProtocolError
		if err := DecodePayload(responseEnvelope, MessageError, &protocolErr); err != nil {
			return FetchedArtifactUpload{}, err
		}
		return FetchedArtifactUpload{}, &protocolErr
	}
	var response ArtifactUploadResponse
	if err := DecodePayload(responseEnvelope, MessageArtifactUpload, &response); err != nil {
		return FetchedArtifactUpload{}, err
	}
	if !reflect.DeepEqual(response, announced) {
		return FetchedArtifactUpload{}, errors.New("worker protocol client: fetched artifact metadata changed after announcement")
	}
	if int64(len(raw)) != manifest.TotalBytes {
		return FetchedArtifactUpload{}, errors.New("worker protocol client: fetched artifact payload size mismatch")
	}
	fetched := FetchedArtifactUpload{Response: response, objects: make(map[string]fetchedArtifactObject, len(manifest.Objects))}
	offset := int64(0)
	for _, object := range manifest.Objects {
		next := offset + object.Size
		data := append([]byte(nil), raw[offset:next]...)
		if err := VerifyArtifact(bytes.NewReader(data), object, maxArtifactBytes); err != nil {
			return FetchedArtifactUpload{}, fmt.Errorf("worker protocol client: verify fetched artifact %q: %w", object.ID, err)
		}
		fetched.objects[object.ID] = fetchedArtifactObject{object: object, data: data}
		offset = next
	}
	return fetched, nil
}

func (c *Client) requireSnapshot(snapshot domain.WorkerSnapshot) error {
	if snapshot.WorkerID != c.config.WorkerID || snapshot.WorkerEpoch != c.config.WorkerEpoch ||
		snapshot.CoordinatorEpoch != c.config.CoordinatorEpoch {
		return errors.New("worker protocol client: snapshot identity does not match session")
	}
	return nil
}

func (c *Client) exchange(ctx context.Context, requestType, responseType MessageType, payload, result any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unusable {
		return errors.New("worker protocol client: session is unusable after an ambiguous exchange")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.sequence++
	now := c.config.Now().UTC()
	requestID := c.config.SessionID + "-" + strconv.FormatInt(c.sequence, 10)
	request, err := NewEnvelope(
		requestType, c.config.SessionID, requestID,
		c.config.CoordinatorID, c.config.WorkerID,
		c.config.CoordinatorEpoch, c.config.WorkerEpoch, c.sequence,
		now, now.Add(c.config.RequestTimeout), payload,
	)
	if err != nil {
		return err
	}
	if err := SignEnvelope(&request, c.config.SignerPrincipal, c.config.SignerKeyID, c.config.SignerSecret); err != nil {
		return err
	}
	response, err := c.config.Transport.RoundTripWithRetry(ctx, request, c.config.RetryPolicy)
	if err != nil {
		c.unusable = true
		return fmt.Errorf("worker protocol client: exchange %s: %w", requestID, err)
	}
	if response.Type == MessageError {
		var protocolErr ProtocolError
		if err := DecodePayload(response, MessageError, &protocolErr); err != nil {
			return err
		}
		return &protocolErr
	}
	if response.Type != responseType {
		return &ProtocolError{Code: ErrorMalformed, Message: fmt.Sprintf("unexpected response type %q", response.Type), RequestID: requestID}
	}
	if err := DecodePayload(response, responseType, result); err != nil {
		return err
	}
	return nil
}
