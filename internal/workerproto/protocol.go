package workerproto

import (
	"maps"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const Version = 1

type MessageType string

const (
	MessageCapabilities     MessageType = "capabilities"
	MessageSnapshot         MessageType = "snapshot"
	MessageOffers           MessageType = "offers"
	MessageClaims           MessageType = "claims"
	MessageLeaseRenewals    MessageType = "lease-renewals"
	MessageCommands         MessageType = "commands"
	MessageAcknowledgements MessageType = "acknowledgements"
	MessageObservations     MessageType = "observations"
	MessageArtifactUpload   MessageType = "artifact-upload"
	MessageArtifactDownload MessageType = "artifact-download"
	MessageError            MessageType = "error"
)

type Authentication struct {
	Principal string `json:"principal"`
	KeyID     string `json:"keyId"`
	Signature string `json:"signature"`
}

type Envelope struct {
	Version          int             `json:"version"`
	Type             MessageType     `json:"type"`
	SessionID        string          `json:"sessionId"`
	RequestID        string          `json:"requestId"`
	InReplyTo        string          `json:"inReplyTo,omitempty"`
	Sender           string          `json:"sender"`
	Recipient        string          `json:"recipient"`
	CoordinatorEpoch int64           `json:"coordinatorEpoch"`
	WorkerEpoch      string          `json:"workerEpoch"`
	Sequence         int64           `json:"sequence"`
	SentAt           time.Time       `json:"sentAt"`
	Deadline         time.Time       `json:"deadline"`
	PayloadSHA256    string          `json:"payloadSha256"`
	Authentication   Authentication  `json:"authentication"`
	Payload          json.RawMessage `json:"payload,omitempty"`
}

type CapabilityNegotiation struct {
	SupportedVersions []int    `json:"supportedVersions"`
	Capabilities      []string `json:"capabilities,omitempty"`
	MaxMessageBytes   int64    `json:"maxMessageBytes"`
	MaxArtifactBytes  int64    `json:"maxArtifactBytes"`
}

type AssignmentOffer struct {
	Assignment domain.Assignment        `json:"assignment"`
	Package    ExecutionPackageManifest `json:"package"`
	ExpiresAt  time.Time                `json:"expiresAt"`
}

type AssignmentOffers struct {
	Offers []AssignmentOffer `json:"offers"`
}

type AssignmentClaims struct {
	Claims []domain.AssignmentClaimRequest `json:"claims"`
}

type LeaseRenewals struct {
	Renewals []domain.AssignmentLeaseRenewal `json:"renewals"`
}

type CommandDelivery struct {
	Commands []domain.WorkerCommand              `json:"commands"`
	Packages map[string]ExecutionPackageManifest `json:"packages,omitempty"`
}

type Acknowledgements struct {
	Acknowledgements []domain.WorkerAcknowledgement `json:"acknowledgements"`
}

type Observations struct {
	Snapshot domain.WorkerSnapshot `json:"snapshot"`
}

type ErrorCode string

const (
	ErrorUnsupportedVersion ErrorCode = "unsupported-version"
	ErrorStaleEpoch         ErrorCode = "stale-epoch"
	ErrorAuthentication     ErrorCode = "authentication"
	ErrorAuthorization      ErrorCode = "authorization"
	ErrorReplay             ErrorCode = "replay"
	ErrorReordered          ErrorCode = "reordered"
	ErrorMalformed          ErrorCode = "malformed"
	ErrorLimit              ErrorCode = "limit"
	ErrorTimeout            ErrorCode = "timeout"
	ErrorCancelled          ErrorCode = "cancelled"
	ErrorBackpressure       ErrorCode = "backpressure"
	ErrorInternal           ErrorCode = "internal"
)

type ProtocolError struct {
	Code       ErrorCode `json:"code"`
	Message    string    `json:"message"`
	Retryable  bool      `json:"retryable"`
	RetryAfter string    `json:"retryAfter,omitempty"`
	RequestID  string    `json:"requestId,omitempty"`
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Code) + ": " + e.Message
}

type Codec struct {
	MaxBytes int64
}

func (c Codec) Encode(w io.Writer, value any) error {
	if c.MaxBytes <= 0 {
		return errors.New("protocol codec: max bytes must be positive")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("protocol codec: encode: %w", err)
	}
	if int64(len(data)+1) > c.MaxBytes {
		return &ProtocolError{Code: ErrorLimit, Message: "encoded message exceeds limit"}
	}
	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("protocol codec: write: %w", err)
	}
	return nil
}

func (c Codec) Decode(r io.Reader, value any) error {
	if c.MaxBytes <= 0 {
		return errors.New("protocol codec: max bytes must be positive")
	}
	limited := io.LimitReader(r, c.MaxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("protocol codec: read: %w", err)
	}
	if int64(len(data)) > c.MaxBytes {
		return &ProtocolError{Code: ErrorLimit, Message: "message exceeds limit"}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return &ProtocolError{Code: ErrorMalformed, Message: "invalid JSON: " + err.Error()}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return &ProtocolError{Code: ErrorMalformed, Message: "message must contain one JSON value"}
	}
	return nil
}

func DecodePayload(envelope Envelope, want MessageType, value any) error {
	if envelope.Type != want {
		return &ProtocolError{Code: ErrorMalformed, Message: "unexpected payload type", RequestID: envelope.RequestID}
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return &ProtocolError{Code: ErrorMalformed, Message: "invalid payload JSON: " + err.Error(), RequestID: envelope.RequestID}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return &ProtocolError{Code: ErrorMalformed, Message: "payload must contain one JSON value", RequestID: envelope.RequestID}
	}
	return nil
}

func NewEnvelope(kind MessageType, sessionID, requestID, sender, recipient string, epoch int64, workerEpoch string, sequence int64, sentAt, deadline time.Time, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("protocol envelope: payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	return Envelope{
		Version: Version, Type: kind, SessionID: sessionID, RequestID: requestID,
		Sender: sender, Recipient: recipient, CoordinatorEpoch: epoch, WorkerEpoch: workerEpoch, Sequence: sequence,
		SentAt: sentAt.UTC(), Deadline: deadline.UTC(), PayloadSHA256: hex.EncodeToString(sum[:]),
		Payload: raw,
	}, nil
}

func SignEnvelope(envelope *Envelope, principal, keyID string, secret []byte) error {
	if envelope == nil || principal == "" || keyID == "" || len(secret) < 16 {
		return errors.New("protocol authentication: envelope, principal, key id, and a 16-byte secret are required")
	}
	envelope.Authentication = Authentication{Principal: principal, KeyID: keyID}
	canonical, err := signatureBytes(*envelope)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(canonical)
	envelope.Authentication.Signature = hex.EncodeToString(mac.Sum(nil))
	return nil
}

func VerifyEnvelopeSignature(envelope Envelope, secret []byte) error {
	signature, err := hex.DecodeString(envelope.Authentication.Signature)
	if err != nil || len(signature) != sha256.Size {
		return &ProtocolError{Code: ErrorAuthentication, Message: "invalid signature encoding"}
	}
	canonical, err := signatureBytes(envelope)
	if err != nil {
		return &ProtocolError{Code: ErrorAuthentication, Message: err.Error()}
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(canonical)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return &ProtocolError{Code: ErrorAuthentication, Message: "signature mismatch"}
	}
	return nil
}

func signatureBytes(envelope Envelope) ([]byte, error) {
	envelope.Authentication.Signature = ""
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("protocol authentication: canonicalize: %w", err)
	}
	return data, nil
}

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (p RetryPolicy) Delay(attempt int) time.Duration {
	if attempt <= 0 || p.BaseDelay <= 0 {
		return 0
	}
	delay := p.BaseDelay
	for i := 1; i < attempt && delay < p.MaxDelay; i++ {
		delay *= 2
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

type Handler func(context.Context, Envelope) (MessageType, any, error)

type ServerConfig struct {
	CoordinatorID     string
	WorkerID          string
	CoordinatorEpoch  int64
	WorkerEpoch       string
	PeerPrincipal     string
	PeerKeyID         string
	PeerSecret        []byte
	SignerPrincipal   string
	SignerKeyID       string
	SignerSecret      []byte
	Allowed           map[MessageType]bool
	SupportedVersions []int
	MaxClockSkew      time.Duration
	MaxInFlight       int
	MaxCachedRequests int
	Now               func() time.Time
}

type cachedExchange struct {
	digest   string
	response Envelope
	ready    bool
}

type Server struct {
	config       ServerConfig
	mu           sync.Mutex
	last         map[string]int64
	requests     map[string]cachedExchange
	requestOrder []string
	inFlight     int
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.CoordinatorID == "" || config.WorkerID == "" || config.WorkerEpoch == "" || config.CoordinatorEpoch < 1 ||
		config.PeerPrincipal == "" || config.PeerKeyID == "" || len(config.PeerSecret) < 16 ||
		config.SignerPrincipal == "" || config.SignerKeyID == "" || len(config.SignerSecret) < 16 {
		return nil, errors.New("protocol server: complete peer, signer, and epoch authentication are required")
	}
	if len(config.SupportedVersions) == 0 {
		config.SupportedVersions = []int{Version}
	}
	if config.MaxClockSkew <= 0 {
		config.MaxClockSkew = time.Minute
	}
	if config.MaxInFlight <= 0 {
		config.MaxInFlight = 1
	}
	if config.MaxCachedRequests <= 0 {
		config.MaxCachedRequests = 4096
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	config.PeerSecret = append([]byte(nil), config.PeerSecret...)
	config.SignerSecret = append([]byte(nil), config.SignerSecret...)
	config.SupportedVersions = append([]int(nil), config.SupportedVersions...)
	config.Allowed = cloneAllowed(config.Allowed)
	return &Server{config: config, last: make(map[string]int64), requests: make(map[string]cachedExchange)}, nil
}

func (s *Server) Handle(ctx context.Context, envelope Envelope, handler Handler) (Envelope, error) {
	if handler == nil {
		return Envelope{}, errors.New("protocol server: handler is required")
	}
	digest, protocolErr := s.validate(envelope)
	if protocolErr != nil {
		return Envelope{}, protocolErr
	}
	cacheKey := s.config.PeerPrincipal + "/" + envelope.RequestID

	s.mu.Lock()
	if cached, ok := s.requests[cacheKey]; ok {
		s.mu.Unlock()
		if cached.digest != digest {
			return Envelope{}, &ProtocolError{Code: ErrorReplay, Message: "request id was reused with different content", RequestID: envelope.RequestID}
		}
		if !cached.ready {
			return Envelope{}, &ProtocolError{Code: ErrorBackpressure, Message: "identical request is already in flight", Retryable: true, RetryAfter: "1s", RequestID: envelope.RequestID}
		}
		return cached.response, nil
	}
	if envelope.Sequence != s.last[envelope.SessionID]+1 {
		s.mu.Unlock()
		return Envelope{}, &ProtocolError{Code: ErrorReordered, Message: "sequence is not the next session value", RequestID: envelope.RequestID}
	}
	if s.inFlight >= s.config.MaxInFlight {
		s.mu.Unlock()
		return Envelope{}, &ProtocolError{Code: ErrorBackpressure, Message: "server concurrency limit reached", Retryable: true, RetryAfter: "1s", RequestID: envelope.RequestID}
	}
	s.inFlight++
	s.last[envelope.SessionID] = envelope.Sequence
	s.requests[cacheKey] = cachedExchange{digest: digest}
	s.requestOrder = append(s.requestOrder, cacheKey)
	for len(s.requestOrder) > s.config.MaxCachedRequests {
		evicted := s.requestOrder[0]
		s.requestOrder = s.requestOrder[1:]
		if s.requests[evicted].ready {
			delete(s.requests, evicted)
		} else {
			s.requestOrder = append(s.requestOrder, evicted)
			break
		}
	}
	s.mu.Unlock()

	kind, payload, err := handler(ctx, envelope)
	if err != nil {
		code := ErrorInternal
		if errors.Is(err, context.DeadlineExceeded) {
			code = ErrorTimeout
		} else if errors.Is(err, context.Canceled) {
			code = ErrorCancelled
		}
		if existing := new(ProtocolError); errors.As(err, &existing) {
			code = existing.Code
		}
		payload = ProtocolError{Code: code, Message: err.Error(), RequestID: envelope.RequestID}
		kind = MessageError
	}
	response, err := NewEnvelope(kind, envelope.SessionID, "response-"+envelope.RequestID,
		s.config.WorkerID, s.config.CoordinatorID, s.config.CoordinatorEpoch,
		s.config.WorkerEpoch, envelope.Sequence, s.config.Now(), envelope.Deadline, payload)
	if err == nil {
		response.InReplyTo = envelope.RequestID
		err = SignEnvelope(&response, s.config.SignerPrincipal, s.config.SignerKeyID, s.config.SignerSecret)
	}

	s.mu.Lock()
	s.inFlight--
	if err == nil {
		s.requests[cacheKey] = cachedExchange{digest: digest, response: response, ready: true}
	} else {
		delete(s.requests, cacheKey)
	}
	s.mu.Unlock()
	if err != nil {
		return Envelope{}, err
	}
	return response, nil
}

func (s *Server) validate(envelope Envelope) (string, *ProtocolError) {
	if !slices.Contains(s.config.SupportedVersions, envelope.Version) {
		return "", &ProtocolError{Code: ErrorUnsupportedVersion, Message: "unsupported protocol version", RequestID: envelope.RequestID}
	}
	if envelope.SessionID == "" || envelope.RequestID == "" || envelope.Sequence < 1 {
		return "", &ProtocolError{Code: ErrorMalformed, Message: "session, request, and positive sequence are required", RequestID: envelope.RequestID}
	}
	if envelope.Sender != s.config.CoordinatorID || envelope.Recipient != s.config.WorkerID {
		return "", &ProtocolError{Code: ErrorAuthorization, Message: "sender or recipient is not authorized", RequestID: envelope.RequestID}
	}
	if envelope.CoordinatorEpoch != s.config.CoordinatorEpoch || envelope.WorkerEpoch != s.config.WorkerEpoch {
		return "", &ProtocolError{Code: ErrorStaleEpoch, Message: "coordinator or worker epoch does not match", RequestID: envelope.RequestID}
	}
	if envelope.Authentication.Principal != s.config.PeerPrincipal || envelope.Authentication.KeyID != s.config.PeerKeyID {
		return "", &ProtocolError{Code: ErrorAuthentication, Message: "principal or key id does not match authenticated SSH identity", RequestID: envelope.RequestID}
	}
	if !s.config.Allowed[envelope.Type] {
		return "", &ProtocolError{Code: ErrorAuthorization, Message: "principal is not authorized for message type", RequestID: envelope.RequestID}
	}
	now := s.config.Now()
	if envelope.Deadline.IsZero() || !now.Before(envelope.Deadline) {
		return "", &ProtocolError{Code: ErrorTimeout, Message: "request deadline expired", RequestID: envelope.RequestID}
	}
	if envelope.SentAt.IsZero() || envelope.SentAt.Before(now.Add(-s.config.MaxClockSkew)) || envelope.SentAt.After(now.Add(s.config.MaxClockSkew)) {
		return "", &ProtocolError{Code: ErrorAuthentication, Message: "request timestamp is outside allowed skew", RequestID: envelope.RequestID}
	}
	sum := sha256.Sum256(envelope.Payload)
	digest := hex.EncodeToString(sum[:])
	if !hmac.Equal([]byte(strings.ToLower(envelope.PayloadSHA256)), []byte(digest)) {
		return "", &ProtocolError{Code: ErrorMalformed, Message: "payload checksum mismatch", RequestID: envelope.RequestID}
	}
	if err := VerifyEnvelopeSignature(envelope, s.config.PeerSecret); err != nil {
		return "", err.(*ProtocolError)
	}
	canonical, err := signatureBytes(envelope)
	if err != nil {
		return "", &ProtocolError{Code: ErrorMalformed, Message: err.Error(), RequestID: envelope.RequestID}
	}
	requestSum := sha256.Sum256(canonical)
	return hex.EncodeToString(requestSum[:]), nil
}

func cloneAllowed(source map[MessageType]bool) map[MessageType]bool {
	result := make(map[MessageType]bool, len(source))
	maps.Copy(result, source)
	return result
}
