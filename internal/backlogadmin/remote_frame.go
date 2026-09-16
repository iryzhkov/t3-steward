package backlogadmin

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// credentialEnvironmentName maps a reference onto the restricted environment
// variable that carries it. It matches the worker runtime's mapping on
// purpose: one naming rule for every secret reference on a host.
func credentialEnvironmentName(reference string) string {
	var result strings.Builder
	for _, char := range reference {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			result.WriteRune(unicode.ToUpper(char))
		} else {
			result.WriteByte('_')
		}
	}
	return result.String()
}

// RemoteTransportVersion names the signed frame the remote carrier wraps around
// the ordinary operation envelope.
const RemoteTransportVersion = "backlog.admin.remote/v1"

// remoteSignatureDomain separates admin signatures from worker signatures. The
// worker protocol signs the bare canonical envelope; an admin frame signs this
// tag first, so no byte string is ever a valid signature input for both. That
// is a second fence behind the disjoint credential namespaces, not a
// replacement for them.
const remoteSignatureDomain = "t3-steward/coordinator-admin-remote/v1\n"

// Credential namespaces. Admin clients and workers never share a reference.
const (
	AdminCredentialPrefix  = "secretref:f03-admin/"
	WorkerCredentialPrefix = "secretref:f02-protocol/"
)

// RemoteAdminRole is the role the coordinator grants a verified remote client.
// It is deliberately not the local peer-UID role.
const RemoteAdminRole = "remote-admin"

// LocalAdminRole is the role the owner-only socket grants its peer.
const LocalAdminRole = "local-admin"

// SupervisorRole is the role a campaign overseer acts under. It is the third
// and narrowest role: where local-admin is everything and remote-admin is
// reads plus an allowlist, a supervisor is bound to one run and one activation
// epoch, and the binding is enforced by the coordinator's own authorizer
// rather than by anything the credential carries. See SupervisorAuthorizer.
const SupervisorRole = "supervisor"

// ValidateAdminCredentialReference refuses anything outside the admin
// namespace, and says so by name when a worker reference was supplied.
func ValidateAdminCredentialReference(reference string) error {
	switch {
	case strings.TrimSpace(reference) != reference || reference == "":
		return errors.New("coordinator admin credential reference must be trimmed and non-empty")
	case strings.HasPrefix(reference, WorkerCredentialPrefix):
		return fmt.Errorf("%q is a worker protocol credential; coordinator admin clients need a %s reference", reference, AdminCredentialPrefix)
	case !strings.HasPrefix(reference, AdminCredentialPrefix):
		return fmt.Errorf("coordinator admin credential reference must start with %s (got %q)", AdminCredentialPrefix, reference)
	case len(reference) == len(AdminCredentialPrefix):
		return errors.New("coordinator admin credential reference must name a client")
	}
	return nil
}

// The other half of this rule, refusing an admin reference where a worker
// credential is expected, is enforced where worker credentials are actually
// declared, in internal/config's backlog_v2 validation. It is not duplicated
// here: an exported validator with no caller protects nothing.

// AdminCredentials is the mutually authenticated identity material behind one
// admin credential reference. Values are resolved on both ends and never
// appear in configuration, manifests or diagnostics.
type AdminCredentials struct {
	ClientPrincipal      string `json:"clientPrincipal"`
	ClientKeyID          string `json:"clientKeyId"`
	ClientSecret         []byte `json:"clientSecret"`
	CoordinatorPrincipal string `json:"coordinatorPrincipal"`
	CoordinatorKeyID     string `json:"coordinatorKeyId"`
	CoordinatorSecret    []byte `json:"coordinatorSecret"`
}

// Complete reports whether both directions carry a usable identity.
func (c AdminCredentials) Complete() bool {
	return c.ClientPrincipal != "" && c.ClientKeyID != "" && len(c.ClientSecret) >= 16 &&
		c.CoordinatorPrincipal != "" && c.CoordinatorKeyID != "" && len(c.CoordinatorSecret) >= 16
}

// AdminCredentialResolver resolves an admin credential reference to its value.
type AdminCredentialResolver interface {
	ResolveAdmin(reference string) (AdminCredentials, error)
}

// EnvironmentAdminCredentialResolver reads the JSON bundle from the same
// restricted environment namespace the worker credentials use, under a
// reference that the admin namespace check has already accepted.
type EnvironmentAdminCredentialResolver struct {
	Prefix string
	Lookup func(string) (string, bool)
}

func (r EnvironmentAdminCredentialResolver) ResolveAdmin(reference string) (AdminCredentials, error) {
	if err := ValidateAdminCredentialReference(reference); err != nil {
		return AdminCredentials{}, err
	}
	prefix := r.Prefix
	if prefix == "" {
		prefix = "T3_STEWARD_CREDENTIAL_"
	}
	lookup := r.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	raw, ok := lookup(prefix + credentialEnvironmentName(reference))
	if !ok || raw == "" {
		return AdminCredentials{}, fmt.Errorf("resolve admin credentials: reference %q is unavailable", reference)
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var credentials AdminCredentials
	if err := decoder.Decode(&credentials); err != nil {
		return AdminCredentials{}, fmt.Errorf("resolve admin credentials: reference %q is malformed", reference)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return AdminCredentials{}, fmt.Errorf("resolve admin credentials: reference %q has trailing content", reference)
	}
	if !credentials.Complete() {
		return AdminCredentials{}, fmt.Errorf("resolve admin credentials: reference %q is incomplete", reference)
	}
	return credentials, nil
}

// remoteAuthentication is the signature block of one admin frame.
type remoteAuthentication struct {
	Principal string `json:"principal"`
	KeyID     string `json:"keyId"`
	Signature string `json:"signature"`
}

// remoteFrame is the security envelope the remote carrier adds around the
// ordinary operation envelope: version, session, request id, sequence,
// sent-at, deadline, payload digest and an HMAC over the canonical form. The
// payload is the same localRequest or localResponse the local carrier sends,
// so both carriers speak one operation vocabulary.
type remoteFrame struct {
	Version        string               `json:"version"`
	Operation      string               `json:"operation"`
	SessionID      string               `json:"sessionId"`
	RequestID      string               `json:"requestId"`
	InReplyTo      string               `json:"inReplyTo,omitempty"`
	Sender         string               `json:"sender"`
	Recipient      string               `json:"recipient"`
	Sequence       int64                `json:"sequence"`
	SentAt         time.Time            `json:"sentAt"`
	Deadline       time.Time            `json:"deadline"`
	PayloadSHA256  string               `json:"payloadSha256"`
	Authentication remoteAuthentication `json:"authentication"`
	Payload        json.RawMessage      `json:"payload,omitempty"`
}

func newRemoteFrame(operation, sessionID, requestID, sender, recipient string, sequence int64, sentAt, deadline time.Time, payload any) (remoteFrame, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return remoteFrame{}, fmt.Errorf("admin frame: payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	return remoteFrame{
		Version: RemoteTransportVersion, Operation: operation, SessionID: sessionID, RequestID: requestID,
		Sender: sender, Recipient: recipient, Sequence: sequence,
		SentAt: sentAt.UTC(), Deadline: deadline.UTC(),
		PayloadSHA256: hex.EncodeToString(sum[:]), Payload: raw,
	}, nil
}

func remoteSignatureBytes(frame remoteFrame) ([]byte, error) {
	frame.Authentication.Signature = ""
	data, err := json.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("admin frame: canonicalize: %w", err)
	}
	return append([]byte(remoteSignatureDomain), data...), nil
}

func signRemoteFrame(frame *remoteFrame, principal, keyID string, secret []byte) error {
	if frame == nil || principal == "" || keyID == "" || len(secret) < 16 {
		return errors.New("admin frame: principal, key id and a 16-byte secret are required")
	}
	frame.Authentication = remoteAuthentication{Principal: principal, KeyID: keyID}
	canonical, err := remoteSignatureBytes(*frame)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(canonical)
	frame.Authentication.Signature = hex.EncodeToString(mac.Sum(nil))
	return nil
}

func verifyRemoteFrame(frame remoteFrame, secret []byte) error {
	signature, err := hex.DecodeString(frame.Authentication.Signature)
	if err != nil || len(signature) != sha256.Size {
		return &workerproto.ProtocolError{Code: workerproto.ErrorAuthentication, Message: "invalid admin frame signature encoding", RequestID: frame.RequestID}
	}
	canonical, err := remoteSignatureBytes(frame)
	if err != nil {
		return &workerproto.ProtocolError{Code: workerproto.ErrorAuthentication, Message: err.Error(), RequestID: frame.RequestID}
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(canonical)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return &workerproto.ProtocolError{Code: workerproto.ErrorAuthentication, Message: "admin frame signature mismatch", RequestID: frame.RequestID}
	}
	sum := sha256.Sum256(frame.Payload)
	if !hmac.Equal([]byte(strings.ToLower(frame.PayloadSHA256)), []byte(hex.EncodeToString(sum[:]))) {
		return &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: "admin frame payload checksum mismatch", RequestID: frame.RequestID}
	}
	return nil
}

// frameDigest identifies the request behind one request id, so that a retry
// with the same id and different content can be refused while an honest retry
// of the same request is recognised.
//
// It deliberately covers the operation, the two identities and the payload
// digest, and not the timestamps or the signature: a retry is a new frame sent
// at a new time, and hashing that would make every retry look like a different
// request. The submission archive bytes are not in it either; those are covered
// by the submission service's own content digest, one layer up.
func frameDigest(frame remoteFrame) (string, error) {
	canonical, err := json.Marshal([]string{
		RemoteTransportVersion, frame.Operation, frame.Sender, frame.Recipient,
		strings.ToLower(frame.PayloadSHA256),
	})
	if err != nil {
		return "", fmt.Errorf("admin frame: digest: %w", err)
	}
	sum := sha256.Sum256(append([]byte(remoteSignatureDomain), canonical...))
	return hex.EncodeToString(sum[:]), nil
}

// writeRemoteFrame emits one newline-delimited JSON frame, bounded by maxBytes.
func writeRemoteFrame(writer io.Writer, frame remoteFrame, maxBytes int64) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("admin frame: encode: %w", err)
	}
	if int64(len(data)+1) > maxBytes {
		return &workerproto.ProtocolError{Code: workerproto.ErrorLimit, Message: "admin frame exceeds the configured message limit", RequestID: frame.RequestID}
	}
	if _, err := writer.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

// readRemoteFrame consumes one newline-delimited JSON frame and leaves the
// reader positioned on the raw bytes that follow it, which is what keeps the
// artifact and submission streams working through the remote carrier.
func readRemoteFrame(reader byteLineReader, maxBytes int64) (remoteFrame, error) {
	if maxBytes <= 0 {
		return remoteFrame{}, &workerproto.ProtocolError{Code: workerproto.ErrorLimit, Message: "admin frame limit must be positive"}
	}
	var line []byte
	for int64(len(line)) <= maxBytes {
		b, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) == 0 {
				return remoteFrame{}, io.EOF
			}
			return remoteFrame{}, fmt.Errorf("admin frame: read: %w", err)
		}
		if b == '\n' {
			decoder := json.NewDecoder(bytes.NewReader(line))
			decoder.DisallowUnknownFields()
			var frame remoteFrame
			if err := decoder.Decode(&frame); err != nil {
				return remoteFrame{}, &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: "invalid admin frame JSON: " + err.Error()}
			}
			var trailing json.RawMessage
			if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
				return remoteFrame{}, &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: "admin frame must contain one JSON value"}
			}
			return frame, nil
		}
		line = append(line, b)
	}
	return remoteFrame{}, &workerproto.ProtocolError{Code: workerproto.ErrorLimit, Message: "admin frame exceeds the configured message limit"}
}

type byteLineReader interface {
	io.Reader
	ReadByte() (byte, error)
}

// classOfProtocolError maps a wire error code onto the shared taxonomy.
func classOfProtocolError(code workerproto.ErrorCode) TransportClass {
	switch code {
	case workerproto.ErrorAuthentication, workerproto.ErrorAuthorization:
		return ClassAuthentication
	case workerproto.ErrorUnsupportedVersion, workerproto.ErrorMalformed,
		workerproto.ErrorLimit, workerproto.ErrorReordered:
		return ClassProtocol
	case workerproto.ErrorTimeout:
		return ClassTimeout
	case workerproto.ErrorCancelled:
		return ClassUnavailable
	default:
		return ClassRejected
	}
}
