package backlogadmin

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// CoordinatorAdminTransport carries every coordinator-admin operation. It is
// the union of the six narrow service interfaces the CLI already depends on
// and the three operations that used to be reached through their own inline
// clients: node wait, graph amendment and worker enrollment.
//
// The method signatures are the ones LocalClient already had, so LocalClient
// satisfies this interface unchanged and no caller had to be rewritten around
// a new shape. The H1 freeze sketched the same nine operations with shorter
// parameter names (Action, ArtifactRequest, ArtifactStream and friends); those
// types do not exist in this package, and the freeze also states that existing
// method signatures are preserved verbatim, so the verbatim signatures win.
type CoordinatorAdminTransport interface {
	Query(ctx context.Context, request Query) (Response, error)
	Mutate(ctx context.Context, mutation Mutation) (MutationResponse, error)
	RecoverUnknown(ctx context.Context, principal Principal, request UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error)
	PutSchedule(ctx context.Context, principal Principal, request LocalScheduleDefinitionRequest) (LocalScheduleDefinitionResponse, error)
	SubmitArchive(ctx context.Context, request LocalSubmissionRequest, body io.Reader, size int64) (LocalSubmissionResponse, error)
	OpenArtifact(ctx context.Context, principal Principal, artifactID string) (ArtifactContent, error)
	NodeWait(ctx context.Context, request NodeWaitOperation) (NodeWaitResponse, error)
	AmendGraph(ctx context.Context, request domain.GraphAmendment) (domain.GraphAmendmentResult, error)
	EnrollWorker(ctx context.Context, request domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error)
	Describe() TransportDescription
}

// Carrier names. They appear in command output so that an agent never has to
// guess which coordinator answered.
const (
	CarrierLocal  = "local"
	CarrierRemote = "ssh"
)

// TransportDescription says which coordinator a client will reach and how. It
// never carries a credential value or a credential reference.
type TransportDescription struct {
	Carrier       string `json:"carrier"`
	CoordinatorID string `json:"coordinatorId,omitempty"`
	Endpoint      string `json:"endpoint"`
}

// TransportClass is the shared failure taxonomy of both carriers. An agent
// branches on it instead of parsing prose.
type TransportClass string

const (
	ClassOK                  TransportClass = "ok"
	ClassClientConfiguration TransportClass = "client-configuration"
	ClassAuthentication      TransportClass = "authentication"
	ClassUnavailable         TransportClass = "unavailable"
	ClassTimeout             TransportClass = "timeout"
	ClassProtocol            TransportClass = "protocol"
	ClassRejected            TransportClass = "rejected"
)

// TransportError classifies one failed coordinator-admin exchange.
type TransportError struct {
	Class       TransportClass
	Operation   string
	Coordinator string
	Err         error
}

func (e *TransportError) Error() string {
	if e == nil {
		return ""
	}
	var builder strings.Builder
	builder.WriteString(string(e.Class))
	if e.Operation != "" {
		builder.WriteString(" (")
		builder.WriteString(e.Operation)
		if e.Coordinator != "" {
			builder.WriteString(" on ")
			builder.WriteString(e.Coordinator)
		}
		builder.WriteString(")")
	}
	if e.Err != nil {
		builder.WriteString(": ")
		builder.WriteString(e.Err.Error())
	}
	return builder.String()
}

func (e *TransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ClassOf reports the transport class of an error, or ClassOK when the error is
// nil and an empty class when the failure was never transport-classified.
func ClassOf(err error) TransportClass {
	if err == nil {
		return ClassOK
	}
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		return transportErr.Class
	}
	return ""
}

// ExitCode maps a class to the process exit code frozen for H1. Anything that
// is not transport-classified keeps the generic exit 1, so automation that only
// checks for a non-zero status keeps working.
func ExitCode(class TransportClass) int {
	switch class {
	case ClassOK:
		return 0
	case ClassClientConfiguration:
		return 3
	case ClassAuthentication:
		return 4
	case ClassUnavailable:
		return 5
	case ClassTimeout:
		return 6
	case ClassProtocol:
		return 7
	case ClassRejected:
		return 8
	default:
		return 1
	}
}

// ExitCodeFor is ExitCode applied to whatever a command returned.
func ExitCodeFor(err error) int {
	if err == nil {
		return 0
	}
	return ExitCode(ClassOf(err))
}

// TransportErrorEnvelope is the --json error document both carriers print.
type TransportErrorEnvelope struct {
	Version   string         `json:"version"`
	Kind      string         `json:"kind"`
	Class     TransportClass `json:"class"`
	Operation string         `json:"operation"`
	Message   string         `json:"message"`
}

// NewTransportErrorEnvelope builds the versioned error document for err, or
// reports false when the failure was never transport-classified.
func NewTransportErrorEnvelope(err error) (TransportErrorEnvelope, bool) {
	var transportErr *TransportError
	if err == nil || !errors.As(err, &transportErr) {
		return TransportErrorEnvelope{}, false
	}
	message := ""
	if transportErr.Err != nil {
		message = transportErr.Err.Error()
	}
	return TransportErrorEnvelope{
		Version:   Version,
		Kind:      "error",
		Class:     transportErr.Class,
		Operation: transportErr.Operation,
		Message:   message,
	}, true
}

// classify wraps err for one operation unless it already carries a class.
func classify(class TransportClass, operation, coordinator string, err error) error {
	if err == nil {
		return nil
	}
	var existing *TransportError
	if errors.As(err, &existing) {
		return err
	}
	return &TransportError{Class: class, Operation: operation, Coordinator: coordinator, Err: err}
}

var _ CoordinatorAdminTransport = LocalClient{}
