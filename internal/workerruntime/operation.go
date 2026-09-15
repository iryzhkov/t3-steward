package workerruntime

import "github.com/iryzhkov/t3-steward/internal/workerproto"

// The three operations the restricted SSH worker endpoint can carry. They are
// stream disciplines, not authority levels: receiving artifacts consumes raw
// object bytes after the envelope, sending them writes raw bytes out after the
// response, and control is one bounded request and one bounded response.
//
// What a worker will accept is decided by the allowlist NewWorkerService builds
// and by the envelope signature, neither of which depends on how the process
// was invoked.
const (
	OperationControl         = "control"
	OperationArtifactReceive = "artifact-receive"
	OperationArtifactSend    = "artifact-send"
)

// OperationFor names the operation that knows how to carry one message.
//
// The message type is a signed field of the envelope, so a coordinator asking
// for artifact delivery cannot be mistaken for one asking for control unless it
// holds the worker's credential. That is what lets one forced command serve a
// worker for all three: a coordinator dials one address for every operation, so
// a forced command that pinned one word could never carry the other two.
func OperationFor(kind workerproto.MessageType) string {
	switch kind {
	case workerproto.MessageArtifactDownload:
		return OperationArtifactReceive
	case workerproto.MessageArtifactUpload:
		return OperationArtifactSend
	default:
		return OperationControl
	}
}
