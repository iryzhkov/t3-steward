package workerproto

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	refTestObjectID = "a90c0978567908625629368af0749936c9ff6b92"
	refTestRef      = "refs/heads/steward/run-x/task-y/cp-1"
)

func refTestRequest() RepositoryRefRequest {
	return RepositoryRefRequest{
		Repository:     "https://github.com/owner/project",
		Ref:            refTestRef,
		CredentialRefs: []string{"secretref:f03-admin/homelab"},
		TimeoutSeconds: 30,
	}
}

// TestRepositoryRefMessagesRoundTripStrictly states the wire shape of the pair
// and that the payload decoder stays as strict as every other message: an
// unknown field is refused, not ignored.
func TestRepositoryRefMessagesRoundTripStrictly(t *testing.T) {
	now := time.Date(2026, 10, 6, 7, 0, 0, 0, time.UTC)
	request := refTestRequest()
	envelope, err := NewEnvelope(MessageRepositoryRefResolve, "session", "request", "coordinator", "worker",
		7, "worker-1", 1, now, now.Add(time.Minute), request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded RepositoryRefRequest
	if err := DecodePayload(envelope, MessageRepositoryRefResolve, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Ref != request.Ref || decoded.Repository != request.Repository ||
		len(decoded.CredentialRefs) != 1 || decoded.TimeoutSeconds != 30 {
		t.Fatalf("decoded request = %+v", decoded)
	}

	resolution := RepositoryRefResolution{
		Ref: refTestRef, Status: RefResolutionResolved, ObjectID: refTestObjectID,
		CredentialsResolved: true, ObservedAt: now,
	}
	reply, err := NewEnvelope(MessageRepositoryRefResolution, "session", "reply", "worker", "coordinator",
		7, "worker-1", 1, now, now.Add(time.Minute), resolution)
	if err != nil {
		t.Fatal(err)
	}
	var decodedReply RepositoryRefResolution
	if err := DecodePayload(reply, MessageRepositoryRefResolution, &decodedReply); err != nil {
		t.Fatal(err)
	}
	if decodedReply.ObjectID != refTestObjectID || decodedReply.Status != RefResolutionResolved || !decodedReply.ObservedAt.Equal(now) {
		t.Fatalf("decoded reply = %+v", decodedReply)
	}
	var fields map[string]any
	if err := json.Unmarshal(reply.Payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ref", "status", "objectId", "observedAt"} {
		if _, present := fields[name]; !present {
			t.Fatalf("reply payload lacks %q: %s", name, reply.Payload)
		}
	}

	// The probe pair keeps its own type: a reachability observation is not a
	// ref resolution and decoding one as the other is refused.
	if err := DecodePayload(reply, MessageRepositoryObservation, &RepositoryObservation{}); err == nil {
		t.Fatal("a ref resolution decoded as a repository observation")
	}
	extended := reply
	extended.Payload = json.RawMessage(strings.Replace(string(reply.Payload), `{`, `{"refs":["x"],`, 1))
	if err := DecodePayload(extended, MessageRepositoryRefResolution, &RepositoryRefResolution{}); err == nil {
		t.Fatal("an unknown reply field was accepted")
	}
}

// TestValidateRepositoryRefRequestBounds pins the transport bounds. Ref syntax
// beyond these bounds is the worker's to judge, and it answers invalid-ref.
func TestValidateRepositoryRefRequestBounds(t *testing.T) {
	if err := ValidateRepositoryRefRequest(refTestRequest()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*RepositoryRefRequest){
		"empty repository":   func(r *RepositoryRefRequest) { r.Repository = "" },
		"empty ref":          func(r *RepositoryRefRequest) { r.Ref = "" },
		"option shaped ref":  func(r *RepositoryRefRequest) { r.Ref = "--upload-pack=touch /tmp/pwned" },
		"option shaped repo": func(r *RepositoryRefRequest) { r.Repository = "-oProxyCommand=x" },
		"newline in ref":     func(r *RepositoryRefRequest) { r.Ref = "refs/heads/a\nrefs/heads/b" },
		"oversized ref": func(r *RepositoryRefRequest) {
			r.Ref = "refs/heads/" + strings.Repeat("a", MaxRepositoryProbeValueBytes)
		},
		"negative timeout":       func(r *RepositoryRefRequest) { r.TimeoutSeconds = -1 },
		"timeout above maximum":  func(r *RepositoryRefRequest) { r.TimeoutSeconds = int(MaxRepositoryProbeTimeout/time.Second) + 1 },
		"too many credentials":   func(r *RepositoryRefRequest) { r.CredentialRefs = make([]string, MaxRepositoryProbeCredentialRefs+1) },
		"empty credential ref":   func(r *RepositoryRefRequest) { r.CredentialRefs = []string{""} },
		"invalid UTF-8 in ref":   func(r *RepositoryRefRequest) { r.Ref = "refs/heads/\xff" },
		"NUL in repository":      func(r *RepositoryRefRequest) { r.Repository = "https://x.invalid/\x00" },
		"carriage return in ref": func(r *RepositoryRefRequest) { r.Ref = "refs/heads/a\r" },
	} {
		t.Run(name, func(t *testing.T) {
			request := refTestRequest()
			mutate(&request)
			if err := ValidateRepositoryRefRequest(request); err == nil {
				t.Fatal("request accepted")
			}
		})
	}
}

// TestValidateRepositoryRefResolution states what a coordinator accepts back: a
// closed status set, an object ID only on a resolved answer and always a full
// one, the ref it asked about and nothing else, and a bounded detail.
func TestValidateRepositoryRefResolution(t *testing.T) {
	now := time.Date(2026, 10, 6, 7, 0, 0, 0, time.UTC)
	request := refTestRequest()
	valid := []RepositoryRefResolution{
		{Ref: refTestRef, Status: RefResolutionResolved, ObjectID: refTestObjectID, ObservedAt: now},
		{Ref: refTestRef, Status: RefResolutionResolved, ObjectID: strings.Repeat("b", 64), ObservedAt: now},
		{Ref: refTestRef, Status: RefResolutionNotFound, Class: "ref-not-found", ExitCode: 2, ObservedAt: now},
		{Ref: refTestRef, Status: RefResolutionUnreachable, Class: "timeout", ObservedAt: now},
		{Ref: refTestRef, Status: RefResolutionInvalidRef, Detail: "not a safe Git ref", ObservedAt: now},
	}
	for _, resolution := range valid {
		if err := ValidateRepositoryRefResolution(request, resolution); err != nil {
			t.Fatalf("%+v: %v", resolution, err)
		}
	}
	invalid := map[string]RepositoryRefResolution{
		"unknown status":         {Ref: refTestRef, Status: "maybe", ObservedAt: now},
		"empty status":           {Ref: refTestRef, ObservedAt: now},
		"resolved without id":    {Ref: refTestRef, Status: RefResolutionResolved, ObservedAt: now},
		"abbreviated id":         {Ref: refTestRef, Status: RefResolutionResolved, ObjectID: refTestObjectID[:12], ObservedAt: now},
		"uppercase id":           {Ref: refTestRef, Status: RefResolutionResolved, ObjectID: strings.ToUpper(refTestObjectID), ObservedAt: now},
		"non-hex id":             {Ref: refTestRef, Status: RefResolutionResolved, ObjectID: strings.Repeat("z", 40), ObservedAt: now},
		"not found carries id":   {Ref: refTestRef, Status: RefResolutionNotFound, ObjectID: refTestObjectID, ObservedAt: now},
		"unreachable carries id": {Ref: refTestRef, Status: RefResolutionUnreachable, ObjectID: refTestObjectID, ObservedAt: now},
		"different ref":          {Ref: "refs/heads/main", Status: RefResolutionResolved, ObjectID: refTestObjectID, ObservedAt: now},
		"no observation time":    {Ref: refTestRef, Status: RefResolutionResolved, ObjectID: refTestObjectID},
		"oversized detail": {Ref: refTestRef, Status: RefResolutionUnreachable,
			Detail: strings.Repeat("x", MaxRepositoryProbeDetailBytes+1), ObservedAt: now},
		"oversized class": {Ref: refTestRef, Status: RefResolutionUnreachable,
			Class: strings.Repeat("x", MaxRepositoryProbeValueBytes+1), ObservedAt: now},
	}
	for name, resolution := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := ValidateRepositoryRefResolution(request, resolution); err == nil {
				t.Fatal("resolution accepted")
			}
		})
	}
}

// refLoopback answers every exchange with one scripted payload and records the
// envelopes it saw, so the client's own validation is what is under test.
type refLoopback struct {
	kind    MessageType
	reply   any
	seen    []Envelope
	failure error
}

func (l *refLoopback) RoundTripWithRetry(_ context.Context, request Envelope, _ RetryPolicy) (Envelope, error) {
	l.seen = append(l.seen, request)
	if l.failure != nil {
		return Envelope{}, l.failure
	}
	response, err := NewEnvelope(l.kind, request.SessionID, request.RequestID+"-reply", request.Recipient, request.Sender,
		request.CoordinatorEpoch, request.WorkerEpoch, request.Sequence, request.SentAt, request.Deadline, l.reply)
	if err != nil {
		return Envelope{}, err
	}
	response.InReplyTo = request.RequestID
	if err := SignEnvelope(&response, "ssh:worker", "worker-key", []byte("worker-secret-value-0")); err != nil {
		return Envelope{}, err
	}
	return response, nil
}

func refTestClient(t *testing.T, transport RoundTripper) *Client {
	t.Helper()
	client, err := NewClient(ClientConfig{
		CoordinatorID: "coordinator", WorkerID: "worker", CoordinatorEpoch: 7, WorkerEpoch: "worker-1",
		SessionID: "session", RequestTimeout: time.Minute,
		SignerPrincipal: "ssh:coordinator", SignerKeyID: "coordinator-key",
		SignerSecret: []byte("coordinator-secret-value"),
		RetryPolicy:  RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		Transport:    transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestClientResolveRepositoryRef states that the client sends the dedicated
// message, refuses an out-of-bounds request before any transport, and refuses a
// reply that does not answer the question it asked.
func TestClientResolveRepositoryRef(t *testing.T) {
	now := time.Now().UTC()
	loopback := &refLoopback{kind: MessageRepositoryRefResolution, reply: RepositoryRefResolution{
		Ref: refTestRef, Status: RefResolutionResolved, ObjectID: refTestObjectID, ObservedAt: now,
	}}
	client := refTestClient(t, loopback)
	resolution, err := client.ResolveRepositoryRef(context.Background(), refTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resolution.ObjectID != refTestObjectID || len(loopback.seen) != 1 || loopback.seen[0].Type != MessageRepositoryRefResolve {
		t.Fatalf("resolution = %+v, sent %d", resolution, len(loopback.seen))
	}

	bad := refTestRequest()
	bad.Ref = "-x"
	if _, err := refTestClient(t, loopback).ResolveRepositoryRef(context.Background(), bad); err == nil {
		t.Fatal("an option-shaped ref was sent")
	}
	if len(loopback.seen) != 1 {
		t.Fatalf("a refused request reached the transport (%d sends)", len(loopback.seen))
	}

	mismatched := &refLoopback{kind: MessageRepositoryRefResolution, reply: RepositoryRefResolution{
		Ref: "refs/heads/main", Status: RefResolutionResolved, ObjectID: refTestObjectID, ObservedAt: now,
	}}
	if _, err := refTestClient(t, mismatched).ResolveRepositoryRef(context.Background(), refTestRequest()); err == nil {
		t.Fatal("a resolution of a different ref was accepted")
	}

	wrongKind := &refLoopback{kind: MessageRepositoryObservation, reply: RepositoryObservation{Class: "authenticated-ok", ObservedAt: now}}
	if _, err := refTestClient(t, wrongKind).ResolveRepositoryRef(context.Background(), refTestRequest()); err == nil {
		t.Fatal("a reachability observation was accepted as a ref resolution")
	}

	failing := &refLoopback{failure: errors.New("connection refused")}
	if _, err := refTestClient(t, failing).ResolveRepositoryRef(context.Background(), refTestRequest()); err == nil {
		t.Fatal("a transport failure produced a resolution")
	}
}
