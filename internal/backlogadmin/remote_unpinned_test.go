package backlogadmin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// serveOnce runs one exchange against a server whose forced command pinned the
// given operation, empty meaning it pinned none, and returns the answer frame's
// decoded payload alongside whatever Serve reported.
func serveOnce(t *testing.T, pinned string, request localRequest) (localResponse, error) {
	t.Helper()
	credentials := testAdminCredentials()
	server, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID:   testCoordinatorID,
		Clients:         map[string]AdminCredentials{credentials.ClientPrincipal: credentials},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := newRemoteFrame(request.Operation, "session-1", "req/1",
		credentials.ClientPrincipal, testCoordinatorID, 1, time.Now(), time.Now().Add(time.Minute), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
		t.Fatal(err)
	}
	var input, out bytes.Buffer
	if err := writeRemoteFrame(&input, frame, 1<<20); err != nil {
		t.Fatal(err)
	}
	serveErr := server.Serve(context.Background(), pinned, &input, &out)
	answer, err := readRemoteFrame(bufio.NewReader(bytes.NewReader(out.Bytes())), 1<<20)
	if err != nil {
		t.Fatalf("the coordinator wrote no readable answer: %v (serve reported: %v)", err, serveErr)
	}
	var response localResponse
	if err := json.Unmarshal(answer.Payload, &response); err != nil {
		t.Fatal(err)
	}
	return response, serveErr
}

func queryRequest() localRequest {
	return localRequest{
		Version: LocalTransportVersion, Operation: localOperationQuery,
		Query: &Query{Version: Version, Kind: QueryViability},
	}
}

// An unpinned forced command serves whichever operation the signed frame names.
// One key then serves a client for everything it needs, which is what lets
// campaign submit run its readiness query and its submission from one
// coordinator client: the client declares one ssh destination and one identity,
// so a pinned word would confine it to one operation.
func TestUnpinnedForcedCommandServesTheFrameOperation(t *testing.T) {
	for _, operation := range Operations() {
		t.Run(operation, func(t *testing.T) {
			request := localRequest{Version: LocalTransportVersion, Operation: operation}
			switch operation {
			case localOperationQuery:
				request.Query = &Query{Version: Version, Kind: QueryStatus}
			case localOperationArtifact:
				request.ArtifactID = "artifact-1"
			default:
				// The other operations need a relay to answer; reaching the
				// relay at all is what this asserts, so they are covered by
				// the refusal not being about the operation word.
			}
			response, _ := serveOnce(t, "", request)
			if strings.Contains(response.Error, "frame operation does not match") ||
				strings.Contains(response.Error, "unknown admin frame operation") {
				t.Fatalf("an unpinned endpoint refused the %q frame: %q", operation, response.Error)
			}
		})
	}
}

// A pinned forced command still narrows the key to its one operation, which is
// the tighter arrangement the runbook keeps documenting.
func TestPinnedForcedCommandStillRefusesAnotherOperation(t *testing.T) {
	response, err := serveOnce(t, localOperationSubmission, queryRequest())
	if err == nil {
		t.Fatal("a key pinned to submission served a query")
	}
	if !strings.Contains(response.Error, "frame operation does not match the invoked operation") {
		t.Fatalf("refusal = %q", response.Error)
	}
	if response.ErrorClass != ClassProtocol {
		t.Fatalf("class = %q", response.ErrorClass)
	}
	// The matching operation is served by the same pinned endpoint.
	matching := localRequest{
		Version: LocalTransportVersion, Operation: localOperationQuery,
		Query: &Query{Version: Version, Kind: QueryStatus},
	}
	response, _ = serveOnce(t, localOperationQuery, matching)
	if strings.Contains(response.Error, "frame operation does not match") {
		t.Fatalf("a pinned endpoint refused its own operation: %q", response.Error)
	}
}

// An unpinned endpoint is not an unvalidated one: the frame must still name a
// known operation, and the envelope inside it must agree with the frame.
func TestUnpinnedForcedCommandStillValidatesTheOperation(t *testing.T) {
	unknown := localRequest{Version: LocalTransportVersion, Operation: "rotate-epoch"}
	response, err := serveOnce(t, "", unknown)
	if err == nil {
		t.Fatal("an unpinned endpoint served an unknown operation")
	}
	if !strings.Contains(response.Error, "unknown admin frame operation") {
		t.Fatalf("refusal = %q", response.Error)
	}
	// A frame whose envelope names a different operation than the frame is
	// refused, so the word cannot be laundered through the payload either.
	mismatched := localRequest{
		Version: LocalTransportVersion, Operation: localOperationSubmission,
		Query: &Query{Version: Version, Kind: QueryStatus},
	}
	frameSaysQuery := mismatched
	frameSaysQuery.Operation = localOperationSubmission
	response, err = serveOnceWithFrameOperation(t, "", localOperationQuery, frameSaysQuery)
	if err == nil {
		t.Fatal("an unpinned endpoint served a frame whose envelope disagreed with it")
	}
	if !strings.Contains(response.Error, "operation envelope does not match the frame") {
		t.Fatalf("refusal = %q", response.Error)
	}
}

// serveOnceWithFrameOperation signs a frame whose own operation differs from the
// envelope it carries, which is the shape a client would use to try to have one
// operation authorised and another performed.
func serveOnceWithFrameOperation(t *testing.T, pinned, frameOperation string, request localRequest) (localResponse, error) {
	t.Helper()
	credentials := testAdminCredentials()
	server, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID:   testCoordinatorID,
		Clients:         map[string]AdminCredentials{credentials.ClientPrincipal: credentials},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := newRemoteFrame(frameOperation, "session-1", "req/1",
		credentials.ClientPrincipal, testCoordinatorID, 1, time.Now(), time.Now().Add(time.Minute), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
		t.Fatal(err)
	}
	var input, out bytes.Buffer
	if err := writeRemoteFrame(&input, frame, 1<<20); err != nil {
		t.Fatal(err)
	}
	serveErr := server.Serve(context.Background(), pinned, &input, &out)
	answer, err := readRemoteFrame(bufio.NewReader(bytes.NewReader(out.Bytes())), 1<<20)
	if err != nil {
		t.Fatalf("the coordinator wrote no readable answer: %v (serve reported: %v)", err, serveErr)
	}
	var response localResponse
	if err := json.Unmarshal(answer.Payload, &response); err != nil {
		t.Fatal(err)
	}
	return response, serveErr
}
