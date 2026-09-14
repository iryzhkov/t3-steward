package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTransportClassExitCodeTable(t *testing.T) {
	for class, want := range map[TransportClass]int{
		ClassOK:                  0,
		ClassClientConfiguration: 3,
		ClassAuthentication:      4,
		ClassUnavailable:         5,
		ClassTimeout:             6,
		ClassProtocol:            7,
		ClassRejected:            8,
		TransportClass(""):       1,
		TransportClass("other"):  1,
	} {
		if got := ExitCode(class); got != want {
			t.Fatalf("ExitCode(%q) = %d, want %d", class, got, want)
		}
	}
	if got := ExitCodeFor(nil); got != 0 {
		t.Fatalf("ExitCodeFor(nil) = %d", got)
	}
	if got := ExitCodeFor(errors.New("plain")); got != 1 {
		t.Fatalf("unclassified exit code = %d, want 1", got)
	}
	classified := &TransportError{Class: ClassTimeout, Operation: "query", Err: errors.New("deadline")}
	if got := ExitCodeFor(classified); got != 6 {
		t.Fatalf("classified exit code = %d, want 6", got)
	}
}

func TestTransportErrorWrapsAndClassifiesOnce(t *testing.T) {
	cause := errors.New("cause")
	first := classify(ClassUnavailable, "query", "normandy", cause)
	if !errors.Is(first, cause) {
		t.Fatal("classified error lost its cause")
	}
	if ClassOf(first) != ClassUnavailable {
		t.Fatalf("class = %q", ClassOf(first))
	}
	// A second classification must not overwrite the first, so the layer that
	// knows most about the failure keeps the last word.
	second := classify(ClassProtocol, "query", "normandy", first)
	if ClassOf(second) != ClassUnavailable {
		t.Fatalf("reclassified to %q", ClassOf(second))
	}
	if classify(ClassRejected, "query", "normandy", nil) != nil {
		t.Fatal("classified a nil error")
	}
	if ClassOf(nil) != ClassOK {
		t.Fatal("nil error is not ok")
	}
	if ClassOf(errors.New("plain")) != "" {
		t.Fatal("unclassified error reported a class")
	}
}

func TestTransportErrorEnvelopeIsVersioned(t *testing.T) {
	envelope, ok := NewTransportErrorEnvelope(&TransportError{
		Class: ClassAuthentication, Operation: "submission", Err: errors.New("signature mismatch"),
	})
	if !ok {
		t.Fatal("classified error produced no envelope")
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"version":"backlog.admin/v1","kind":"error","class":"authentication",` +
		`"operation":"submission","message":"signature mismatch"}`
	if string(raw) != want {
		t.Fatalf("envelope = %s, want %s", raw, want)
	}
	if _, ok := NewTransportErrorEnvelope(errors.New("plain")); ok {
		t.Fatal("unclassified error produced an envelope")
	}
}

func TestLocalClientClassifiesConfigurationAndAvailability(t *testing.T) {
	ctx := context.Background()
	relative := LocalClient{Path: "relative.sock", MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, RequestTimeout: time.Second}
	if _, err := relative.Query(ctx, Query{}); ClassOf(err) != ClassClientConfiguration {
		t.Fatalf("relative socket path class = %q (%v)", ClassOf(err), err)
	}
	unlimited := LocalClient{Path: "/tmp/absent.sock", MaxResponseBytes: 0, MaxArtifactBytes: 1 << 20, RequestTimeout: time.Second}
	if _, err := unlimited.Query(ctx, Query{}); ClassOf(err) != ClassClientConfiguration {
		t.Fatalf("non-positive limit class = %q (%v)", ClassOf(err), err)
	}
	absent := LocalClient{
		Path:             filepath.Join(shortTempRoot(t), "absent.sock"),
		MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, RequestTimeout: time.Second,
	}
	if _, err := absent.Query(ctx, Query{}); ClassOf(err) != ClassUnavailable {
		t.Fatalf("absent socket class = %q (%v)", ClassOf(err), err)
	}
}

func TestLocalClientClassifiesServerRefusals(t *testing.T) {
	service := &localTransportService{}
	// An unexpected UID makes the server refuse the principal rather than the
	// request, which must classify as authentication, not rejection.
	foreign, cancel, done := startLocalTransport(t, uint32(os.Getuid())+1, service)
	if _, err := foreign.Query(context.Background(), Query{Version: Version, Kind: QueryStatus}); ClassOf(err) != ClassAuthentication {
		t.Fatalf("foreign uid class = %q (%v)", ClassOf(err), err)
	}
	stopLocalTransport(t, cancel, done)

	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
	// An unknown operation is a request the coordinator answered and refused.
	var response localResponse
	err := client.call(context.Background(), localRequest{Version: LocalTransportVersion, Operation: "nonsense"}, &response)
	if ClassOf(err) != ClassRejected {
		t.Fatalf("unknown operation class = %q (%v)", ClassOf(err), err)
	}
	stopLocalTransport(t, cancel, done)
}

func TestLocalClientDescribesItsCarrier(t *testing.T) {
	client := LocalClient{Path: "/run/t3/state.admin.sock", CoordinatorID: "normandy"}
	description := client.Describe()
	if description.Carrier != CarrierLocal || description.CoordinatorID != "normandy" ||
		description.Endpoint != "/run/t3/state.admin.sock" {
		t.Fatalf("description = %+v", description)
	}
}
