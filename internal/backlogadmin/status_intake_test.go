package backlogadmin

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Frozen d232756 RuntimeStatus: kept independent of the live type.
type parentRuntimeStatus struct {
	Release              string         `json:"release,omitempty"`
	ConfigurationDigest  string         `json:"configurationDigest,omitempty"`
	LastReload           time.Time      `json:"lastReload,omitzero"`
	LastReloadReceipt    *ReloadReceipt `json:"lastReloadReceipt,omitempty"`
	Mode                 string         `json:"mode"`
	Owner                string         `json:"owner"`
	Epoch                int64          `json:"epoch"`
	Health               string         `json:"health"`
	Transport            string         `json:"transport"`
	FreshWorkers         int            `json:"freshWorkers"`
	StaleWorkers         int            `json:"staleWorkers"`
	FreshQuotaPools      int            `json:"freshQuotaPools"`
	StaleQuotaPools      int            `json:"staleQuotaPools"`
	ReconciliationIssues []string       `json:"reconciliationIssues,omitempty"`
	UnknownExecutionIDs  []string       `json:"unknownExecutionIds,omitempty"`
	CustodyIncidentIDs   []string       `json:"custodyIncidentIds,omitempty"`
}
type parentStatusResponse struct {
	Version  string `json:"version"`
	Response *struct {
		Version     string    `json:"version"`
		Kind        QueryKind `json:"kind"`
		GeneratedAt time.Time `json:"generatedAt"`
		Status      *struct {
			WorkflowRuns map[domain.ProgressState]int  `json:"workflowRuns"`
			Tasks        map[domain.ProgressState]int  `json:"tasks"`
			Workers      map[string]int                `json:"workers"`
			QuotaPools   map[domain.AdmissionState]int `json:"quotaPools"`
			Reservations int                           `json:"reservations"`
			Locks        int                           `json:"locks"`
			Runtime      parentRuntimeStatus           `json:"runtime"`
		} `json:"status"`
	} `json:"response,omitempty"`
}
type statusTransportService struct {
	localTransportService
	query func(context.Context, Query) (Response, error)
}

func (s *statusTransportService) Query(ctx context.Context, q Query) (Response, error) {
	return s.query(ctx, q)
}

func intakeService(t *testing.T, state string) *Service {
	t.Helper()
	s, err := New(openAdminTestStore(t), &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	s.SetRuntimeInfo(RuntimeInfo{LegacyFileIntake: state, Mode: "coordinator", Owner: "coordinator-1", Epoch: 7})
	return s
}

func TestIntakeStatusStrictParentAndNewLocalClient(t *testing.T) {
	for _, state := range []string{"enabled", "disabled"} {
		t.Run(state, func(t *testing.T) {
			service := intakeService(t, state)
			client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: service.Query})
			defer stopLocalTransport(t, cancel, done)
			// Parent client sends the historical query and uses the actual framed
			// strict codec; it does not pass through the new negotiation helper.
			conn, err := net.Dial("unix", client.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			err = writeLocalJSON(conn, localRequest{Version: LocalTransportVersion, Operation: localOperationQuery, Query: &Query{Version: Version, Kind: QueryStatus}})
			if err != nil {
				t.Fatal(err)
			}
			var old parentStatusResponse
			if err := readLocalJSON(conn, client.MaxResponseBytes, &old); err != nil {
				t.Fatal(err)
			}
			if old.Response == nil || old.Response.Status == nil || old.Response.Status.Runtime.Owner != "coordinator-1" || old.Response.Status.Runtime.Epoch != 7 {
				t.Fatalf("parent status: %+v", old)
			}
			got, err := client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus})
			if err != nil || got.Status == nil || got.Status.Runtime.LegacyFileIntake != state {
				t.Fatalf("new status: %+v %v", got, err)
			}
			// Sensitivity: the extended projection really is incompatible with the
			// frozen strict shape, rather than a permissive test hiding the defect.
			extended, err := service.Query(context.Background(), Query{Version: StatusIntakeVersion, Kind: QueryStatus})
			if err != nil {
				t.Fatal(err)
			}
			var frame bytes.Buffer
			if err := writeLocalJSON(&frame, localResponse{Version: LocalTransportVersion, Response: &extended}); err != nil {
				t.Fatal(err)
			}
			if err := readLocalJSON(&frame, client.MaxResponseBytes, &old); err == nil || !strings.Contains(err.Error(), "legacyFileIntake") {
				t.Fatalf("extended parent decode: %v", err)
			}
		})
	}
}

func TestIntakeStatusOlderLocalCoordinatorAndFailures(t *testing.T) {
	for _, mode := range []string{"older", "auth", "config", "protocol", "other-rejected"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			service := intakeService(t, "disabled")
			q := func(ctx context.Context, query Query) (Response, error) {
				calls.Add(1)
				if query.Version == StatusIntakeVersion {
					switch mode {
					case "older":
						return Response{}, fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, query.Version, Version)
					case "auth":
						return Response{}, &TransportError{Class: ClassAuthentication, Err: errors.New("credential refused")}
					case "config":
						return Response{}, configurationError("configuration invalid")
					case "protocol":
						return Response{}, &TransportError{Class: ClassProtocol, Err: errors.New("malformed reply")}
					default:
						return Response{}, errors.New("authorize status: unsupported backlog admin version")
					}
				}
				return service.Query(ctx, query)
			}
			client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: q})
			defer stopLocalTransport(t, cancel, done)
			got, err := client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus})
			if mode == "older" {
				if err != nil || got.Status == nil || got.Status.Runtime.LegacyFileIntake != "" || calls.Load() != 2 {
					t.Fatalf("older: %+v %v calls=%d", got, err, calls.Load())
				}
			} else {
				// Service query errors keep the historical local-server rejected class.
				expected := ClassRejected
				if err == nil || calls.Load() != 1 || ClassOf(err) != expected {
					t.Fatalf("failure masked/reclassified: %v calls=%d want=%s", err, calls.Load(), expected)
				}
			}
		})
	}
}

func TestIntakeStatusSSHProjectionAndOlderCoordinator(t *testing.T) {
	for _, state := range []string{"enabled", "disabled", "older"} {
		t.Run(state, func(t *testing.T) {
			h := newRemoteHarness(t, false)
			service := intakeService(t, state)
			var calls atomic.Int32
			h.service.mu.Lock()
			h.service.query = func(ctx context.Context, q Query) (Response, error) {
				calls.Add(1)
				if state == "older" && q.Version != Version {
					return Response{}, fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, q.Version, Version)
				}
				return service.Query(ctx, q)
			}
			h.service.mu.Unlock()
			got, err := h.client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus})
			want := state
			count := int32(1)
			if state == "older" {
				want = ""
				count = 2
			}
			if err != nil || got.Status == nil || got.Status.Runtime.LegacyFileIntake != want || calls.Load() != count {
				t.Fatalf("SSH: %+v %v calls=%d", got, err, calls.Load())
			}
		})
	}
}

func TestIntakeStatusVersionIsReadOnlyAndCodecStillStrict(t *testing.T) {
	s := intakeService(t, "disabled")
	denied := errors.New("operator not authorized")
	auth := &allowAuthorizer{err: denied}
	guarded, err := New(openAdminTestStore(t), auth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guarded.Query(context.Background(), Query{Version: StatusIntakeVersion, Kind: QueryStatus}); !errors.Is(err, denied) {
		t.Fatalf("negotiated read bypassed authorization: %v", err)
	}
	if len(auth.actions) != 1 || auth.actions[0].Kind != QueryStatus {
		t.Fatalf("authorization action: %+v", auth.actions)
	}
	if _, err := s.Query(context.Background(), Query{Version: StatusIntakeVersion, Kind: QueryWorkers}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("non-status: %v", err)
	}
	if _, err := s.Mutate(context.Background(), Mutation{Version: StatusIntakeVersion}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("mutation: %v", err)
	}
	for _, raw := range []string{`{"version":"x","unknown":true}`, `{"version":"x"} {}`} {
		var frame bytes.Buffer
		_ = binary.Write(&frame, binary.BigEndian, uint32(len(raw)))
		frame.WriteString(raw)
		var request localRequest
		if err := readLocalJSON(&frame, 1<<20, &request); err == nil {
			t.Fatalf("accepted malformed request %s", raw)
		}
	}
}
