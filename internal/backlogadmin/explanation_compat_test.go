package backlogadmin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// Frozen d2e8276 (rc.115) Blocker and Explanation: kept independent of the
// live types, so a field this release adds is an unknown field here.
type parentBlocker struct {
	Code            string                        `json:"code"`
	Detail          string                        `json:"detail"`
	DependsOn       string                        `json:"dependsOn,omitempty"`
	WorkerID        string                        `json:"workerId,omitempty"`
	QuotaPoolID     string                        `json:"quotaPoolId,omitempty"`
	Resource        string                        `json:"resource,omitempty"`
	OwnerID         string                        `json:"ownerId,omitempty"`
	EarliestAt      *time.Time                    `json:"earliestAt,omitempty"`
	GateID          string                        `json:"gateId,omitempty"`
	HoldID          string                        `json:"holdId,omitempty"`
	SupervisionCode domain.SupervisionBlockerCode `json:"supervisionCode,omitempty"`
}

type parentExplanation struct {
	WorkflowRunID string          `json:"workflowRunId"`
	TaskID        string          `json:"taskId"`
	AttemptID     string          `json:"attemptId,omitempty"`
	Eligible      bool            `json:"eligible"`
	Summary       string          `json:"summary"`
	EarliestAt    *time.Time      `json:"earliestAt,omitempty"`
	Blockers      []parentBlocker `json:"blockers"`
	Details       []string        `json:"details,omitempty"`
}

type parentExplanationResponse struct {
	Version  string `json:"version"`
	Response *struct {
		Version     string             `json:"version"`
		Kind        QueryKind          `json:"kind"`
		GeneratedAt time.Time          `json:"generatedAt"`
		Explanation *parentExplanation `json:"explanation,omitempty"`
	} `json:"response,omitempty"`
}

// explanationService answers an explanation of a placed attempt whose
// structured review verdict is recorded, so it has both fields this release
// added to an explanation.
func explanationService(t *testing.T) *Service {
	t.Helper()
	verdict := &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 2, FindingTitles: []string{"lost evidence"}}
	decision := &domain.PlacementDecision{SelectedWorkerID: "original", ResourceEvaluations: []domain.ResourceEvaluation{{WorkerID: "original", State: "known", Rank: 1}}}
	reader := explainReader{records: sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "w"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w"}},
		Tasks:        []domain.Task{{ID: "t", Name: "review", WorkflowID: "w"}},
		Attempts:     []domain.Attempt{{ID: "a", WorkflowRunID: "r", TaskID: "t", Number: 1, Progress: domain.ProgressSucceeded, ReviewVerdict: verdict}},
		Assignments:  []domain.Assignment{{ID: "assignment", AttemptID: "a", Placement: decision}},
	}}
	s, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var explanationQuery = Query{Version: Version, Kind: QueryExplanation, WorkflowRunID: "r", TaskID: "t"}

// An rc.115 client decodes the local admin framing strictly, so a v1
// explanation keeps its frozen shape: the review verdict and the placement
// decision reach only a client that asks for them, and this release's client
// does ask.
func TestExplanationStrictParentAndNewLocalClient(t *testing.T) {
	service := explanationService(t)
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: service.Query})
	defer stopLocalTransport(t, cancel, done)

	// The parent client sends the historical query over the actual framed
	// strict codec; it does not pass through the negotiation helper.
	conn, err := net.Dial("unix", client.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(testtiming.Bound(time.Second)))
	query := explanationQuery
	if err := writeLocalJSON(conn, localRequest{Version: LocalTransportVersion, Operation: localOperationQuery, Query: &query}); err != nil {
		t.Fatal(err)
	}
	var old parentExplanationResponse
	if err := readLocalJSON(conn, client.MaxResponseBytes, &old); err != nil {
		t.Fatalf("rc.115 client cannot read the explanation: %v", err)
	}
	if old.Response == nil || old.Response.Explanation == nil || old.Response.Explanation.TaskID != "t" || old.Response.Explanation.AttemptID != "a" {
		t.Fatalf("parent explanation: %+v", old)
	}

	got, err := client.Query(context.Background(), explanationQuery)
	if err != nil || got.Explanation == nil {
		t.Fatalf("new explanation: %+v %v", got, err)
	}
	if got.Explanation.Placement == nil || got.Explanation.Placement.SelectedWorkerID != "original" ||
		got.Explanation.ReviewVerdict == nil || got.Explanation.ReviewVerdict.Verdict != "changes-requested" {
		t.Fatalf("the new client lost placement or verdict: %+v", got.Explanation)
	}

	// Sensitivity: the extended projection really is incompatible with the
	// frozen strict shape, rather than a permissive test hiding the defect.
	extended, err := service.Query(context.Background(), Query{Version: ExtendedReadVersion, Kind: QueryExplanation, WorkflowRunID: "r", TaskID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	var frame bytes.Buffer
	if err := writeLocalJSON(&frame, localResponse{Version: LocalTransportVersion, Response: &extended}); err != nil {
		t.Fatal(err)
	}
	if err := readLocalJSON(&frame, client.MaxResponseBytes, &old); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("extended parent decode: %v", err)
	}
}

// A coordinator older than this release refuses the extended version; the
// client then asks again with v1, once, and reads the explanation without the
// new fields. No other failure is retried.
func TestExplanationOlderLocalCoordinatorAndFailures(t *testing.T) {
	for _, mode := range []string{"older", "auth", "other-rejected"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			service := explanationService(t)
			q := func(ctx context.Context, query Query) (Response, error) {
				calls.Add(1)
				if query.Version != Version {
					switch mode {
					case "older":
						return Response{}, fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, query.Version, Version)
					case "auth":
						return Response{}, &TransportError{Class: ClassAuthentication, Err: errors.New("credential refused")}
					default:
						return Response{}, errors.New("authorize explanation: unsupported backlog admin version")
					}
				}
				return service.Query(ctx, query)
			}
			client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: q})
			defer stopLocalTransport(t, cancel, done)
			got, err := client.Query(context.Background(), explanationQuery)
			if mode == "older" {
				// It refuses rc.120 through rc.116 read versions before v1.
				if err != nil || got.Explanation == nil || got.Explanation.AttemptID != "a" || calls.Load() != 6 {
					t.Fatalf("older: %+v %v calls=%d", got, err, calls.Load())
				}
				if got.Explanation.Placement != nil || got.Explanation.ReviewVerdict != nil {
					t.Fatalf("a v1 explanation carries the new fields: %+v", got.Explanation)
				}
				return
			}
			if err == nil || calls.Load() != 1 || ClassOf(err) != ClassRejected {
				t.Fatalf("failure masked/reclassified: %v calls=%d", err, calls.Load())
			}
		})
	}
}

// The SSH admin endpoint serves the same framing, so it negotiates the same
// way, against a current and an older coordinator.
func TestExplanationSSHProjectionAndOlderCoordinator(t *testing.T) {
	for _, state := range []string{"current", "older"} {
		t.Run(state, func(t *testing.T) {
			h := newRemoteHarness(t, false)
			service := explanationService(t)
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
			got, err := h.client.Query(context.Background(), explanationQuery)
			if err != nil || got.Explanation == nil {
				t.Fatalf("SSH: %+v %v", got, err)
			}
			extended := got.Explanation.Placement != nil && got.Explanation.ReviewVerdict != nil
			wantCalls := int32(1)
			if state == "older" {
				wantCalls = 6
			}
			if extended != (state == "current") || calls.Load() != wantCalls {
				t.Fatalf("SSH %s: %+v calls=%d", state, got.Explanation, calls.Load())
			}
		})
	}
}

// The extended version is a read of anything but status, which has its own,
// and never a mutation; v1 keeps the explanation's frozen shape.
func TestExtendedReadVersionIsOnlyForReads(t *testing.T) {
	s := explanationService(t)
	if _, err := s.Query(context.Background(), Query{Version: ExtendedReadVersion, Kind: QueryStatus}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("status: %v", err)
	}
	if _, err := s.Query(context.Background(), Query{Version: StatusIntakeVersion, Kind: QueryExplanation, WorkflowRunID: "r", TaskID: "t"}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("explanation under the status version: %v", err)
	}
	if _, err := s.Mutate(context.Background(), Mutation{Version: ExtendedReadVersion}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("mutation: %v", err)
	}
	legacy, err := s.Query(context.Background(), explanationQuery)
	if err != nil || legacy.Explanation == nil || legacy.Explanation.Placement != nil || legacy.Explanation.ReviewVerdict != nil {
		t.Fatalf("v1 explanation: %+v %v", legacy.Explanation, err)
	}
}
