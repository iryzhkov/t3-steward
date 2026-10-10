package backlogadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// rc116ExtendedService answers with fields an rc.116 client does not know:
// an attempt's review gate and a worker's quota runway.
func rc116ExtendedService(t *testing.T) *Service {
	t.Helper()
	reviewed, physical := strings.Repeat("a", 40), strings.Repeat("b", 40)
	gate := domain.EvaluateReviewCompletionGate(
		&domain.ReviewRoundHead{RoundID: "rc-1", Number: 1, CheckpointID: "cp-1", HeadCommit: reviewed, Verdict: "accept", Accepted: true},
		&domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema, Head: physical}, nil)
	now := time.Now().UTC()
	drains := now.Add(time.Hour)
	reader := explainReader{
		records: sqlite.CoordinatorRecords{
			Workflows:    []domain.Workflow{{ID: "w"}},
			WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w"}},
			Tasks:        []domain.Task{{ID: "t", Name: "review", WorkflowID: "w"}},
			Attempts: []domain.Attempt{{ID: "a", WorkflowRunID: "r", TaskID: "t", Number: 1,
				Progress: domain.ProgressFailed, Failure: gate.Failure(), ReviewGate: &gate}},
		},
		workers: []domain.WorkerSnapshot{{WorkerID: "worker", Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
			QuotaObservations: []domain.WorkerQuotaObservation{{Key: domain.BucketKey{ProviderInstanceID: "claude", Window: "5h"},
				Phase: domain.PhaseNormal, UsedPercent: 40, Healthy: true, ObservedAt: now,
				RatePerMinute: 0.8, DrainPercent: 90, DrainsAt: &drains}}}},
	}
	s, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// rc116Schema is every key path an rc.116 client accepts in an extended
// read, as TestWriteV1ResponseSchema wrote it on v0.11.0-rc.116.
func rc116Schema(t *testing.T) keySchema {
	t.Helper()
	text, err := os.ReadFile("rc116_extended_response_schema.txt")
	if err != nil {
		t.Fatal(err)
	}
	return parseKeySchema(string(text))
}

var rc116ExtendedQueries = []Query{
	{Kind: QueryTask, WorkflowRunID: "r", TaskID: "t"},
	{Kind: QueryDiagnose, WorkflowRunID: "r"},
	{Kind: QueryWorkflow, WorkflowRunID: "r"},
	{Kind: QueryWorkflows},
	{Kind: QueryWorkers},
	{Kind: QueryExplanation, WorkflowRunID: "r", TaskID: "t"},
}

// An rc.116 admin client asks every read but status for
// "backlog.admin/v1-extended-read" and decodes the answer strictly. Fields
// added since, such as an attempt's review gate or a worker's quota runway,
// made task show, task result, workflow and explain reads, and workers, fail
// with "unknown field" against a coordinator upgraded first. The old client
// is played over the real local framing, sending its own extended query.
func TestRC116ExtendedReadsKeepTheRC116Shape(t *testing.T) {
	schema := rc116Schema(t)
	service := rc116ExtendedService(t)
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: service.Query})
	defer stopLocalTransport(t, cancel, done)
	for _, query := range rc116ExtendedQueries {
		t.Run(string(query.Kind), func(t *testing.T) {
			conn, err := net.Dial("unix", client.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(testtiming.Bound(5 * time.Second)))
			sent := query
			sent.Version = "backlog.admin/v1-extended-read"
			if err := writeLocalJSON(conn, localRequest{Version: LocalTransportVersion, Operation: localOperationQuery, Query: &sent}); err != nil {
				t.Fatal(err)
			}
			var frame struct {
				Version  string          `json:"version"`
				Error    string          `json:"error"`
				Response json.RawMessage `json:"response"`
			}
			if err := readLocalJSON(conn, client.MaxResponseBytes, &frame); err != nil {
				t.Fatal(err)
			}
			if frame.Error != "" || len(frame.Response) == 0 {
				t.Fatalf("extended %s: error %q, response %s", query.Kind, frame.Error, frame.Response)
			}
			var tree any
			if err := json.Unmarshal(frame.Response, &tree); err != nil {
				t.Fatal(err)
			}
			var refused []string
			schema.violations(tree, "", &refused)
			sort.Strings(refused)
			if len(refused) != 0 {
				t.Fatalf("an rc.116 client refuses the extended %s response: unknown fields %v", query.Kind, refused)
			}
		})
	}
}

// Every field of every extended read is projected at any depth, including the
// ones a later release adds, and nothing an rc.116 client reads is removed.
func TestRC116ExtendedProjectionIsCompleteAndKeepsRC116Fields(t *testing.T) {
	schema := rc116Schema(t)
	current := parseKeySchema(strings.Join(jsonKeySchema(reflect.TypeOf(Response{})), "\n"))
	var missing []string
	for path := range schema.paths {
		if !current.paths[path] {
			missing = append(missing, path)
		}
	}
	if len(schema.paths) == 0 || len(missing) != 0 {
		sort.Strings(missing)
		t.Fatalf("rc.116 extended fields are gone: %v", missing)
	}

	var response Response
	populateForSchema(reflect.ValueOf(&response).Elem(), map[reflect.Type]bool{})
	refused := func() []string {
		raw, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		var tree any
		if err := json.Unmarshal(raw, &tree); err != nil {
			t.Fatal(err)
		}
		var out []string
		schema.violations(tree, "", &out)
		sort.Strings(out)
		return out
	}
	if len(refused()) == 0 {
		t.Fatal("sensitivity: a fully populated response carries no field rc.116 refuses")
	}
	if err := projectRC116ExtendedResponse(&response); err != nil {
		t.Fatal(err)
	}
	if out := refused(); len(out) != 0 {
		t.Fatalf("the rc.116 extended projection leaves fields rc.116 refuses: %v", out)
	}
	// The projection keeps what rc.116 knows, such as H1's review verdict.
	if response.Task == nil || response.Task.Attempt == nil || response.Task.Attempt.ReviewVerdict == nil {
		t.Fatalf("the projection dropped fields rc.116 declares: %+v", response.Task)
	}
}

// This release's client asks for its own read version first, then for the
// rc.116 extended version, then for v1, each step only after the exact
// unsupported-version refusal. Against each coordinator it gets the fields
// that coordinator can give and nothing its own decoder would refuse.
func TestReadsNegotiateWithCurrentRC116AndRC115Coordinators(t *testing.T) {
	const rc116Version = "backlog.admin/v1-extended-read"
	for _, tc := range []struct {
		coordinator string
		knows       map[string]bool
		calls       int32
	}{
		{coordinator: "current", calls: 1},
		{coordinator: "rc.121", knows: map[string]bool{Version: true, rc116Version: true, RC117ReadVersion: true, RC118ReadVersion: true, RC119ReadVersion: true, RC120ReadVersion: true}, calls: 2},
		{coordinator: "rc.119", knows: map[string]bool{Version: true, rc116Version: true, RC117ReadVersion: true, RC118ReadVersion: true, RC119ReadVersion: true}, calls: 3},
		{coordinator: "rc.118", knows: map[string]bool{Version: true, rc116Version: true, RC117ReadVersion: true, RC118ReadVersion: true}, calls: 4},
		{coordinator: "rc.117", knows: map[string]bool{Version: true, rc116Version: true, RC117ReadVersion: true}, calls: 5},
		{coordinator: "rc.116", knows: map[string]bool{Version: true, rc116Version: true}, calls: 6},
		{coordinator: "rc.115", knows: map[string]bool{Version: true}, calls: 7},
	} {
		t.Run(tc.coordinator, func(t *testing.T) {
			service := rc116ExtendedService(t)
			var mu sync.Mutex
			var calls int32
			var versions []string
			q := func(ctx context.Context, query Query) (Response, error) {
				mu.Lock()
				calls++
				versions = append(versions, query.Version)
				mu.Unlock()
				if tc.knows != nil && !tc.knows[query.Version] {
					return Response{}, fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, query.Version, Version)
				}
				return service.Query(ctx, query)
			}
			client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: q})
			defer stopLocalTransport(t, cancel, done)
			got, err := client.Query(context.Background(), Query{Version: Version, Kind: QueryTask, WorkflowRunID: "r", TaskID: "t"})
			if err != nil || got.Task == nil || got.Task.Attempt == nil {
				t.Fatalf("task: %+v %v", got, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != tc.calls {
				t.Fatalf("%s: %d calls with versions %v, want %d", tc.coordinator, calls, versions, tc.calls)
			}
			// Only a coordinator of rc.117 or later sends the review gate; an
			// rc.116 or rc.115 one cannot, and the older shapes leave it out.
			if (got.Task.Attempt.ReviewGate != nil) != (tc.coordinator == "current" || tc.coordinator == "rc.121" || tc.coordinator == "rc.119" || tc.coordinator == "rc.118" || tc.coordinator == "rc.117") {
				t.Fatalf("%s: review gate %+v", tc.coordinator, got.Task.Attempt.ReviewGate)
			}
		})
	}
}
