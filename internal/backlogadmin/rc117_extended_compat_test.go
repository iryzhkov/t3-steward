package backlogadmin

import (
	"encoding/json"
	"net"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// rc117ExtendedService answers with fields an rc.117 client does not know: a
// review gate's round budget (M16-4), a task's role selection (M17-3) and a
// rerun's reused failed commits (A3).
func rc117ExtendedService(t *testing.T) *Service {
	t.Helper()
	head := strings.Repeat("d", 40)
	gate := domain.EvaluateReviewCompletionGate(
		&domain.ReviewRoundHead{RoundID: "round-2", Number: 2, CheckpointID: "cp-2", HeadCommit: head, Verdict: "reject", RoundsUsed: 2, RoundLimit: 2},
		&domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema, Head: head}, nil)
	if gate.RoundLimit == 0 {
		t.Fatal("sensitivity: the gate carries no round budget")
	}
	now := time.Now().UTC()
	selection := domain.RoleSelection{Role: "review", Route: "codex/gpt", Effort: "medium", PolicyDigest: "digest", Reason: "first eligible candidate in policy order", ResolvedAt: now}
	reused := []domain.ReusedCommit{{Producer: "implement", Name: "unit", Commit: head, SourceAttemptID: "a-0", VerificationFailures: []string{"verification command failed (1): false"}}}
	task := domain.Task{ID: "t", Name: "review", WorkflowID: "w", Role: "review", RoleEffort: "medium", RoleSelection: &selection}
	reader := explainReader{
		records: sqlite.CoordinatorRecords{
			Workflows: []domain.Workflow{{ID: "w"}},
			WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w", RouteSelections: map[string]domain.RoleSelection{"t": selection},
				Graph: &domain.GraphDefinition{RunID: "r", Revision: 1, Tasks: []domain.Task{task}, RerunOf: &domain.RerunProvenance{SourceRunID: "r-0", SourceTaskID: "t", ReusedCommits: &reused}}}},
			Tasks: []domain.Task{task},
			Attempts: []domain.Attempt{{ID: "a", WorkflowRunID: "r", TaskID: "t", Number: 1,
				Progress: domain.ProgressFailed, Failure: gate.Failure(), ReviewGate: &gate}},
		},
	}
	s, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func rc117Schema(t *testing.T) keySchema {
	t.Helper()
	text, err := os.ReadFile("rc117_extended_response_schema.txt")
	if err != nil {
		t.Fatal(err)
	}
	return parseKeySchema(string(text))
}

// An rc.117 admin client asks every read but status for
// "backlog.admin/v1-extended-read-rc117" and decodes the answer strictly. The
// rc.118 units add fields to those answers, so task show, task result,
// workflow, explain and diagnose from an rc.117 client failed with "unknown
// field" against a coordinator upgraded first. The old client is played over
// the real local framing, sending its own extended query.
func TestRC117ExtendedReadsKeepTheRC117Shape(t *testing.T) {
	schema := rc117Schema(t)
	service := rc117ExtendedService(t)
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: service.Query})
	defer stopLocalTransport(t, cancel, done)
	for _, query := range rc116ExtendedQueries {
		t.Run(string(query.Kind), func(t *testing.T) {
			conn, err := net.Dial("unix", client.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			sent := query
			sent.Version = "backlog.admin/v1-extended-read-rc117"
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
				t.Fatalf("rc.117 extended %s: error %q, response %s", query.Kind, frame.Error, frame.Response)
			}
			var tree any
			if err := json.Unmarshal(frame.Response, &tree); err != nil {
				t.Fatal(err)
			}
			var refused []string
			schema.violations(tree, "", &refused)
			sort.Strings(refused)
			if len(refused) != 0 {
				t.Fatalf("an rc.117 client refuses the extended %s response: unknown fields %v", query.Kind, refused)
			}
		})
	}
	// This release's own read still carries the new fields.
	got, err := service.Query(t.Context(), Query{Version: CurrentReadVersion, Kind: QueryTask, WorkflowRunID: "r", TaskID: "t"})
	if err != nil || got.Task == nil || got.Task.Attempt == nil || got.Task.Attempt.ReviewGate == nil || got.Task.Attempt.ReviewGate.RoundLimit != 2 || got.Task.Task.RoleSelection == nil {
		t.Fatalf("current read lost rc.118 fields: %+v %v", got.Task, err)
	}
}

// Every field of every rc.117 extended read is projected at any depth, and
// nothing an rc.117 client reads is removed.
func TestRC117ExtendedProjectionIsCompleteAndKeepsRC117Fields(t *testing.T) {
	schema := rc117Schema(t)
	current := parseKeySchema(strings.Join(jsonKeySchema(reflect.TypeOf(Response{})), "\n"))
	var missing []string
	for path := range schema.paths {
		if !current.paths[path] {
			missing = append(missing, path)
		}
	}
	if len(schema.paths) == 0 || len(missing) != 0 {
		sort.Strings(missing)
		t.Fatalf("rc.117 extended fields are gone: %v", missing)
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
		t.Fatal("sensitivity: a fully populated response carries no field rc.117 refuses")
	}
	if err := projectRC117ExtendedResponse(&response); err != nil {
		t.Fatal(err)
	}
	if out := refused(); len(out) != 0 {
		t.Fatalf("the rc.117 extended projection leaves fields rc.117 refuses: %v", out)
	}
	// The projection keeps what rc.117 knows, such as M16-3's review gate.
	if response.Task == nil || response.Task.Attempt == nil || response.Task.Attempt.ReviewGate == nil {
		t.Fatalf("the projection dropped fields rc.117 declares: %+v", response.Task)
	}
}
