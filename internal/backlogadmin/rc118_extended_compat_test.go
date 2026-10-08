package backlogadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func rc118RankedService(t *testing.T) *Service {
	t.Helper()
	selection := domain.RoleSelection{Role: "execute", Route: "codex/gpt", Effort: "medium", PolicyDigest: "digest", Reason: "healthy candidate", Ranking: domain.RouteRankingV1, ResolvedAt: time.Now().UTC(),
		Candidates: []domain.RoleCandidateVerdict{{Route: "codex/gpt", Eligible: true, Reason: "healthy", Ordinal: 1, Band: "healthy", Pool: "codex-pool"}}}
	task := domain.Task{ID: "t", Name: "execute", WorkflowID: "w", Role: "execute", RoleSelection: &selection}
	reader := explainReader{records: sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "w"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w", RouteSelections: map[string]domain.RoleSelection{"t": selection}, Graph: &domain.GraphDefinition{RunID: "r", Revision: 1, Tasks: []domain.Task{task}}}},
		Tasks:        []domain.Task{task},
	}}
	service, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetViability(ViabilitySettings{Projects: []backlog.ProjectDefinition{{Name: "p"}}, ResolveRoles: func(_ context.Context, _ []ViabilityTask, _ []Project, _ RoleWorkerEligible) (map[string]domain.RoleSelection, map[string]ViabilityReason) {
		return map[string]domain.RoleSelection{"t": selection}, nil
	}})
	return service
}

// The frozen schema is consulted at the shared query boundary, including all
// task/run/graph receipt copies and viability, over the actual local framing.
func TestRC118ReadsFreezeRankedReceiptsAtEveryDepth(t *testing.T) {
	service := rc118RankedService(t)
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: service.Query})
	defer stopLocalTransport(t, cancel, done)
	queries := []Query{
		{Kind: QueryTask, WorkflowRunID: "r", TaskID: "t"},
		{Kind: QueryWorkflow, WorkflowRunID: "r"},
		{Kind: QueryGraph, WorkflowRunID: "r"},
		{Kind: QueryDiagnose, WorkflowRunID: "r"},
		{Kind: QueryViability, Viability: &ViabilityRequest{Tasks: []ViabilityTask{{Name: "t", Project: "p", Role: "execute"}}}},
	}
	for _, q := range queries {
		t.Run(string(q.Kind), func(t *testing.T) {
			for _, version := range []string{"backlog.admin/v1-extended-read-rc118", CurrentReadVersion} {
				q.Version = version
				conn, err := net.Dial("unix", client.Path)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if err := writeLocalJSON(conn, localRequest{Version: LocalTransportVersion, Operation: localOperationQuery, Query: &q}); err != nil {
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
				if frame.Error != "" {
					t.Fatal(frame.Error)
				}
				var tree any
				if err := json.Unmarshal(frame.Response, &tree); err != nil {
					t.Fatal(err)
				}
				var refused []string
				rc118ExtendedResponseSchema.violations(tree, "", &refused)
				if version == RC118ReadVersion {
					if len(refused) != 0 {
						t.Fatalf("rc118 strict response rejects: %v", refused)
					}
					if q.Kind != QueryGraph && !bytes.Contains(frame.Response, []byte(`"policyDigest":"digest"`)) {
						t.Fatalf("old receipt disappeared: %s", frame.Response)
					}
				} else if q.Kind != QueryGraph {
					if len(refused) == 0 || !bytes.Contains(frame.Response, []byte(`"ranking":"route-ranking/v1"`)) || !bytes.Contains(frame.Response, []byte(`"pool":"codex-pool"`)) {
						t.Fatalf("current ranking disappeared: %s", frame.Response)
					}
				}
			}
		})
	}
}

func TestRC118ProjectionCompleteAndKeepsFrozenFields(t *testing.T) {
	schema := rc118ExtendedResponseSchema
	current := parseKeySchema(strings.Join(jsonKeySchema(reflect.TypeOf(Response{})), "\n"))
	var missing []string
	for path := range schema.paths {
		if !current.paths[path] {
			missing = append(missing, path)
		}
	}
	if len(schema.paths) == 0 || len(missing) != 0 {
		sort.Strings(missing)
		t.Fatalf("rc118 fields missing: %v", missing)
	}
	var response Response
	populateForSchema(reflect.ValueOf(&response).Elem(), map[reflect.Type]bool{})
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var original any
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	var refused []string
	schema.violations(original, "", &refused)
	if len(refused) == 0 {
		t.Fatal("sensitivity: full current response has no rc118-incompatible fields")
	}
	// Exact JSON projection retains every frozen key and its value, at all depths.
	schema.prune(original, "")
	if err := projectRC118ExtendedResponse(&response); err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var projected any
	if err := json.Unmarshal(raw, &projected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, projected) {
		t.Fatal("projection dropped or changed frozen fields")
	}
}
