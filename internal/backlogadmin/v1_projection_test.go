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
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// violations lists the key paths of value, the JSON at path, that a strict
// decoder with this schema refuses.
func (s keySchema) violations(value any, path string, out *[]string) {
	switch value := value.(type) {
	case []any:
		for _, item := range value {
			s.violations(item, path, out)
		}
	case map[string]any:
		if s.whole[path] {
			return
		}
		for key, item := range value {
			switch {
			case s.paths[path+"."+key]:
				s.violations(item, path+"."+key, out)
			case s.paths[path+".*"]:
				s.violations(item, path+".*", out)
			default:
				*out = append(*out, path+"."+key)
			}
		}
	}
}

// rc115Refuses lists what an rc.115 strict client refuses in a response.
func rc115Refuses(t *testing.T, raw []byte) []string {
	t.Helper()
	var tree any
	if err := json.Unmarshal(raw, &tree); err != nil {
		t.Fatal(err)
	}
	var refused []string
	v1ResponseSchema.violations(tree, "", &refused)
	sort.Strings(refused)
	return refused
}

var v1CompatQueries = []Query{
	{Version: Version, Kind: QueryTask, WorkflowRunID: "r", TaskID: "t"},
	{Version: Version, Kind: QueryDiagnose, WorkflowRunID: "r"},
	{Version: Version, Kind: QueryWorkflow, WorkflowRunID: "r"},
	{Version: Version, Kind: QueryWorkflows},
	{Version: Version, Kind: QueryWorkers},
	{Version: Version, Kind: QueryExplanation, WorkflowRunID: "r", TaskID: "t"},
}

// An rc.115 admin client decodes responses strictly, so every v1 read, not
// only explain, keeps the shape v1 had in that release: task show of an
// attempt with a review verdict, and diagnose with the explanations it
// embeds, used to fail with "unknown field". The old client is played over
// the real local framing, sending the historical query without negotiation.
func TestV1ReadsKeepTheRC115ShapeForEveryQuery(t *testing.T) {
	service := explanationService(t)
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: service.Query})
	defer stopLocalTransport(t, cancel, done)
	for _, query := range v1CompatQueries {
		t.Run(string(query.Kind)+"/"+query.WorkflowRunID, func(t *testing.T) {
			conn, err := net.Dial("unix", client.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(testtiming.Bound(5 * time.Second)))
			sent := query
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
				t.Fatalf("v1 %s: error %q, response %s", query.Kind, frame.Error, frame.Response)
			}
			if refused := rc115Refuses(t, frame.Response); len(refused) != 0 {
				t.Fatalf("an rc.115 client refuses the v1 %s response: unknown fields %v", query.Kind, refused)
			}
		})
	}

	// Sensitivity: the fixture does carry fields rc.115 refuses, in the task
	// and in diagnose's own explanations, so the checks above test something.
	for _, query := range v1CompatQueries[:2] {
		extended := query
		extended.Version = ExtendedReadVersion
		response, err := service.Query(context.Background(), extended)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		refused := strings.Join(rc115Refuses(t, raw), " ")
		want := map[QueryKind]string{QueryTask: ".task.attempt.reviewVerdict", QueryDiagnose: ".diagnosis.explanations.reviewVerdict"}[query.Kind]
		if !strings.Contains(refused, want) {
			t.Fatalf("extended %s: refused %q, want it to include %s", query.Kind, refused, want)
		}
	}
}

// Every field of every v1 read is projected, at any depth, including the
// ones a later release adds: a response with every field set is refused
// nothing by the rc.115 schema once projected. A field added without
// omitempty would survive as its zero value and fail here.
func TestV1ProjectionLeavesNothingRC115Refuses(t *testing.T) {
	var response Response
	populateForSchema(reflect.ValueOf(&response).Elem(), map[reflect.Type]bool{})
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if refused := rc115Refuses(t, raw); len(refused) == 0 {
		t.Fatal("sensitivity: a fully populated response carries no field rc.115 refuses")
	}
	if err := projectV1Response(&response); err != nil {
		t.Fatal(err)
	}
	if raw, err = json.Marshal(response); err != nil {
		t.Fatal(err)
	}
	if refused := rc115Refuses(t, raw); len(refused) != 0 {
		t.Fatalf("the v1 projection leaves fields rc.115 refuses: %v", refused)
	}
	// The projection keeps what rc.115 knows.
	if response.Task == nil || response.Task.Attempt == nil || response.Task.Attempt.ID != "x" ||
		response.Diagnosis == nil || len(response.Diagnosis.Explanations) != 1 || response.Diagnosis.Explanations[0].TaskID != "x" {
		t.Fatalf("the projection dropped fields rc.115 declares: %+v", response.Task)
	}
}

// The frozen schemas stay subsets of this release's answers: no field an
// rc.115 client reads was removed or renamed.
func TestV1ResponseSchemaIsKeptByThisRelease(t *testing.T) {
	frozen := map[string]keySchema{
		"v1_response_schema.txt":         v1ResponseSchema,
		"v1_graph_amendment_schema.txt":  v1GraphAmendmentSchema,
		"v1_unknown_recovery_schema.txt": v1UnknownRecoverySchema,
	}
	for name, answer := range v1SchemaTypes {
		current := parseKeySchema(strings.Join(jsonKeySchema(answer), "\n"))
		var missing []string
		for path := range frozen[name].paths {
			if !current.paths[path] {
				missing = append(missing, path)
			}
		}
		sort.Strings(missing)
		if len(frozen[name].paths) == 0 || len(missing) != 0 {
			t.Fatalf("%s: rc.115 fields are gone: %v", name, missing)
		}
	}
}

// fullAnswerService answers a graph amendment and an unknown recovery with
// every field set, as a coordinator of this release can.
type fullAnswerService struct {
	localTransportService
}

func (*fullAnswerService) AmendGraph(context.Context, Principal, domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
	var result domain.GraphAmendmentResult
	populateForSchema(reflect.ValueOf(&result).Elem(), map[reflect.Type]bool{})
	return result, nil
}

func (*fullAnswerService) RecoverUnknown(context.Context, Principal, UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error) {
	var decision domain.UnknownAssignmentRecoveryDecision
	populateForSchema(reflect.ValueOf(&decision).Elem(), map[reflect.Type]bool{})
	return decision, nil
}

// The graph amendment and unknown recovery answers carry no version to
// negotiate with, and an rc.115 client decodes them strictly too: a task's
// resource preset or review output in the amended graph, or an attempt's
// review verdict in the recovery decision, made it fail after the coordinator
// had already applied the request. The transports answer every client in the
// rc.115 shape, which this release's clients read as well.
func TestMutationAnswersKeepTheRC115Shape(t *testing.T) {
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &fullAnswerService{})
	defer stopLocalTransport(t, cancel, done)
	for _, tc := range []struct {
		name    string
		request localRequest
		schema  keySchema
		answer  reflect.Type
	}{
		{name: "graph amendment", schema: v1GraphAmendmentSchema, answer: reflect.TypeOf(domain.GraphAmendmentResult{}),
			request: localRequest{Operation: localOperationGraphAmendment, GraphAmendment: &domain.GraphAmendment{ID: "amend", RunID: "r"}}},
		{name: "unknown recovery", schema: v1UnknownRecoverySchema, answer: reflect.TypeOf(domain.UnknownAssignmentRecoveryDecision{}),
			request: localRequest{Operation: localOperationUnknownRecovery, UnknownRecovery: &UnknownRecoveryRequest{ID: "recovery", AssignmentID: "assignment"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Sensitivity: the answer the service gives does carry fields
			// rc.115 refuses.
			full := reflect.New(tc.answer)
			populateForSchema(full.Elem(), map[reflect.Type]bool{})
			raw, err := json.Marshal(full.Interface())
			if err != nil {
				t.Fatal(err)
			}
			var tree any
			if err := json.Unmarshal(raw, &tree); err != nil {
				t.Fatal(err)
			}
			var refused []string
			if tc.schema.violations(tree, "", &refused); len(refused) == 0 {
				t.Fatal("sensitivity: the full answer carries nothing rc.115 refuses")
			}

			conn, err := net.Dial("unix", client.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(testtiming.Bound(5 * time.Second)))
			request := tc.request
			request.Version = LocalTransportVersion
			if err := writeLocalJSON(conn, request); err != nil {
				t.Fatal(err)
			}
			var frame struct {
				Version                 string          `json:"version"`
				Error                   string          `json:"error"`
				GraphAmendment          json.RawMessage `json:"graphAmendment"`
				UnknownRecoveryResponse json.RawMessage `json:"unknownRecoveryResponse"`
			}
			if err := readLocalJSON(conn, client.MaxResponseBytes, &frame); err != nil {
				t.Fatal(err)
			}
			answer := append(frame.GraphAmendment, frame.UnknownRecoveryResponse...)
			if frame.Error != "" || len(answer) == 0 {
				t.Fatalf("error %q, answer %s", frame.Error, answer)
			}
			tree = nil
			if err := json.Unmarshal(answer, &tree); err != nil {
				t.Fatal(err)
			}
			refused = nil
			if tc.schema.violations(tree, "", &refused); len(refused) != 0 {
				sort.Strings(refused)
				t.Fatalf("an rc.115 client refuses the %s answer: unknown fields %v", tc.name, refused)
			}
		})
	}
}

// This release's clients ask every read but status for the extended
// version, and fall back to v1, once, against a coordinator that refuses it.
func TestTaskReadNegotiatesTheExtendedVersion(t *testing.T) {
	for _, older := range []bool{false, true} {
		t.Run(fmt.Sprintf("older=%v", older), func(t *testing.T) {
			service := explanationService(t)
			var calls atomic.Int32
			q := func(ctx context.Context, query Query) (Response, error) {
				calls.Add(1)
				if older && query.Version != Version {
					return Response{}, fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, query.Version, Version)
				}
				return service.Query(ctx, query)
			}
			client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), &statusTransportService{query: q})
			defer stopLocalTransport(t, cancel, done)
			got, err := client.Query(context.Background(), v1CompatQueries[0])
			if err != nil || got.Task == nil || got.Task.Attempt == nil {
				t.Fatalf("task: %+v %v", got, err)
			}
			wantCalls := int32(1)
			if older {
				wantCalls = 6
			}
			if (got.Task.Attempt.ReviewVerdict != nil) == older || calls.Load() != wantCalls {
				t.Fatalf("older=%v: verdict %+v, calls %d", older, got.Task.Attempt.ReviewVerdict, calls.Load())
			}
		})
	}
}

// populateForSchema sets every field reachable from v, once per type on the
// current path, so the JSON of v has every key its type can have.
func populateForSchema(v reflect.Value, stack map[reflect.Type]bool) {
	switch {
	case v.Type() == reflect.TypeOf(time.Time{}):
		v.Set(reflect.ValueOf(time.Unix(1, 0).UTC()))
		return
	case v.Type() == reflect.TypeOf(json.RawMessage{}):
		v.Set(reflect.ValueOf(json.RawMessage(`{}`)))
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if stack[v.Type().Elem()] {
			return
		}
		v.Set(reflect.New(v.Type().Elem()))
		populateForSchema(v.Elem(), stack)
	case reflect.Struct:
		if stack[v.Type()] {
			return
		}
		stack[v.Type()] = true
		defer delete(stack, v.Type())
		for index := 0; index < v.NumField(); index++ {
			if v.Field(index).CanSet() {
				populateForSchema(v.Field(index), stack)
			}
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			v.SetBytes([]byte("x"))
			return
		}
		if stack[v.Type().Elem()] {
			return
		}
		items := reflect.MakeSlice(v.Type(), 1, 1)
		populateForSchema(items.Index(0), stack)
		v.Set(items)
	case reflect.Map:
		entries := reflect.MakeMap(v.Type())
		key := reflect.New(v.Type().Key()).Elem()
		populateForSchema(key, stack)
		value := reflect.New(v.Type().Elem()).Elem()
		populateForSchema(value, stack)
		entries.SetMapIndex(key, value)
		v.Set(entries)
	case reflect.String:
		v.SetString("x")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	case reflect.Bool:
		v.SetBool(true)
	}
}
