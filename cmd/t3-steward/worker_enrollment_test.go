package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const (
	enrollDesiredDigest  = "1111111111111111111111111111111111111111111111111111111111111111"
	enrollAcceptedDigest = "2222222222222222222222222222222222222222222222222222222222222222"
)

// fakeLocalCoordinator answers the owner-only admin socket the way the
// coordinator would: one framed request per connection, one framed response.
// It records every request so a test can assert the structured enrollment
// that left the command, which is the whole point: the digest and revision
// must come from the coordinator's own answer, never from an operator's hand.
type fakeLocalCoordinator struct {
	t        *testing.T
	requests []map[string]any
	answer   func(request map[string]any) any
	done     chan struct{}
}

func serveFakeCoordinator(t *testing.T, statePath string, answer func(map[string]any) any) *fakeLocalCoordinator {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: statePath + ".admin.sock", Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeLocalCoordinator{t: t, answer: answer, done: make(chan struct{})}
	go func() {
		defer close(fake.done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			fake.serve(conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-fake.done
	})
	return fake
}

func (f *fakeLocalCoordinator) serve(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	var size uint32
	if binary.Read(conn, binary.BigEndian, &size) != nil || size > 1<<20 {
		return
	}
	var request map[string]any
	if json.NewDecoder(io.LimitReader(conn, int64(size))).Decode(&request) != nil {
		return
	}
	f.requests = append(f.requests, request)
	response, _ := json.Marshal(f.answer(request))
	_ = binary.Write(conn, binary.BigEndian, uint32(len(response)))
	_, _ = conn.Write(response)
}

func enrollmentTestConfig(t *testing.T) (string, string) {
	t.Helper()
	root := shortTempDir(t)
	statePath := filepath.Join(root, "state.db")
	configPath := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(configPath, []byte("state_path: "+statePath+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return statePath, configPath
}

func workersAnswer(workers ...backlogadmin.Worker) map[string]any {
	return map[string]any{
		"version":  backlogadmin.LocalTransportVersion,
		"response": backlogadmin.Response{Version: backlogadmin.Version, Kind: backlogadmin.QueryWorkers, Workers: workers},
	}
}

func staleWorker(id string, revision int64) backlogadmin.Worker {
	return backlogadmin.Worker{
		State:       "draining",
		Requirement: &domain.WorkerRequirement{WorkerID: id, WorkerEpoch: "worker-1", CatalogRevision: enrollDesiredDigest, Connection: "persistent-ssh"},
		Enrollment: &domain.WorkerEnrollment{
			Request:  domain.WorkerEnrollmentRequest{ID: "first", WorkerID: id, CatalogRevision: enrollAcceptedDigest},
			Revision: revision,
		},
		Snapshot: domain.WorkerSnapshot{WorkerID: id},
	}
}

func currentWorker(id string, revision int64) backlogadmin.Worker {
	worker := staleWorker(id, revision)
	worker.State = "observed"
	worker.Enrolled = true
	worker.Enrollment.Request.CatalogRevision = enrollDesiredDigest
	return worker
}

func enrollmentRequest(t *testing.T, request map[string]any) map[string]any {
	t.Helper()
	if request["operation"] != "worker-enrollment" {
		t.Fatalf("request is not an enrollment: %+v", request)
	}
	body, _ := request["workerEnrollment"].(map[string]any)
	if body == nil {
		t.Fatalf("enrollment request has no body: %+v", request)
	}
	return body
}

// --current-catalog reads the digest the coordinator requires and the worker's
// current enrollment revision from the same workers query `backlog workers
// --json` prints, and submits exactly the request an operator who copied those
// two values by hand would have submitted.
func TestWorkerEnrollCurrentCatalogReadsDigestAndRevisionFromCoordinator(t *testing.T) {
	statePath, configPath := enrollmentTestConfig(t)
	fake := serveFakeCoordinator(t, statePath, func(request map[string]any) any {
		if request["operation"] == "query" {
			return workersAnswer(staleWorker("homelab", 4))
		}
		body, _ := request["workerEnrollment"].(map[string]any)
		return map[string]any{
			"version": backlogadmin.LocalTransportVersion,
			"workerEnrollment": domain.WorkerEnrollment{
				Request:  domain.WorkerEnrollmentRequest{ID: body["id"].(string), WorkerID: "homelab", CatalogRevision: body["catalogRevision"].(string)},
				Revision: 5,
			},
		}
	})
	var out bytes.Buffer
	g := globalFlags{configPath: configPath}
	if err := runWorkerEnroll(g, []string{"homelab", "--current-catalog", "--reason", "re-enroll after project add"}, &out); err != nil {
		t.Fatalf("--current-catalog: %v", err)
	}
	if len(fake.requests) != 2 {
		t.Fatalf("expected one query and one enrollment, got %d requests: %+v", len(fake.requests), fake.requests)
	}
	query, _ := fake.requests[0]["query"].(map[string]any)
	if fake.requests[0]["operation"] != "query" || query["kind"] != string(backlogadmin.QueryWorkers) {
		t.Fatalf("first request is not the workers query: %+v", fake.requests[0])
	}
	derived := enrollmentRequest(t, fake.requests[1])
	if derived["catalogRevision"] != enrollDesiredDigest {
		t.Fatalf("catalog revision %v, want the coordinator's required digest", derived["catalogRevision"])
	}
	if derived["expectedRevision"] != float64(4) {
		t.Fatalf("expected revision %v, want the worker's current enrollment revision 4", derived["expectedRevision"])
	}
	id, _ := derived["id"].(string)
	if !strings.HasPrefix(id, "enroll-homelab-"+enrollDesiredDigest[:12]+"-rev4") {
		t.Fatalf("request id %q is not derived from worker, digest and revision", id)
	}
	var result domain.WorkerEnrollment
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || result.Revision != 5 {
		t.Fatalf("stdout is not the enrollment result: %s (%v)", out.Bytes(), err)
	}

	// The explicit invocation with the same two values produces the same
	// request, apart from the operator-chosen id.
	if err := runWorkerEnroll(g, []string{"homelab", "--request-id", id, "--catalog-revision", enrollDesiredDigest, "--expected-revision", "4", "--reason", "re-enroll after project add"}, &out); err != nil {
		t.Fatalf("explicit invocation: %v", err)
	}
	explicit := enrollmentRequest(t, fake.requests[2])
	for _, key := range []string{"id", "workerId", "catalogRevision", "expectedRevision", "reason"} {
		if derived[key] != explicit[key] {
			t.Fatalf("%s differs: derived %v, explicit %v", key, derived[key], explicit[key])
		}
	}
}

// A worker that has never enrolled is fenced at revision 0, which is what a
// first enrollment must pass.
func TestWorkerEnrollCurrentCatalogFirstEnrollmentFencesAtZero(t *testing.T) {
	statePath, configPath := enrollmentTestConfig(t)
	worker := staleWorker("homelab", 0)
	worker.Enrollment = nil
	worker.State = "configured"
	fake := serveFakeCoordinator(t, statePath, func(request map[string]any) any {
		if request["operation"] == "query" {
			return workersAnswer(worker)
		}
		return map[string]any{"version": backlogadmin.LocalTransportVersion, "workerEnrollment": domain.WorkerEnrollment{Revision: 1}}
	})
	var out bytes.Buffer
	if err := runWorkerEnroll(globalFlags{configPath: configPath}, []string{"homelab", "--current-catalog", "--reason", "first"}, &out); err != nil {
		t.Fatal(err)
	}
	body := enrollmentRequest(t, fake.requests[1])
	if body["expectedRevision"] != float64(0) || body["catalogRevision"] != enrollDesiredDigest {
		t.Fatalf("first enrollment request = %+v", body)
	}
}

// The fenced flags and the self-serving flag are alternatives. Mixing them is
// refused before anything is dialled, so a stale digest typed by hand can
// never override the one the coordinator reports.
func TestWorkerEnrollCurrentCatalogRefusesFencedFlags(t *testing.T) {
	_, configPath := enrollmentTestConfig(t)
	for _, args := range [][]string{
		{"homelab", "--current-catalog", "--catalog-revision", enrollDesiredDigest, "--reason", "x"},
		{"homelab", "--current-catalog", "--expected-revision", "3", "--reason", "x"},
		{"--all", "--reason", "x"},
		{"homelab", "--all", "--current-catalog", "--reason", "x"},
	} {
		var out bytes.Buffer
		err := runWorkerEnroll(globalFlags{configPath: configPath}, args, &out)
		if err == nil || !strings.Contains(err.Error(), "--current-catalog") {
			t.Fatalf("%v: err = %v", args, err)
		}
	}
}

// --all re-enrolls every configured worker whose accepted digest differs from
// the one the coordinator requires, and says per worker what it did.
func TestWorkerEnrollAllCurrentCatalogReportsEachWorker(t *testing.T) {
	statePath, configPath := enrollmentTestConfig(t)
	fake := serveFakeCoordinator(t, statePath, func(request map[string]any) any {
		if request["operation"] == "query" {
			return workersAnswer(staleWorker("homelab", 4), currentWorker("normandy", 2), staleWorker("omarchy-pc", 7))
		}
		body, _ := request["workerEnrollment"].(map[string]any)
		if body["workerId"] == "omarchy-pc" {
			return map[string]any{"version": backlogadmin.LocalTransportVersion, "error": "worker is not ready for effective catalog"}
		}
		return map[string]any{"version": backlogadmin.LocalTransportVersion, "workerEnrollment": domain.WorkerEnrollment{
			Request: domain.WorkerEnrollmentRequest{WorkerID: body["workerId"].(string), CatalogRevision: enrollDesiredDigest}, Revision: 5,
		}}
	})
	var out bytes.Buffer
	err := runWorkerEnroll(globalFlags{configPath: configPath}, []string{"--all", "--current-catalog", "--reason", "catalog changed"}, &out)
	if err == nil || !strings.Contains(err.Error(), "omarchy-pc") {
		t.Fatalf("a refused worker must fail the command and be named: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("want one line per worker, got %q", out.String())
	}
	for _, expected := range []string{
		"homelab: enrolled",
		"normandy: already current",
		"omarchy-pc: refused: ",
		"worker is not ready for effective catalog",
	} {
		if !strings.Contains(out.String(), expected) {
			t.Errorf("output lacks %q:\n%s", expected, out.String())
		}
	}
	enrolled := 0
	for _, request := range fake.requests {
		if request["operation"] == "worker-enrollment" {
			enrolled++
			body := enrollmentRequest(t, request)
			if body["workerId"] == "normandy" {
				t.Fatal("a worker that is already current was re-enrolled")
			}
		}
	}
	if enrolled != 2 {
		t.Fatalf("expected two enrollment requests, got %d", enrolled)
	}
}

// A worker the coordinator has no requirement row for cannot be enrolled to
// "the current catalog" because there is no such digest for it; the refusal
// names the fenced form.
func TestWorkerEnrollCurrentCatalogRefusesUnknownWorker(t *testing.T) {
	statePath, configPath := enrollmentTestConfig(t)
	serveFakeCoordinator(t, statePath, func(request map[string]any) any {
		return workersAnswer(staleWorker("homelab", 4))
	})
	var out bytes.Buffer
	err := runWorkerEnroll(globalFlags{configPath: configPath}, []string{"laptop", "--current-catalog", "--reason", "x"}, &out)
	if err == nil || !strings.Contains(err.Error(), "laptop") || !strings.Contains(err.Error(), "--catalog-revision") {
		t.Fatalf("err = %v", err)
	}
}
