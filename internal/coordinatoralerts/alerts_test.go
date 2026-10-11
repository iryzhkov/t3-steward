package coordinatoralerts

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type testStore struct {
	err       error
	snapshots []domain.WorkerSnapshot
	enrolled  []domain.WorkerEnrollment
	deadlines []time.Duration
}

func (s *testStore) SchemaVersion(ctx context.Context) (int, error) {
	deadline, ok := ctx.Deadline()
	if ok {
		s.deadlines = append(s.deadlines, time.Until(deadline))
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return 1, s.err
}
func (s *testStore) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return s.snapshots, s.err
}
func (s *testStore) LoadWorkerEnrollments(context.Context) ([]domain.WorkerEnrollment, error) {
	return s.enrolled, s.err
}

func handlerFixture() Handler {
	return Handler{Store: &testStore{}, StateDir: "/configured/state",
		FreeBytes: func(string) (uint64, error) { return 6000 * 1024 * 1024, nil },
		Now:       func() time.Time { return time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC) },
	}
}
func request(t *testing.T, h Handler, method, path string, status int) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	if w.Code != status {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, w.Code, status, w.Body.String())
	}
	return w
}
func TestHTTPContract(t *testing.T) {
	t.Parallel()
	h := handlerFixture()
	w := request(t, h, "GET", "/alerts", 200)
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatal(w.Header())
	}
	var report Report
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Schema != "steward-alerts/v1" || !report.ObservedAt.Equal(h.Now()) || len(report.Signals) != 5 {
		t.Fatalf("%+v", report)
	}
	for _, name := range []string{"coordinator", "disk", "litestream", "stuck_attempt", "quota"} {
		signal := report.Signals[name]
		if !signal.ObservedAt.Equal(report.ObservedAt) {
			t.Fatalf("%s: %+v", name, signal)
		}
		request(t, h, "GET", "/alerts/"+name, 200)
		if name == "litestream" && (signal.State != "unavailable" || signal.Detail != "Litestream is not deployed (unit 1.16)") {
			t.Fatalf("%+v", signal)
		}
		if (name == "stuck_attempt" || name == "quota") && (signal.State != "unavailable" || signal.Detail != "not implemented yet") {
			t.Fatalf("%+v", signal)
		}
	}
	paths := []string{"/healthz", "/alerts", "/alerts/coordinator", "/alerts/disk", "/alerts/litestream", "/alerts/stuck_attempt", "/alerts/quota"}
	for _, path := range paths {
		if got := request(t, h, "HEAD", path, 200).Body.Len(); got != 0 {
			t.Fatalf("HEAD body length %d", got)
		}
		for _, method := range []string{"POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE"} {
			response := request(t, h, method, path, 405)
			if response.Header().Get("Allow") != "GET, HEAD" {
				t.Fatal(response.Header())
			}
		}
	}
	for _, path := range []string{"/", "/alerts/no-such-signal", "/alerts/disk/extra", "/healthz/"} {
		request(t, h, "GET", path, 404)
	}
}
func TestCoordinatorReadFailureAndTimeout(t *testing.T) {
	t.Parallel()
	h := handlerFixture()
	store := h.Store.(*testStore)
	for _, failure := range []error{nil, errors.New("secret /home/private/prompt")} {
		store.err = failure
		status := 200
		if failure != nil {
			status = 503
		}
		for _, path := range []string{"/healthz", "/alerts/coordinator"} {
			w := request(t, h, "GET", path, status)
			if strings.Contains(w.Body.String(), "/home/private") || strings.Contains(w.Body.String(), "secret") {
				t.Fatal(w.Body.String())
			}
			if request(t, h, "HEAD", path, status).Body.Len() != 0 {
				t.Fatal("HEAD body")
			}
		}
		request(t, h, "GET", "/alerts", 200)
	}
	for _, deadline := range store.deadlines {
		if deadline <= 0 || deadline > storeTimeout {
			t.Fatalf("unbounded deadline %v", deadline)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store.err = nil
	if got := h.coordinator(ctx, h.Now()); got.State != "firing" {
		t.Fatalf("%+v", got)
	}
}
func TestDiskSignals(t *testing.T) {
	t.Parallel()
	mb := func(n int64) *int64 { return &n }
	cases := []struct {
		name            string
		local           *int64
		workspace, temp *int64
		enrolled        bool
		storeErr        error
		threshold       int64
		want            string
	}{
		{"healthy", mb(6000), mb(7000), mb(8000), true, nil, 0, "ok"},
		{"at default boundary", mb(5120), nil, nil, false, nil, 0, "ok"},
		{"local full", mb(5119), mb(6000), mb(6000), true, nil, 0, "firing"},
		{"workspace full", mb(6000), mb(0), mb(7000), true, nil, 0, "firing"},
		{"temp full", mb(6000), mb(7000), mb(5119), true, nil, 0, "firing"},
		{"no readings", nil, nil, nil, false, nil, 0, "unknown"},
		{"missing worker reading", mb(6000), nil, nil, true, nil, 0, "unknown"},
		{"low worker despite local error", nil, mb(0), mb(6000), true, nil, 0, "firing"},
		{"store error", mb(6000), nil, nil, false, errors.New("private path"), 0, "unknown"},
		{"custom threshold", mb(6000), nil, nil, false, nil, 6001, "firing"},
		{"unenrolled ignored", mb(6000), mb(0), mb(0), false, nil, 0, "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := handlerFixture()
			s := h.Store.(*testStore)
			s.err = tc.storeErr
			s.snapshots = []domain.WorkerSnapshot{{WorkerID: "host", Inventory: domain.WorkerInventory{
				Telemetry:  &domain.WorkerTelemetry{WorkspaceFreeMB: tc.workspace, TempFreeMB: tc.temp},
				WebBaseURL: "secret-output", Projects: []domain.WorkerProjectInventory{{Name: "/home/private"}},
			}}}
			if tc.enrolled {
				s.enrolled = []domain.WorkerEnrollment{{Request: domain.WorkerEnrollmentRequest{WorkerID: "host"}, CredentialRef: "secret-credential"}}
			}
			h.MinFreeMB = tc.threshold
			h.FreeBytes = func(path string) (uint64, error) {
				if path != h.StateDir {
					t.Errorf("measured wrong directory %s", path)
				}
				if tc.local == nil {
					return 0, errors.New("/home/private")
				}
				return uint64(*tc.local) * 1024 * 1024, nil
			}
			status := 200
			if tc.want == "firing" || tc.want == "unknown" {
				status = 503
			}
			w := request(t, h, "GET", "/alerts/disk", status)
			var signal Signal
			if err := json.Unmarshal(w.Body.Bytes(), &signal); err != nil {
				t.Fatal(err)
			}
			if signal.State != tc.want {
				t.Fatalf("%+v, want %s", signal, tc.want)
			}
			if request(t, h, "HEAD", "/alerts/disk", status).Body.Len() != 0 {
				t.Fatal("HEAD body")
			}
			all := request(t, h, "GET", "/alerts", 200)
			for _, forbidden := range []string{"secret", "/home/private", "/configured/state"} {
				if strings.Contains(all.Body.String(), forbidden) {
					t.Fatalf("leaked %s: %s", forbidden, all.Body.String())
				}
			}
		})
	}
}
func TestListenerLifecycle(t *testing.T) {
	t.Parallel()
	if listener, err := Start(context.Background(), "", Handler{}); listener != nil || err != nil {
		t.Fatalf("disabled: %v %v", listener, err)
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	if listener, err := Start(context.Background(), occupied.Addr().String(), handlerFixture()); listener != nil || err == nil {
		t.Fatal("binding failure not returned")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := Start(ctx, "127.0.0.1:0", handlerFixture())
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if listener.server.ReadHeaderTimeout != 5*time.Second || listener.server.MaxHeaderBytes != 16<<10 {
		t.Fatal("HTTP bounds")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get("http://" + listener.server.Addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	cancel()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-listener.done:
	default:
		t.Fatal("server still running")
	}
}
