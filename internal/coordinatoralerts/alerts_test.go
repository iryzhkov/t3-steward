package coordinatoralerts

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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
	if listener.server.ReadHeaderTimeout != 5*time.Second || listener.server.MaxHeaderBytes != 16<<10 ||
		listener.server.WriteTimeout != 10*time.Second || listener.server.IdleTimeout != 30*time.Second {
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

type admissionEvent struct {
	record *headerAdmission
	name   string
}

func nextAdmission(t *testing.T, events <-chan admissionEvent, name string) *headerAdmission {
	t.Helper()
	for {
		select {
		case event := <-events:
			if event.name == name {
				return event.record
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("missing admission event %s", name)
		}
	}
}
func admissionFixture(t *testing.T, h http.Handler, hooks listenerHooks) (*Listener, chan admissionEvent) {
	t.Helper()
	events := make(chan admissionEvent, 128)
	observe := hooks.event
	hooks.event = func(a *headerAdmission, name string) {
		events <- admissionEvent{a, name}
		if observe != nil {
			observe(a, name)
		}
	}
	l, err := startWithHooks(context.Background(), "127.0.0.1:0", func(context.Context) http.Handler { return h }, hooks)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, events
}
func h2Transport(t *testing.T) *http.Transport {
	t.Helper()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: protocols, MaxConnsPerHost: 1}
	t.Cleanup(tr.CloseIdleConnections)
	return tr
}
func TestInitialHeaderAdmissionBounds(t *testing.T) {
	preface := "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	for name, prefix := range map[string]string{
		"silent": "", "short H1": "G", "partial H2": preface[:13],
		"full H2": preface, "no HEADERS": preface + "\x00\x00\x00\x04\x00\x00\x00\x00\x00",
		"partial HEADERS": preface + "\x00\x00\x00\x04\x00\x00\x00\x00\x00" + "\x00\x00\x04\x01\x04\x00\x00\x00\x01\x82",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			l, events := admissionFixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }), listenerHooks{})
			conn, err := net.Dial("tcp", l.server.Addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
			if _, err := io.WriteString(conn, prefix); err != nil {
				t.Fatal(err)
			}
			a := nextAdmission(t, events, "armed")
			if a.deadline.Sub(a.acceptedAt) != 5*time.Second {
				t.Fatal("initial absolute bound")
			}
			if name == "no HEADERS" || name == "partial HEADERS" {
				// A server SETTINGS byte proves actual H2 handoff. Independently
				// clear the inherited raw deadline, as stdlib handoff may do.
				var first [1]byte
				if _, err := io.ReadFull(conn, first[:]); err != nil {
					t.Fatal(err)
				}
				if err := a.conn.SetReadDeadline(time.Time{}); err != nil {
					t.Fatal(err)
				}
				if nextAdmission(t, events, "expired") != a {
					t.Fatal("wrong timer owner")
				}
			}
			_, err = io.Copy(io.Discard, conn)
			if err != nil {
				var timeout net.Error
				if errors.As(err, &timeout) && timeout.Timeout() {
					t.Fatal("connection did not close", err)
				}
			}
			_ = l.Close()
			<-a.timerDone
			// net/http may close earlier (for example its SETTINGS timeout).
			if calls.Load() != 0 {
				t.Fatal("incomplete headers dispatched")
			}
		})
	}
}
func TestHTTP1KeepAliveHeaderBounds(t *testing.T) {
	l, events := admissionFixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }), listenerHooks{})
	conn, err := net.Dial("tcp", l.server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	reader := bufio.NewReader(conn)
	for range 2 {
		if _, err := io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != 204 {
			t.Fatal(response.Status)
		}
	}
	a := nextAdmission(t, events, "timerjoined")
	if _, err := io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost:"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = reader.ReadByte()
	if err == nil || time.Since(start) < 4*time.Second {
		t.Fatalf("subsequent headers not bounded: %v", err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != "ready" {
		t.Fatal("first admission lost")
	}
}
func TestHTTP2TransferFirstStreams(t *testing.T) {
	for _, outcome := range []string{"success", "clear failure", "Close"} {
		t.Run(outcome, func(t *testing.T) {
			release, clearing := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var calls, clears, joins atomic.Int32
			l, events := admissionFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.ProtoMajor != 2 {
					t.Error("HTTP2 not negotiated")
				}
				w.WriteHeader(204)
			}), listenerHooks{
				event: func(_ *headerAdmission, name string) {
					if name == "timerjoined" {
						joins.Add(1)
					}
				},
				deadline: func(conn net.Conn, deadline time.Time) error {
					if deadline.IsZero() {
						clears.Add(1)
						close(clearing)
						<-release
						if outcome == "clear failure" {
							return errors.New("injected clear failure")
						}
					}
					return conn.SetReadDeadline(deadline)
				},
			})
			// Cleanup releases a blocked setter before Close waits for its owner.
			t.Cleanup(func() { unblock(); _ = l.Close() })
			tr := h2Transport(t)
			results := make(chan error, 2)
			var jobs sync.WaitGroup
			t.Cleanup(func() { unblock(); _ = l.Close(); jobs.Wait() })
			start := func() {
				jobs.Add(1)
				go func() {
					defer jobs.Done()
					request, err := http.NewRequestWithContext(t.Context(), "POST", "http://"+l.server.Addr+"/healthz", nil)
					if err == nil {
						var response *http.Response
						response, err = tr.RoundTrip(request)
						if response != nil {
							_, _ = io.Copy(io.Discard, response.Body)
							_ = response.Body.Close()
							if response.ProtoMajor != 2 || response.StatusCode != 204 {
								err = fmt.Errorf("unexpected response %s", response.Status)
							}
						}
					}
					results <- err
				}()
			}
			start()
			select {
			case <-clearing:
			case <-time.After(15 * time.Second):
				t.Fatal("clear not entered")
			}
			start()
			a := nextAdmission(t, events, "waiting")
			if calls.Load() != 0 || clears.Load() != 1 || joins.Load() != 1 {
				t.Fatal("dispatch before transfer", calls.Load(), clears.Load(), joins.Load())
			}
			closed := make(chan struct{})
			if outcome == "Close" {
				go func() { _ = l.Close(); close(closed) }()
				nextAdmission(t, events, "closed")
				select {
				case <-closed:
					t.Fatal("Close missed transfer owner")
				default:
				}
			}
			unblock()
			for range 2 {
				select {
				case err := <-results:
					if (err == nil) != (outcome == "success") {
						t.Fatalf("%s request: %v", outcome, err)
					}
				case <-time.After(15 * time.Second):
					t.Fatal("request not joined")
				}
			}
			if outcome == "Close" {
				<-closed
			}
			want := int32(0)
			if outcome == "success" {
				want = 2
			}
			if calls.Load() != want || clears.Load() != 1 || joins.Load() != 1 {
				t.Fatal("transfer counts", calls.Load(), clears.Load(), joins.Load())
			}
			select {
			case <-a.transferDone:
			default:
				t.Fatal("transfer incomplete")
			}
		})
	}
}
func TestHTTP2LongStreamAndMultiplex(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	l, events := admissionFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Error("not HTTP2")
		}
		if r.URL.Path == "/worker" {
			close(entered)
			w.WriteHeader(200)
			_ = http.NewResponseController(w).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(204)
	}), listenerHooks{})
	t.Cleanup(func() { unblock(); _ = l.Close() })
	tr := h2Transport(t)
	client := &http.Client{Transport: tr, Timeout: 15 * time.Second}
	input, output := io.Pipe()
	defer input.Close()
	defer output.Close()
	response, err := client.Post("http://"+l.server.Addr+"/worker", "application/octet-stream", input)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	<-entered
	a := nextAdmission(t, events, "cleared")
	<-a.timerDone
	timer := time.NewTimer(time.Until(a.deadline) + 50*time.Millisecond)
	defer timer.Stop()
	<-timer.C
	monitor, err := client.Get("http://" + l.server.Addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = monitor.Body.Close()
	if monitor.ProtoMajor != 2 || monitor.StatusCode != 204 {
		t.Fatal("multiplexed monitor", monitor.Status)
	}
	unblock()
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal("long open-upload response", err)
	}
	if response.ProtoMajor != 2 {
		t.Fatal("not negotiated HTTP2")
	}
}
func TestAdmissionFailureAndTimerCloseJoins(t *testing.T) {
	for _, which := range []string{"initial set", "expired callback", "Close before admission", "late headers"} {
		t.Run(which, func(t *testing.T) {
			expiring, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var calls atomic.Int32
			l, events := admissionFixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }), listenerHooks{
				event: func(_ *headerAdmission, event string) {
					if which == "expired callback" && event == "expired" {
						close(expiring)
						<-release
					}
				},
				deadline: func(c net.Conn, d time.Time) error {
					if which == "initial set" {
						return errors.New("injected set failure")
					}
					return c.SetReadDeadline(d)
				},
			})
			t.Cleanup(func() { unblock(); _ = l.Close() })
			conn, err := net.Dial("tcp", l.server.Addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			a := nextAdmission(t, events, "armed")
			if which == "expired callback" {
				select {
				case <-expiring:
				case <-time.After(15 * time.Second):
					t.Fatal("timer did not claim expiry")
				}
			}
			if which == "late headers" {
				a.mu.Lock()
				a.deadline = time.Now().Add(-time.Second)
				a.mu.Unlock()
				if a.admit(true) {
					t.Fatal("late admission allowed")
				}
			}
			done := make(chan struct{})
			go func() {
				var group sync.WaitGroup
				for range 2 {
					group.Add(1)
					go func() { defer group.Done(); _ = l.Close() }()
				}
				group.Wait()
				close(done)
			}()
			if which == "expired callback" {
				select {
				case <-done:
					t.Fatal("Close failed to join running timer")
				default:
				}
			}
			unblock()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Fatal("Close did not join")
			}
			<-a.timerDone
			if a.admit(true) || calls.Load() != 0 {
				t.Fatal("failed admission dispatched")
			}
		})
	}
}

func TestCloseDuringConnectionRegistration(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls atomic.Int32
	l, events := admissionFixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }), listenerHooks{
		event: func(_ *headerAdmission, event string) {
			if event == "accepted" {
				<-release
			}
		},
	})
	t.Cleanup(func() { unblock(); _ = l.Close() })
	conn, err := net.Dial("tcp", l.server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	a := nextAdmission(t, events, "accepted")
	done := make(chan struct{})
	go func() { _ = l.Close(); close(done) }()
	t.Cleanup(func() { unblock(); <-done })
	nextAdmission(t, events, "closed")
	select {
	case <-done:
		t.Fatal("Close missed ConnContext")
	default:
	}
	unblock()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("ConnContext Close did not join")
	}
	<-a.timerDone
	if a.admit(true) || calls.Load() != 0 {
		t.Fatal("closing registration dispatched")
	}
}

func TestHandlerFactoryLifetime(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	for _, bind := range []string{"", occupied.Addr().String(), "127.0.0.1:0"} {
		var child context.Context
		l, err := StartWithHandler(context.Background(), bind, func(ctx context.Context) http.Handler { child = ctx; return http.NotFoundHandler() })
		if bind == occupied.Addr().String() && err == nil {
			t.Fatal("missing bind error")
		}
		if l != nil {
			_ = l.Close()
		}
		select {
		case <-child.Done():
		default:
			t.Fatal("child lifetime not cancelled")
		}
	}
}
