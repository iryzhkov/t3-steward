// Package coordinatoralerts exposes read-only coordinator monitoring signals.
package coordinatoralerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/resourcetelemetry"
)

const DefaultMinFreeMB int64 = 5120
const storeTimeout = 2 * time.Second

type Store interface {
	SchemaVersion(context.Context) (int, error)
	LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error)
	LoadWorkerEnrollments(context.Context) ([]domain.WorkerEnrollment, error)
}

type Signal struct {
	State      string    `json:"state"`
	Detail     string    `json:"detail"`
	ObservedAt time.Time `json:"observedAt"`
}

type Report struct {
	Schema     string            `json:"schema"`
	ObservedAt time.Time         `json:"observedAt"`
	Signals    map[string]Signal `json:"signals"`
}

type Handler struct {
	Store     Store
	StateDir  string
	MinFreeMB int64
	FreeBytes func(string) (uint64, error)
	Now       func() time.Time
}

func (h Handler) coordinator(ctx context.Context, now time.Time) Signal {
	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()
	signal := Signal{State: "ok", Detail: "store read succeeded", ObservedAt: now}
	if _, err := h.Store.SchemaVersion(ctx); err != nil {
		signal.State, signal.Detail = "firing", "store read failed or timed out"
	}
	return signal
}

func (h Handler) disk(ctx context.Context, now time.Time) Signal {
	threshold := h.MinFreeMB
	if threshold == 0 {
		threshold = DefaultMinFreeMB
	}
	read := h.FreeBytes
	if read == nil {
		read = resourcetelemetry.New().FreeBytes
	}
	count, missing := 0, false
	var minimum int64
	add := func(value *int64) {
		if value == nil || *value < 0 {
			missing = true
			return
		}
		if count == 0 || *value < minimum {
			minimum = *value
		}
		count++
	}
	if h.StateDir == "" {
		missing = true
	} else if bytes, err := read(h.StateDir); err != nil {
		missing = true
	} else {
		mb := int64(bytes / (1024 * 1024))
		add(&mb)
	}
	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()
	enrolled, err := h.Store.LoadWorkerEnrollments(ctx)
	if err != nil {
		missing = true
	}
	snapshots, err := h.Store.LoadWorkerSnapshots(ctx)
	if err != nil {
		missing = true
	}
	latest := make(map[string]domain.WorkerSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		latest[snapshot.WorkerID] = snapshot
	}
	for _, worker := range enrolled {
		snapshot, exists := latest[worker.Request.WorkerID]
		telemetry := snapshot.Inventory.Telemetry
		if !exists || telemetry == nil {
			missing = true
			continue
		}
		add(telemetry.WorkspaceFreeMB)
		add(telemetry.TempFreeMB)
	}
	signal := Signal{State: "ok", ObservedAt: now,
		Detail: fmt.Sprintf("minimum free space %d MiB across %d readings; threshold %d MiB", minimum, count, threshold)}
	if count > 0 && minimum < threshold {
		signal.State = "firing"
	} else if missing || count == 0 {
		signal.State, signal.Detail = "unknown", "disk readings unavailable"
	}
	return signal
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	now := time.Now().UTC()
	if h.Now != nil {
		now = h.Now().UTC()
	}
	var value any
	status := http.StatusOK
	switch {
	case r.URL.Path == "/healthz":
		signal := h.coordinator(r.Context(), now)
		value = signal
		if signal.State != "ok" {
			status = http.StatusServiceUnavailable
		}
	case r.URL.Path == "/alerts" || strings.HasPrefix(r.URL.Path, "/alerts/"):
		name := strings.TrimPrefix(r.URL.Path, "/alerts/")
		if r.URL.Path != "/alerts" && name != "coordinator" && name != "disk" && name != "litestream" && name != "stuck_attempt" && name != "quota" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		signals := map[string]Signal{
			"litestream":    {State: "unavailable", Detail: "Litestream is not deployed (unit 1.16)", ObservedAt: now},
			"stuck_attempt": {State: "unavailable", Detail: "not implemented yet", ObservedAt: now},
			"quota":         {State: "unavailable", Detail: "not implemented yet", ObservedAt: now},
		}
		if r.URL.Path == "/alerts" || name == "coordinator" {
			signals["coordinator"] = h.coordinator(r.Context(), now)
		}
		if r.URL.Path == "/alerts" || name == "disk" {
			signals["disk"] = h.disk(r.Context(), now)
		}
		if r.URL.Path == "/alerts" {
			value = Report{Schema: "steward-alerts/v1", ObservedAt: now, Signals: signals}
		} else {
			signal := signals[name]
			value = signal
			if signal.State == "firing" || signal.State == "unknown" {
				status = http.StatusServiceUnavailable
			}
		}
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(value)
	}
}

// Listener owns acceptance, connection admission, handlers and cancellation work.
type Listener struct {
	server      *http.Server
	done        chan struct{}
	socket      net.Listener
	ctx         context.Context
	cancel      context.CancelFunc
	stopContext func() bool
	parentDone  chan struct{}
	parentOnce  sync.Once
	closeOnce   sync.Once
	mu          sync.Mutex
	closing     bool
	connections map[net.Conn]*headerAdmission
	handlers    sync.WaitGroup
	hooks       listenerHooks
}

// The hooks are private, per listener, and never replace the HTTP implementation.
type listenerHooks struct {
	event    func(*headerAdmission, string)
	deadline func(net.Conn, time.Time) error
}
type admissionKey struct{}
type headerAdmission struct {
	listener             *Listener
	conn                 net.Conn
	acceptedAt, deadline time.Time
	mu                   sync.Mutex
	state                string
	closed               bool
	transferResult       bool
	transferDone         chan struct{}
	timer                *time.Timer
	timerDone            chan struct{}
	timerStop            sync.Once
	connClose            sync.Once
}

func (a *headerAdmission) event(event string) {
	if a.listener.hooks.event != nil {
		a.listener.hooks.event(a, event)
	}
}
func (a *headerAdmission) setDeadline(deadline time.Time) error {
	if set := a.listener.hooks.deadline; set != nil {
		return set(a.conn, deadline)
	}
	return a.conn.SetReadDeadline(deadline)
}
func (a *headerAdmission) finishLocked(success bool) {
	if a.state == "ready" || a.state == "failed" {
		return
	}
	a.transferResult = success
	if success {
		a.state = "ready"
	} else {
		a.state = "failed"
	}
	close(a.transferDone)
}
func (a *headerAdmission) close() {
	a.mu.Lock()
	a.closed = true
	a.finishLocked(false)
	a.mu.Unlock()
	a.connClose.Do(func() { _ = a.conn.Close(); a.event("closed") })
}
func (a *headerAdmission) joinTimer() {
	a.timerStop.Do(func() {
		if a.timer.Stop() {
			close(a.timerDone)
		}
		<-a.timerDone
		a.event("timerjoined")
	})
}
func (a *headerAdmission) admit(h2 bool) (success bool) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return false
	}
	switch a.state {
	case "pending":
		if !time.Now().Before(a.deadline) {
			a.mu.Unlock()
			a.close()
			a.joinTimer()
			return false
		}
		a.state = "transferring"
		a.mu.Unlock()
		// A panic or early failure must release every concurrent first stream.
		defer func() {
			if !success {
				a.close()
			}
		}()
		a.event("admitted")
		a.joinTimer()
		if h2 {
			if err := a.setDeadline(time.Time{}); err != nil {
				log.Print("coordinator HTTP admission: read deadline clear failed")
				a.event("clearfailed")
				return false
			}
			a.event("cleared")
		}
		a.mu.Lock()
		a.finishLocked(!a.closed && a.listener.ctx.Err() == nil)
		success = a.transferResult && !a.closed
		a.mu.Unlock()
		return success
	case "transferring":
		a.mu.Unlock()
		a.event("waiting")
		select {
		case <-a.transferDone:
		case <-a.listener.ctx.Done():
			return false
		}
		a.mu.Lock()
	}
	success = a.state == "ready" && a.transferResult && !a.closed && a.listener.ctx.Err() == nil
	a.mu.Unlock()
	return success
}

// Start preserves the caller-owned handler API.
func Start(ctx context.Context, bind string, handler http.Handler) (*Listener, error) {
	return StartWithHandler(ctx, bind, func(context.Context) http.Handler { return handler })
}

// StartWithHandler gives middleware an independently owned listener lifetime.
// It is cancelled on Close, parent cancellation, disabled bind and bind failure.
func StartWithHandler(ctx context.Context, bind string, factory func(context.Context) http.Handler) (*Listener, error) {
	return startWithHooks(ctx, bind, factory, listenerHooks{})
}
func startWithHooks(parent context.Context, bind string, factory func(context.Context) http.Handler, hooks listenerHooks) (*Listener, error) {
	ctx, cancel := context.WithCancel(parent)
	handler := factory(ctx)
	if bind == "" {
		cancel()
		return nil, nil
	}
	socket, err := net.Listen("tcp", bind)
	if err != nil {
		cancel()
		return nil, err
	}
	l := &Listener{socket: socket, ctx: ctx, cancel: cancel, hooks: hooks,
		done: make(chan struct{}), parentDone: make(chan struct{}), connections: make(map[net.Conn]*headerAdmission)}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	l.server = &http.Server{
		Addr: socket.Addr().String(), ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 16 << 10,
		WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, Protocols: protocols,
		BaseContext: func(net.Listener) context.Context { return ctx },
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			now := time.Now()
			a := &headerAdmission{listener: l, conn: conn, acceptedAt: now, deadline: now.Add(5 * time.Second),
				state: "pending", transferDone: make(chan struct{}), timerDone: make(chan struct{})}
			// Publish the timer before expiry can acquire the record mutex.
			a.mu.Lock()
			a.timer = time.AfterFunc(time.Until(a.deadline), func() {
				defer close(a.timerDone)
				a.mu.Lock()
				expired := a.state == "pending" && !a.closed
				if expired {
					a.closed = true
					a.finishLocked(false)
				}
				a.mu.Unlock()
				if expired {
					a.event("expired")
					a.close()
				}
			})
			a.mu.Unlock()
			l.mu.Lock()
			l.connections[conn] = a
			closing := l.closing
			l.mu.Unlock()
			a.event("accepted")
			if err := a.setDeadline(a.deadline); err != nil {
				log.Print("coordinator HTTP admission: initial read deadline failed")
				a.event("setfailed")
				a.close()
			}
			a.event("armed")
			if closing {
				a.close()
				a.joinTimer()
			}
			return context.WithValue(ctx, admissionKey{}, a)
		},
		ConnState: func(conn net.Conn, state http.ConnState) {
			if state != http.StateClosed {
				return
			}
			l.mu.Lock()
			a := l.connections[conn]
			l.mu.Unlock()
			if a != nil {
				a.close()
				a.joinTimer()
				l.mu.Lock()
				delete(l.connections, conn)
				l.mu.Unlock()
			}
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			l.mu.Lock()
			if l.closing {
				l.mu.Unlock()
				return
			}
			l.handlers.Add(1)
			l.mu.Unlock()
			defer l.handlers.Done()
			a, ok := r.Context().Value(admissionKey{}).(*headerAdmission)
			if ok && a.admit(r.ProtoMajor == 2) {
				handler.ServeHTTP(w, r)
			}
		}),
	}
	l.stopContext = context.AfterFunc(parent, func() {
		defer l.parentOnce.Do(func() { close(l.parentDone) })
		l.beginClose()
	})
	go func() { defer close(l.done); _ = l.server.Serve(socket) }()
	return l, nil
}
func (l *Listener) beginClose() {
	l.closeOnce.Do(func() {
		_ = l.socket.Close()
		l.cancel()
		l.mu.Lock()
		l.closing = true
		connections := make([]*headerAdmission, 0, len(l.connections))
		for _, a := range l.connections {
			connections = append(connections, a)
		}
		l.mu.Unlock()
		for _, a := range connections {
			a.close()
		}
		_ = l.server.Close()
		for _, a := range connections {
			a.joinTimer()
		}
	})
}
func (l *Listener) Close() error {
	if l == nil {
		return nil
	}
	if l.stopContext() {
		l.parentOnce.Do(func() { close(l.parentDone) })
	}
	l.beginClose()
	<-l.done
	l.handlers.Wait()
	<-l.parentDone
	return nil
}
