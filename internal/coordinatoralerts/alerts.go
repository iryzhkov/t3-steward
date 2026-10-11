// Package coordinatoralerts exposes read-only coordinator monitoring signals.
package coordinatoralerts

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
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

// Listener owns the HTTP server and waits for its serving goroutine on Close.
type Listener struct {
	server      *http.Server
	done        chan struct{}
	stopContext func() bool
}

// Start leaves the listener disabled when bind is empty. Binding failures are
// returned before coordinator readiness is reported.
func Start(ctx context.Context, bind string, handler Handler) (*Listener, error) {
	if bind == "" {
		return nil, nil
	}
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Addr: listener.Addr().String(), Handler: handler, ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 16 << 10,
		WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	result := &Listener{server: server, done: make(chan struct{})}
	result.stopContext = context.AfterFunc(ctx, func() { _ = server.Close() })
	go func() {
		defer close(result.done)
		_ = server.Serve(listener)
	}()
	return result, nil
}

func (l *Listener) Close() error {
	if l == nil {
		return nil
	}
	l.stopContext()
	err := l.server.Close()
	<-l.done
	return err
}
