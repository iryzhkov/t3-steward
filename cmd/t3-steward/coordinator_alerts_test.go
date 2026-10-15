package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCoordinatorAlertsDisabledWithoutBind(t *testing.T) {
	t.Parallel()
	cfg := config.Default()
	cfg.StatePath = ":memory:"
	listener, err := startCoordinatorAlerts(context.Background(), cfg, nil)
	if err != nil || listener != nil {
		t.Fatalf("disabled listener: %v %v", listener, err)
	}
	cfg.BacklogV2.Coordinator.Alerts.Bind = "127.0.0.1:0"
	if _, err := startCoordinatorAlerts(context.Background(), cfg, nil); err == nil {
		t.Fatal("enabled listener needs state directory")
	}
}

func TestCoordinatorHTTPExchangeSharesMonitoringListener(t *testing.T) {
	cfg := config.Default()
	cfg.StatePath = filepath.Join(t.TempDir(), "state.db")
	bind, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.BacklogV2.Coordinator.Alerts.Bind = bind.Addr().String()
	bind.Close()
	cfg.BacklogV2.Coordinator.WorkerHTTPEnabled = true
	cfg.BacklogV2.Transport.RequestTimeout = config.Duration(10 * time.Second)
	cfg.BacklogV2.MessageLimits.MaxBytes = 1024
	cfg.BacklogV2.MessageLimits.MaxArtifactBytes = 1024
	cfg.BacklogV2.Workers = map[string]config.V2Worker{
		"http": {ExchangeTransport: "http", Epoch: "epoch"},
		"ssh":  {ExchangeTransport: "ssh", Epoch: "epoch"},
	}
	entered := make(chan workerproto.ExchangePreface, 1)
	release, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := startCoordinatorAlerts(context.Background(), cfg, nil, func(ctx context.Context, p workerproto.ExchangePreface, _ workerproto.StreamDialer) error {
		entered <- p
		select {
		case <-release.Done():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	base := "http://" + cfg.BacklogV2.Coordinator.Alerts.Bind
	post := func(id, epoch string) *http.Response {
		t.Helper()
		raw, _ := json.Marshal(workerproto.ExchangePreface{Version: workerproto.Version, WorkerID: id, WorkerEpoch: epoch})
		var body bytes.Buffer
		if err := (workerproto.FrameCodec{MaxBytes: 1024}).Write(&body, raw); err != nil {
			t.Fatal(err)
		}
		var upload io.Reader = &body
		if id == "http" && epoch == "epoch" {
			input, output := io.Pipe()
			copied := make(chan struct{})
			go func() { defer close(copied); _, _ = io.Copy(output, &body) }()
			t.Cleanup(func() { _ = output.Close(); _ = input.Close(); <-copied })
			upload = input // Keep upload open through successful response completion.
		}
		response, err := client.Post(base+"/worker/exchange", "application/octet-stream", upload)
		if err != nil {
			t.Fatal(err)
		}
		if response.ProtoMajor != 2 {
			t.Fatal("actual listener did not negotiate H2")
		}
		return response
	}
	for _, route := range [][2]string{{"ssh", "epoch"}, {"http", "stale"}} {
		response := post(route[0], route[1])
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest || len(entered) != 0 {
			t.Fatal("nonselected or stale route dispatched")
		}
	}
	response := post("http", "epoch")
	defer response.Body.Close()
	select {
	case p := <-entered:
		if p.WorkerID != "http" || p.WorkerEpoch != "epoch" || response.StatusCode != http.StatusOK {
			t.Fatal("selected route not dispatched")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not start")
	}
	// The callback has entered after first headers: its next five seconds necessarily
	// cross the initial accepted-at deadline. This waits for the behavior under test.
	initialBound := time.NewTimer(5 * time.Second)
	defer initialBound.Stop()
	<-initialBound.C
	monitoring, err := client.Get(base + "/alerts/quota")
	if err != nil {
		t.Fatal(err)
	}
	monitoring.Body.Close()
	if monitoring.StatusCode != http.StatusOK {
		t.Fatal("monitoring blocked by exchange")
	}
	h1 := &http.Client{Timeout: 5 * time.Second}
	health, err := h1.Get(base + "/alerts/quota")
	if err != nil {
		t.Fatal(err)
	}
	_ = health.Body.Close()
	if health.ProtoMajor != 1 || health.StatusCode != http.StatusOK {
		t.Fatal("H1 monitoring changed")
	}
	cancel()
	if _, err := io.ReadAll(workerproto.ExchangeResponseReader{Response: response}); err != nil {
		t.Fatal(err)
	}
}
