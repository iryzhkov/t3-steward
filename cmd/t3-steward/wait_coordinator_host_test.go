package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// recordingNodeWaitClient is a coordinator that answers a registration the way
// a real one does: it records the delivery host the client stated, and its own
// hostname when the client stated none.
type recordingNodeWaitClient struct {
	ops []backlogadmin.NodeWaitOperation
	// own is the coordinator's own hostname, which is what it falls back to.
	own string
}

func (c *recordingNodeWaitClient) NodeWait(_ context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	c.ops = append(c.ops, op)
	host := op.Host
	if host == "" {
		host = c.own
	}
	return backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{{
		Registration: op.Request, Request: op.Request, Host: host,
		CreatedAt: time.Now().UTC(), Deadline: time.Now().UTC().Add(op.Request.Timeout),
		DeliveryID: "node-wake:" + op.Request.ID + ":1", Delivery: "pending",
	}}}, nil
}

// coordinatorWaitHostFixture is a host with a T3 that holds one thread and a
// steward daemon that has just recorded node wake delivery for this host.
func coordinatorWaitHostFixture(t *testing.T) config.Config {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/orchestration/shell" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"threads":[{"id":"thread-1","modelSelection":{"instanceId":"codex","model":"test"},"latestTurn":{"turnId":"turn-1","state":"completed"}}]}`)
	}))
	t.Cleanup(server.Close)
	cfg := config.Default()
	cfg.T3.URL, cfg.T3.Token, cfg.T3.DataDir = server.URL, "test-token", t.TempDir()
	cfg.StatePath = filepath.Join(t.TempDir(), "state.db")
	path, err := nodeWakeDeliveryReceiptPath(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNodeWakeDeliveryReceipt(path, nodeWakeDeliveryReceipt{
		SchemaVersion: nodeWakeDeliveryReceiptSchema, Release: "v0.11.0-rc.75",
		Host: localWakeHost(nil), Interval: "15s", UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// A --node or --quota wait registers for the host that holds the thread, which
// is the calling host and not the coordinator's.
//
// The two registration paths of one command disagreed about this: the
// superseded --run spelling stated the calling host and the documented --node
// spelling did not, so on a host that is not the coordinator the wake was sent
// into the coordinator's T3, where the thread does not exist. Nothing woke the
// thread, and the command told it to end its turn anyway.
func TestCoordinatorKindWaitRegistersForTheCallingHost(t *testing.T) {
	ctx := context.Background()
	cfg := coordinatorWaitHostFixture(t)
	caller := localWakeHost(nil)
	if caller == "" {
		t.Skip("this host cannot name itself")
	}
	for _, kind := range [][]string{
		{"--node", "run-1", "--thread", "thread-1", "--name", "terminal"},
		{"--quota", "claude-main", "--phase", "normal", "--thread", "thread-1", "--name", "quota"},
	} {
		client := &recordingNodeWaitClient{own: "the-coordinator"}
		var err error
		report := captureStderr(t, func() {
			captureStdout(t, func() {
				err = cmdCoordinatorWaitAdd(ctx, cfg, client, func(context.Context) string { return "v0.11.0-rc.75" }, kind)
			})
		})
		if err != nil {
			t.Fatalf("%v: %v", kind, err)
		}
		if len(client.ops) != 1 || client.ops[0].Action != "register" {
			t.Fatalf("%v reached the coordinator as %+v", kind, client.ops)
		}
		if client.ops[0].Host != caller {
			t.Fatalf("%v was registered for host %q, want the calling host %q", kind, client.ops[0].Host, caller)
		}
		if !strings.Contains(report, "End this turn now") || strings.Contains(report, "undeliverable") {
			t.Fatalf("%v reported %q", kind, report)
		}
	}
}

// A coordinator that does not record the calling host is not told one, and the
// registration it answers with is reported as the undeliverable wake it is
// rather than as a wake to end the turn for.
func TestCoordinatorKindWaitReportsAnUndeliverableWake(t *testing.T) {
	ctx := context.Background()
	cfg := coordinatorWaitHostFixture(t)
	client := &recordingNodeWaitClient{own: "the-coordinator"}
	var err error
	report := captureStderr(t, func() {
		captureStdout(t, func() {
			err = cmdCoordinatorWaitAdd(ctx, cfg, client, func(context.Context) string { return "v0.11.0-rc.70" },
				[]string{"--node", "run-1", "--thread", "thread-1"})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(client.ops) != 1 || client.ops[0].Host != "" {
		t.Fatalf("an older coordinator was sent a delivery host: %+v", client.ops)
	}
	for _, want := range []string{"undeliverable", "the-coordinator", "do not end this turn"} {
		if !strings.Contains(report, want) {
			t.Fatalf("the report does not name %q: %q", want, report)
		}
	}
	if strings.Contains(report, "End this turn now") {
		t.Fatalf("an undeliverable wake was reported as one to wait for: %q", report)
	}
}

// The receipt this host's daemon writes is what makes "end this turn" honest,
// so a registration for this host with no receipt says so instead.
func TestCoordinatorKindWaitWithoutADeliveringDaemonSaysSo(t *testing.T) {
	ctx := context.Background()
	cfg := coordinatorWaitHostFixture(t)
	path, err := nodeWakeDeliveryReceiptPath(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	client := &recordingNodeWaitClient{own: "the-coordinator"}
	report := captureStderr(t, func() {
		captureStdout(t, func() {
			if err := cmdCoordinatorWaitAdd(ctx, cfg, client, func(context.Context) string { return "v0.11.0-rc.75" },
				[]string{"--node", "run-1", "--thread", "thread-1"}); err != nil {
				t.Error(err)
			}
		})
	})
	if !strings.Contains(report, "undeliverable") || !strings.Contains(report, "no node wake delivery") {
		t.Fatalf("a registration with no delivering daemon reported %q", report)
	}
}
