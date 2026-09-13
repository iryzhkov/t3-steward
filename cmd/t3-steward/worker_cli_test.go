package main

import (
	"bytes"
	"context"
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

func TestWorkerCommandsReachLocalCoordinator(t *testing.T) {
	for _, command := range [][]string{
		{"worker", "list", "--json"},
		{"worker", "enroll", "homelab", "--request-id", "enroll-test", "--catalog-revision", strings.Repeat("a", 64), "--expected-revision", "0", "--reason", "qualification"},
	} {
		t.Run(command[1], func(t *testing.T) {
			root := shortTempDir(t)
			statePath := filepath.Join(root, "state.db")
			configPath := filepath.Join(root, "config.yaml")
			if err := os.WriteFile(configPath, []byte("state_path: "+statePath+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: statePath + ".admin.sock", Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			requests := make(chan map[string]any, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					requests <- nil
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				var size uint32
				if binary.Read(conn, binary.BigEndian, &size) != nil || size > 1<<20 {
					requests <- nil
					return
				}
				var request map[string]any
				if json.NewDecoder(io.LimitReader(conn, int64(size))).Decode(&request) != nil {
					requests <- nil
					return
				}
				response, _ := json.Marshal(map[string]string{"version": backlogadmin.LocalTransportVersion, "error": "qualification reached coordinator"})
				_ = binary.Write(conn, binary.BigEndian, uint32(len(response)))
				_, _ = conn.Write(response)
				requests <- request
			}()
			err = run(append(command, "--config", configPath))
			if err == nil || !strings.Contains(err.Error(), "qualification reached coordinator") {
				t.Fatalf("command did not reach coordinator: %v", err)
			}
			request := <-requests
			operation := "query"
			if command[1] == "enroll" {
				operation = "worker-enrollment"
			}
			if request["operation"] != operation {
				t.Fatalf("request: %+v", request)
			}
		})
	}
}

func TestWorkerListUsesCoordinatorQuery(t *testing.T) {
	args := []string{"workers", "--json"}
	if !isCoordinatorAdmin(args) {
		t.Fatal("worker list must use the coordinator admin socket")
	}
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version,
		Kind:    backlogadmin.QueryWorkers,
		Workers: []backlogadmin.Worker{{
			State:    "configured",
			Snapshot: domain.WorkerSnapshot{WorkerID: "homelab"},
		}},
	}}
	var out bytes.Buffer
	cli := backlogAdminCLI{service: fake, stdout: &out, principal: backlogadmin.Principal{ID: "local:1000"}}
	if err := cli.runBacklog(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if len(fake.queries) != 1 || fake.queries[0].Kind != backlogadmin.QueryWorkers {
		t.Fatalf("queries: %+v", fake.queries)
	}
	var response backlogadmin.Response
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Workers) != 1 || response.Workers[0].State != "configured" {
		t.Fatalf("response: %+v", response)
	}
	out.Reset()
	if err := cli.runBacklog(context.Background(), []string{"workers"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte("homelab")) || !bytes.Contains(out.Bytes(), []byte("configured")) {
		t.Fatalf("human output: %s", out.Bytes())
	}
	if _, _, err := parseBacklogAdminQuery([]string{"workers", "unexpected"}); err == nil {
		t.Fatal("unexpected worker-list argument accepted")
	}
}
