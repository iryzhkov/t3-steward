package providercontainment

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/t3api"
)

func TestScopedControlUsesOnlyItsSocketAndRefusesReplacement(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "t3-control-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	upstream, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "http://127.0.0.1:9/escape", http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, `{"environmentId":"execution-only"}`)
	})}
	defer server.Close()
	go func() { _ = server.Serve(upstream) }()
	port := upstream.Addr().(*net.TCPAddr).Port
	socket := filepath.Join(root, "api.sock")
	listener, err := ListenControl(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if _, err := ListenControl(socket); err == nil {
		t.Fatal("existing socket replaced")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ControlBridge(ctx, listener, port) }()
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:9")
	client, err := t3api.NewUnix(socket, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Descriptor(ctx)
	if err != nil || got.EnvironmentID != "execution-only" {
		t.Fatalf("descriptor: %+v %v", got, err)
	}
	reply, err := client.HTTP.Get(client.BaseURL + "/redirect")
	if err != nil {
		t.Fatal(err)
	}
	_ = reply.Body.Close()
	if reply.StatusCode != http.StatusFound {
		t.Fatalf("redirect followed: %d", reply.StatusCode)
	}
	// An arbitrary URL authority still cannot make this transport dial TCP.
	reply, err = client.HTTP.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = reply.Body.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("bridge did not stop")
	}
	if _, err = client.Descriptor(context.Background()); err == nil {
		t.Fatal("closed socket remained reachable")
	}
}
