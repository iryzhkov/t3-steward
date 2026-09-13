//go:build linux

package t3api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

func containedFixture(t *testing.T) (directoryresource.Identity, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	file, identity, err := directoryresource.Open(directoryresource.Registration{WorkerID: "test", ResourceID: "control", Revision: "1", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	return identity, path
}

func serveContainedFixture(t *testing.T, path string) {
	t.Helper()
	listener, err := net.Listen("unix", filepath.Join(path, "api.sock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(path, "api.sock"), 0600); err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/t3/environment" {
			fmt.Fprint(w, `{"environmentId":"owned"}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer owned-token" {
			w.WriteHeader(401)
			return
		}
		fmt.Fprint(w, `{"authenticated":true}`)
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
}

func TestContainedTransportPinsSocketAndRejectsReplacement(t *testing.T) {
	identity, path := containedFixture(t)
	serveContainedFixture(t, path)
	if err := os.WriteFile(filepath.Join(path, "token"), []byte("owned-token"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	client, err := NewContained(identity, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := client.Descriptor(context.Background())
	if err != nil || descriptor.EnvironmentID != "owned" {
		t.Fatalf("descriptor=%+v err=%v", descriptor, err)
	}
	token, err := client.Tokens.Token(context.Background())
	if err != nil || token != "owned-token" {
		t.Fatalf("token=%q err=%v", token, err)
	}
	// Same pathname now names a different directory. Neither channel may retarget.
	if err := os.Rename(path, path+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Descriptor(context.Background()); err == nil {
		t.Fatal("replaced directory connected")
	}
	if _, err := client.Tokens.Token(context.Background()); err == nil {
		t.Fatal("replaced directory supplied token")
	}
}

func TestPinnedControlSocketSurvivesPathSwap(t *testing.T) {
	identity, path := containedFixture(t)
	serveContainedFixture(t, path)
	directory, err := directoryresource.Reopen(identity, identity.Registration)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	socket, err := pinControlSocket(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	if err := os.Rename(filepath.Join(path, "api.sock"), filepath.Join(path, "original.sock")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/unrelated/host/socket", filepath.Join(path, "api.sock")); err != nil {
		t.Fatal(err)
	}
	// The already-open inode remains usable; a fresh lookup refuses the symlink.
	conn, err := net.DialTimeout("unix", fmt.Sprintf("/proc/self/fd/%d", socket.Fd()), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if f, err := pinControlSocket(directory); err == nil {
		f.Close()
		t.Fatal("socket symlink accepted")
	}
}

func TestContainedTransportRejectsTokenEscape(t *testing.T) {
	identity, path := containedFixture(t)
	outside := filepath.Join(filepath.Dir(path), "outside-token")
	if err := os.WriteFile(outside, []byte("foreign-token"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(path, "token")); err != nil {
		t.Fatal(err)
	}
	client, err := NewContained(identity, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Tokens.Token(context.Background()); err == nil {
		t.Fatal("token escaped control inode")
	}
}
