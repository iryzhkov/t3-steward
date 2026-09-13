//go:build linux

package providercontainment

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

func inspected(t *testing.T, path, name string, writable bool) directoryresource.Identity {
	t.Helper()
	f, identity, err := directoryresource.Open(directoryresource.Registration{WorkerID: "host", ResourceID: name, Revision: "1", Path: path, Writable: writable})
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	return identity
}

func fixture(t *testing.T) Spec {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"home", "work", "data"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	data := filepath.Join(root, "data")
	if err := os.WriteFile(filepath.Join(data, "source"), []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	identity := inspected(t, data, "data", false)
	binding, err := directoryresource.Bind(identity, "")
	if err != nil {
		t.Fatal(err)
	}
	return Spec{WorkerID: "host", Home: inspected(t, filepath.Join(root, "home"), "home", true), Workspace: inspected(t, filepath.Join(root, "work"), "work", true), Directories: []directoryresource.Binding{binding}, Command: []string{"/bin/true"}}
}

func TestContainmentRejectsReplacementAndWritableAliases(t *testing.T) {
	spec := fixture(t)
	spec.Home = spec.Workspace
	if err := Run(context.Background(), spec, Streams{}); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("overlapping owned storage was not explicitly refused: %v", err)
	}
	spec = fixture(t)
	source := spec.Directories[0].Identity.Registration.Path
	if err := os.Rename(source, source+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), spec, Streams{}); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("replaced source was not explicitly refused: %v", err)
	}
}

func TestKernelContainmentDataChildrenAndNetwork(t *testing.T) {
	if os.Getenv("T3_STEWARD_REQUIRE_CONTAINMENT_TESTS") != "1" {
		t.Skip("set T3_STEWARD_REQUIRE_CONTAINMENT_TESTS=1 on a host with supported bubblewrap/user namespaces")
	}
	spec := fixture(t)
	source := spec.Directories[0].Identity.Registration.Path
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	// The host listener is deliberately not made available in the private network.
	program := `import os, socket, subprocess, sys
assert open('/data/0/source').read() == 'original'
for p in ['/data/0/source', '/data/0/new']:
    try:
        open(p,'w').write('bad')
    except OSError:
        pass
    else:
        raise AssertionError('source write succeeded')
os.symlink('/data/0/source','/workspace/alias')
assert subprocess.run(['/bin/sh','-c','echo bad > /workspace/alias'], stderr=subprocess.DEVNULL).returncode != 0
assert not os.path.exists('/run/dbus/system_bus_socket')
assert not os.path.exists('/home/igor')
assert 'SSH_AUTH_SOCK' not in os.environ
for fd in os.listdir('/proc/self/fd'):
    if int(fd) <= 2:
        continue
    try:
        target = os.readlink('/proc/self/fd/' + fd)
    except FileNotFoundError:
        continue
    raise AssertionError('host mount descriptor leaked: ' + target)
s=socket.socket()
s.settimeout(.2)
try:
    s.connect(('127.0.0.1',int(sys.argv[1])))
except OSError:
    pass
else:
    raise AssertionError('host loopback escaped')
open('/workspace/result','w').write('contained')
print('source-child-network-output passed')
`
	spec.Command = []string{"/usr/bin/python3", "-c", program, strconv.Itoa(port)}
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := Run(ctx, spec, Streams{Stdout: &output, Stderr: &output}); err != nil {
		t.Fatalf("containment failed: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "source-child-network-output passed") {
		t.Fatal(output.String())
	}
	raw, err := os.ReadFile(filepath.Join(source, "source"))
	if err != nil || string(raw) != "original" {
		t.Fatal("source changed")
	}
	raw, err = os.ReadFile(filepath.Join(spec.Workspace.Registration.Path, "result"))
	if err != nil || string(raw) != "contained" {
		t.Fatal("owned output missing")
	}
}
