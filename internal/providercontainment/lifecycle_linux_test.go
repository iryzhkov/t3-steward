//go:build linux

package providercontainment

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

func TestKernelExplicitWriterAndSupervisorCancellation(t *testing.T) {
	if os.Getenv("T3_STEWARD_REQUIRE_CONTAINMENT_TESTS") != "1" {
		t.Skip("requires explicit kernel containment qualification")
	}
	spec := fixture(t)
	spec.Directories[0].Identity.Registration.Writable = true
	spec.Directories[0].Access = directoryresource.ReadWrite
	spec.Cwd = "/data/0"
	spec.Command = []string{"/bin/sh", "-c", "printf changed > source; printf published > /workspace/result"}
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := Run(ctx, spec, Streams{Stdout: &output, Stderr: &output})
	cancel()
	if err != nil {
		t.Fatalf("explicit writer: %v %s", err, output.String())
	}
	raw, err := os.ReadFile(filepath.Join(spec.Directories[0].Identity.Registration.Path, "source"))
	if err != nil || string(raw) != "changed" {
		t.Fatal("explicit direct-cwd write missing")
	}
	raw, err = os.ReadFile(filepath.Join(spec.Workspace.Registration.Path, "result"))
	if err != nil || string(raw) != "published" {
		t.Fatal("owned output missing")
	}

	// Cancellation of the dedicated supervisor must contain descendants; this is
	// explicitly not driven by lease expiry or worker transport loss.
	spec.Command = []string{"/usr/bin/python3", "-c", `import os,time
if os.fork()==0:
    time.sleep(.5)
    open('/workspace/escaped','w').write('bad')
    os._exit(0)
print('ready',flush=True)
time.sleep(10)
`}
	reader, writer := io.Pipe()
	defer reader.Close()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		err := Run(ctx, spec, Streams{Stdout: writer, Stderr: &output})
		_ = writer.Close()
		done <- err
	}()
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil || line != "ready\n" {
		t.Fatalf("child did not become ready: %q %v", line, err)
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled supervisor reported success")
	}
	time.Sleep(650 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(spec.Workspace.Registration.Path, "escaped")); !os.IsNotExist(err) {
		t.Fatal("descendant survived supervisor cancellation")
	}
}
