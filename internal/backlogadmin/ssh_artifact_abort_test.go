package backlogadmin

import (
	"context"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestSSHArtifactPartialReadCloseAborts(t *testing.T) {
	harness := newRemoteHarness(t, false)
	harness.service.artifact = []byte(strings.Repeat("a", 1<<20))
	harness.client.config.RequestTimeout = 3 * time.Second
	factory := harness.client.config.Factory
	var command *exec.Cmd
	harness.client.config.Factory = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		command = factory(ctx, name, args...)
		return command
	}
	content, err := harness.client.OpenArtifact(context.Background(), Principal{}, "artifact-prefix")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(content.Content, make([]byte, 8<<10)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	closeErr := content.Content.Close()
	elapsed := time.Since(start)
	if elapsed >= time.Second {
		t.Errorf("partial Close took %s; want less than 1 second", elapsed)
	}
	if closeErr != nil {
		t.Errorf("partial Close = %v; want nil", closeErr)
	}
	if command.ProcessState == nil || command.ProcessState.Success() {
		t.Errorf("partial Close did not wait for the cancelled process: %v", command.ProcessState)
	}
}

func TestSSHArtifactFullReadClosePreservesCompletion(t *testing.T) {
	harness := newRemoteHarness(t, false)
	want := strings.Repeat("z", 1<<20)
	harness.service.artifact = []byte(want)
	content, err := harness.client.OpenArtifact(context.Background(), Principal{}, "artifact-full")
	if err != nil {
		t.Fatal(err)
	}
	raw, readErr := io.ReadAll(content.Content)
	closeErr := content.Content.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if closeErr != nil {
		t.Fatalf("full Close = %v", closeErr)
	}
	if int64(len(raw)) != content.Metadata.Size || string(raw) != want {
		t.Fatalf("full read got %d bytes, declared %d; content match %t", len(raw), content.Metadata.Size, string(raw) == want)
	}
}
