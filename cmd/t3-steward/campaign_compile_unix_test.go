//go:build unix

package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// A FIFO named as the plan is refused rather than opened: opening it for
// reading would block until a writer appeared, hanging an unattended compile.
func TestCampaignCompileRefusesAFIFOPlanWithoutBlocking(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "plan.md")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		var stdout bytes.Buffer
		done <- campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", fifo, "--out", filepath.Join(t.TempDir(), "wave")})
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(testtiming.Bound(10 * time.Second)):
		t.Fatal("compile blocked opening a FIFO plan")
	}
}
