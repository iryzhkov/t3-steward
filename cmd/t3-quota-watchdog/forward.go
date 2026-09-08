package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/backlog"
	"github.com/iryzhkov/t3-quota-watchdog/internal/config"
)

var hostLine = regexp.MustCompile(`(?m)^host:.*\n`)

// localHostName is the name tasks use for this machine.
func localHostName(cfg config.Config) string {
	if cfg.Backlog.HostName != "" {
		return cfg.Backlog.HostName
	}
	h, _ := os.Hostname()
	return h
}

// rewriteHost sets the host field of a task file to name.
func rewriteHost(raw []byte, name string) []byte {
	line := []byte("host: " + name + "\n")
	if hostLine.Match(raw) {
		return hostLine.ReplaceAll(raw, line)
	}
	if bytes.HasPrefix(raw, []byte("---\n")) {
		return append(append([]byte("---\n"), line...), raw[4:]...)
	}
	return append(append([]byte("---\n"), line...), append([]byte("---\n"), raw...)...)
}

// forwardTask copies a task file into another host's backlog over SSH by
// running `backlog receive` there. The remote binary must be on the login
// shell's PATH.
func forwardTask(ctx context.Context, host string, task backlog.Task) error {
	raw, err := os.ReadFile(task.Path)
	if err != nil {
		return err
	}
	content := rewriteHost(raw, host)
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, "ssh", "-o", "BatchMode=yes", host, "bash", "-lc",
		fmt.Sprintf("'t3-quota-watchdog backlog receive %s'", task.ID))
	cmd.Stdin = bytes.NewReader(content)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if out, err := cmd.Output(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = strings.TrimSpace(string(out))
		}
		return fmt.Errorf("%s: %v: %s", host, err, msg)
	}
	return nil
}

// cmdBacklogReceive stores a task sent by another host, marking it as this
// host's own so it is not forwarded again.
func cmdBacklogReceive(cfg config.Config, dir, id string) error {
	if id == "" || strings.ContainsAny(id, "/\\") || strings.HasPrefix(id, ".") {
		return errors.New("receive needs a plain task id")
	}
	raw, err := readAll(os.Stdin)
	if err != nil {
		return err
	}
	content := rewriteHost(raw, localHostName(cfg))
	if _, err := backlog.Parse(content); err != nil {
		return fmt.Errorf("received task does not parse: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := dir + "/" + id + ".md"
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return err
	}
	fmt.Println(path)
	return nil
}

func readAll(f *os.File) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(f); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// remoteBacklogList prints another host's backlog listing.
func remoteBacklogList(ctx context.Context, host string) error {
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, "ssh", "-o", "BatchMode=yes", host, "bash", "-lc", "'t3-quota-watchdog backlog list'")
	out, err := cmd.CombinedOutput()
	fmt.Printf("== %s\n%s", host, out)
	if err != nil {
		return fmt.Errorf("%s: %v", host, err)
	}
	return nil
}
