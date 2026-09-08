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
	t3control "github.com/iryzhkov/t3-quota-watchdog/internal/control/t3"
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
	content := rewriteHost(raw, "local")
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

// cmdBacklogCheck validates a task file (or stdin with "-") against this
// host, or against the task's host when it names another machine: the
// project must exist there, the provider instance must be enabled and
// signed in, the model offered, and the options known. Exit status 1 on
// any failure.
func cmdBacklogCheck(cfg config.Config, source string) error {
	var raw []byte
	var err error
	if source == "-" {
		raw, err = readAll(os.Stdin)
	} else {
		raw, err = os.ReadFile(source)
	}
	if err != nil {
		return err
	}
	task, err := backlog.Parse(raw)
	if err != nil {
		fmt.Printf("fail  %v\n", err)
		return errors.New("task is invalid")
	}
	host := strings.TrimSpace(task.Host)
	if host == "" {
		host = strings.TrimSpace(cfg.Backlog.DefaultHost)
	}
	if host != "" && !backlog.IsLocalHost(host, localHostName(cfg)) {
		// Ask the host that would run it. Its own copy is written as local.
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(cctx, "ssh", "-o", "BatchMode=yes", host, "bash", "-lc", "'t3-quota-watchdog backlog check -'")
		// "local" always means the receiving machine, whatever it calls itself.
		cmd.Stdin = bytes.NewReader(rewriteHost(raw, "local"))
		out, err := cmd.CombinedOutput()
		fmt.Printf("host  %s\n%s", host, out)
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && len(out) > 0 {
				return errors.New("task is invalid on " + host)
			}
			fmt.Printf("fail  host %q: cannot run the watchdog there over SSH (%v)\n", host, err)
			return errors.New("host is not usable")
		}
		return nil
	}
	logger := newLogger("error")
	client, dataDir, err := connect(cfg, logger)
	if err != nil {
		fmt.Printf("fail  T3 on this host: %v\n", err)
		return errors.New("T3 is not reachable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.T3.RequestTimeout.D())
	defer cancel()
	control := t3control.New(client, logger, true)
	seen := map[string]bool{}
	if threads, err := control.ListThreads(ctx); err == nil {
		for _, t := range threads {
			seen[t.ProviderInstanceID] = true
		}
	} else {
		fmt.Printf("fail  T3 on this host: %v\n", err)
		return errors.New("T3 is not reachable")
	}
	result := backlog.Validator{Control: control, DataDir: dataDir, SeenInstances: seen}.Validate(ctx, task)
	for _, f := range result.Findings {
		fmt.Printf("%-5s %s\n", f.Level, f.Message)
	}
	if !result.OK() {
		return errors.New("task is invalid")
	}
	fmt.Printf("ok    task %q would run on %s\n", task.Title, localHostName(cfg))
	return nil
}
