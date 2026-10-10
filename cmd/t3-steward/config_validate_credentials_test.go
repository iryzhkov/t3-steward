//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

// inotify observes real opens/accesses, including read-only credential opens
// permitted by the separate syscall effects guard.
func configCredentialEvents(t *testing.T, fd int) map[int]uint32 {
	t.Helper()
	events := map[int]uint32{}
	buf := make([]byte, 65536)
	for {
		n, err := unix.Read(fd, buf)
		if errors.Is(err, unix.EAGAIN) {
			return events
		}
		if err != nil {
			t.Fatal(err)
		}
		for pos := 0; pos < n; {
			wd := int(int32(binary.LittleEndian.Uint32(buf[pos:])))
			mask := binary.LittleEndian.Uint32(buf[pos+4:])
			size := int(binary.LittleEndian.Uint32(buf[pos+12:]))
			events[wd] |= mask
			pos += unix.SizeofInotifyEvent + size
		}
	}
}

func TestConfigValidateCredentialReadBoundary(t *testing.T) {
	buildBinary := filepath.Join(t.TempDir(), "t3-steward")
	build := exec.Command("go", "build", "-o", buildBinary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	root, path, _ := configValidationFixture(t)
	for _, e := range configValidationEnvironment(root) {
		key, value, _ := strings.Cut(e, "=")
		t.Setenv(key, value)
	}
	fleet := filepath.Join(root, "home", config.CoordinatorFleetPath)
	if err := os.MkdirAll(filepath.Dir(fleet), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fleet, []byte(`{"kind":"steward-coordinator-catalog-input","schema_version":1,"coordinator_id":"normandy","workers":{},"projects":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	credential := filepath.Join(root, "SECRET-webhook")
	valid := []byte("https://example.invalid/SECRET-webhook")
	invalid := []byte("SECRET-invalid")
	if err := os.WriteFile(credential, valid, 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Notifications.Discord = &config.DiscordNotifications{WebhookURLFile: credential}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Below 3 MiB, with enough parse work to mutate while the process runs.
	raw = append(raw, []byte("\n#"+strings.Repeat("x", (3<<20)-len(raw)-2))...)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}

	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	wd, err := unix.InotifyAddWatch(fd, credential, unix.IN_OPEN|unix.IN_ACCESS|unix.IN_CLOSE_NOWRITE)
	if err != nil {
		t.Fatal(err)
	}
	noRead := func() {
		t.Helper()
		if events := configCredentialEvents(t, fd); events[wd]&(unix.IN_OPEN|unix.IN_ACCESS) != 0 {
			t.Fatal("pure validator opened/accessed credential")
		}
	}
	// Actual runtime LoadFile AND Validate still read and enforce valid/invalid
	// private credentials. These also prove the read observer detects the seam.
	for _, value := range [][]byte{valid, invalid} {
		if err := os.WriteFile(credential, value, 0600); err != nil {
			t.Fatal(err)
		}
		configCredentialEvents(t, fd) // discard this test's writer OPEN
		for _, load := range []bool{false, true} {
			var err error
			if load {
				_, err = config.LoadFile(path)
			} else {
				err = cfg.Validate()
			}
			if (err == nil) != bytes.Equal(value, valid) {
				t.Fatal("runtime credential semantics changed")
			}
			events := configCredentialEvents(t, fd)
			if events[wd]&unix.IN_OPEN == 0 || events[wd]&unix.IN_ACCESS == 0 {
				t.Fatal("inotify positive control missed runtime read")
			}
		}
	}
	argv := []string{"config", "validate", "--file", path, "--json"}
	for _, value := range [][]byte{valid, invalid} {
		if err := os.WriteFile(credential, value, 0600); err != nil {
			t.Fatal(err)
		}
		configCredentialEvents(t, fd)
		before := guidanceTree(t, root)
		configCredentialEvents(t, fd) // tree snapshot itself reads fixture files
		code, out, stderr := captureSpoolStd(t, argv)
		assertConfigValidationRefusal(t, code, out, stderr)
		noRead()
		ctx, cancel := context.WithTimeout(context.Background(), testtiming.Bound(10*time.Second))
		cmd := exec.CommandContext(ctx, buildBinary, argv...)
		cmd.Env = configValidationEnvironment(root)
		var stdout, stderrBuf bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderrBuf
		err := cmd.Run()
		cancel()
		exit, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("expected built-binary refusal: %v", err)
		}
		assertConfigValidationRefusal(t, exit.ExitCode(), stdout.String(), stderrBuf.String())
		noRead()
		if !reflect.DeepEqual(before, guidanceTree(t, root)) {
			t.Fatal("credential refusal changed fixture tree")
		}
		configCredentialEvents(t, fd)
	}

	// Reproduce the review's changed-credential fault with inverted expectations.
	// Keep the writer open before observation, so our mutation adds no OPEN.
	if err := os.WriteFile(credential, valid, 0600); err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(credential, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	configCredentialEvents(t, fd)
	stagedWD, err := unix.InotifyAddWatch(fd, path, unix.IN_OPEN)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testtiming.Bound(10*time.Second))
	defer cancel()
	cmd := exec.CommandContext(ctx, buildBinary, argv...)
	cmd.Env = configValidationEnvironment(root)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	mutated := false
	var result error
	var reads uint32
	finished := false
	for !finished {
		events := configCredentialEvents(t, fd)
		reads |= events[wd]
		if !mutated && events[stagedWD]&unix.IN_OPEN != 0 {
			select {
			case result = <-done:
				finished = true
			default:
				if _, err := writer.WriteAt(invalid, 0); err != nil {
					t.Fatal(err)
				}
				if err := writer.Truncate(int64(len(invalid))); err != nil {
					t.Fatal(err)
				}
				mutated = true
			}
		}
		if !finished {
			select {
			case result = <-done:
				finished = true
			case <-time.After(time.Millisecond):
			}
		}
	}
	reads |= configCredentialEvents(t, fd)[wd]
	if !mutated {
		t.Fatal("mutation fault did not run while child was running")
	}
	if reads&(unix.IN_OPEN|unix.IN_ACCESS) != 0 {
		t.Fatal("changed credential was opened/accessed")
	}
	exit, ok := result.(*exec.ExitError)
	if !ok {
		t.Fatalf("changed credential accepted: %v", result)
	}
	assertConfigValidationRefusal(t, exit.ExitCode(), stdout.String(), stderr.String())
	cmd = exec.CommandContext(ctx, buildBinary, argv...)
	cmd.Env = configValidationEnvironment(root)
	stdout.Reset()
	stderr.Reset()
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	result = cmd.Run()
	exit, ok = result.(*exec.ExitError)
	if !ok {
		t.Fatalf("invalid next credential accepted: %v", result)
	}
	assertConfigValidationRefusal(t, exit.ExitCode(), stdout.String(), stderr.String())
	noRead()
	t.Log("runtime valid/invalid OPEN+ACCESS positive controls; invoke/binary no-read refusals; running-child mutation and identical next invocation refused")
}
