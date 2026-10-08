package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func readHeldResult(t *testing.T, directory *os.File, want string) {
	t.Helper()
	fd, err := unix.Openat(int(directory.Fd()), "final-message.md", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "held final message")
	defer file.Close()
	got, err := io.ReadAll(file)
	if err != nil || string(got) != want {
		t.Fatalf("held generation read %q, err %v, want %q", got, err, want)
	}
}

func TestResultSweepBetweenExchangeAndRetirementKeepsAncientGeneration(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	publishResultFixture(t, directory, "old whole file")
	ancient := time.Now().Add(-2 * interruptedCollectionAge)
	if err := os.Chtimes(directory, ancient, ancient); err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	swept := false
	swapDirectories = func(from, to string) error {
		if err := exchangeDirectories(from, to); err != nil {
			return err
		}
		swept = true
		// A concurrent process's cleanup must defer while publication owns
		// the parent, even though the exchanged directory has ancient mtime.
		if left := removeInterruptedCollections("", directory, time.Now()); len(left) != 0 {
			t.Fatalf("interleaved sweep: %v", left)
		}
		readHeldResult(t, old, "old whole file")
		return nil
	}
	t.Cleanup(func() { swapDirectories = exchangeDirectories })
	publishResultFixture(t, directory, "new whole file")
	if !swept {
		t.Fatal("exchange hook was not exercised")
	}
	if left := removeInterruptedCollections("", directory, time.Now().Add(2*interruptedCollectionAge)); len(left) != 0 {
		t.Fatal(left)
	}
	readHeldResult(t, old, "old whole file")
}

// This subprocess exits at the precise crash boundary, releasing the kernel
// lock without running any publication cleanup. For a delayed publisher it
// first proves the parent is locked, then performs a real blocking publication.
func TestResultPublicationProcessHelper(t *testing.T) {
	directory := os.Getenv("RESULT_TEST_DIRECTORY")
	if directory == "" {
		return
	}
	staged, err := stageResultDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staged, "final-message.md"), []byte("child whole file"), 0o600); err != nil {
		t.Fatal(err)
	}
	mode := os.Getenv("RESULT_TEST_MODE")
	if mode == "publish" {
		probe, err := lockResultParent(directory, false)
		if probe != nil {
			probe.Close()
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			t.Fatalf("publisher was not excluded at exchange boundary: %v", err)
		}
		fmt.Println("blocked")
		if left, err := publishResultDirectory(staged, directory); err != nil || len(left) != 0 {
			t.Fatalf("child publication: %v, %v", left, err)
		}
		return
	}
	lock, err := lockResultParent(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	// Model a preparation older than both grace and interrupted-staging age.
	resultRetirementNow = func() time.Time { return time.Now().Add(-3 * interruptedCollectionAge) }
	prepared, _, err := prepareResultExchange(staged, directory)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "after-exchange" {
		if err := exchangeDirectories(prepared, directory); err != nil {
			t.Fatal(err)
		}
	} else if mode != "before-exchange" {
		t.Fatalf("unknown crash mode %q", mode)
	}
	fmt.Println(prepared)
	os.Exit(0)
}

func resultPublicationChild(t *testing.T, directory, mode string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestResultPublicationProcessHelper$")
	cmd.Env = append(os.Environ(), "RESULT_TEST_DIRECTORY="+directory, "RESULT_TEST_MODE="+mode)
	return cmd
}

func TestResultCrashAfterExchangeUsesBothRetirementGates(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	publishResultFixture(t, directory, "old whole file")
	now := time.Now()
	ancient := now.Add(-2 * interruptedCollectionAge)
	if err := os.Chtimes(directory, ancient, ancient); err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	output, err := resultPublicationChild(t, directory, "after-exchange").CombinedOutput()
	if err != nil {
		t.Fatalf("crash fixture: %v: %s", err, output)
	}
	protected := strings.TrimSpace(string(output))
	if unpublished, err := unpublishedResult(protected); err != nil || unpublished {
		t.Fatalf("exchanged generation classified as staging: %v, %v", unpublished, err)
	}
	// Recovery starts a fresh age; expire that age in a second sweep to
	// prove that neither age threshold can bypass newest K.
	if left := removeInterruptedCollections("", directory, now.Add(2*interruptedCollectionAge)); len(left) != 0 {
		t.Fatal(left)
	}
	if left := removeInterruptedCollections("", directory, now.Add(3*interruptedCollectionAge)); len(left) != 0 {
		t.Fatal(left)
	}
	readHeldResult(t, old, "old whole file")
	// Enough genuinely later publications make this retirement eligible.
	for n := 0; n < resultRetainedGenerations; n++ {
		publishResultFixture(t, directory, "later whole file")
	}
	if left := removeInterruptedCollections("", directory, now.Add(4*interruptedCollectionAge)); len(left) != 0 {
		t.Fatal(left)
	}
	if _, err := os.Stat(resultAgeName(protected, now.Add(2*interruptedCollectionAge))); !os.IsNotExist(err) {
		t.Fatalf("eligible crash retirement survived: %v", err)
	}
}

func TestResultCrashBeforeExchangeDoesNotDisplaceNewestRetirements(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	now := time.Now()
	var newest string
	for n := 0; n < resultRetainedGenerations; n++ {
		path := retiredResultFixture(t, directory, now.Add(-2*resultRetirementGrace+time.Duration(n)), "retired whole file")
		if n == 0 {
			newest = path
		}
	}
	output, err := resultPublicationChild(t, directory, "before-exchange").CombinedOutput()
	if err != nil {
		t.Fatalf("crash fixture: %v: %s", err, output)
	}
	prepared := strings.TrimSpace(string(output))
	if unpublished, err := unpublishedResult(prepared); err != nil || !unpublished {
		t.Fatalf("unpublished preparation misclassified: %v, %v", unpublished, err)
	}
	for _, sweepTime := range []time.Time{now.Add(2 * resultRetirementGrace), now.Add(2 * interruptedCollectionAge)} {
		if left := removeInterruptedCollections("", directory, sweepTime); len(left) != 0 {
			t.Fatal(left)
		}
		if got := readFile(t, newest, "final-message.md"); got != "retired whole file" {
			t.Fatalf("unpublished reservation displaced newest K: %q", got)
		}
	}
	if _, err := os.Stat(prepared); !os.IsNotExist(err) {
		t.Fatalf("abandoned unpublished preparation survived: %v", err)
	}
}

func TestResultDelayedPublishersCannotReorderRetirementsAcrossProcesses(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	publishResultFixture(t, directory, "first whole file")
	var children []*exec.Cmd
	var stderr []*bytes.Buffer
	t.Cleanup(func() {
		for _, cmd := range children {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
	})
	swapDirectories = func(from, to string) error {
		if err := exchangeDirectories(from, to); err != nil {
			return err
		}
		// Pause this publisher after exchange. K other processes attempt to
		// publish; each must confirm exclusion before this one can retire.
		for n := 0; n < resultRetainedGenerations; n++ {
			cmd := resultPublicationChild(t, directory, "publish")
			var diagnostic bytes.Buffer
			cmd.Stderr = &diagnostic
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			children = append(children, cmd)
			stderr = append(stderr, &diagnostic)
			line, err := bufio.NewReader(stdout).ReadString('\n')
			if err != nil || line != "blocked\n" {
				t.Fatalf("publisher entered before retirement: %q, %v", line, err)
			}
		}
		return nil
	}
	t.Cleanup(func() { swapDirectories = exchangeDirectories })
	publishResultFixture(t, directory, "paused whole file")
	swapDirectories = exchangeDirectories
	for n, cmd := range children {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("delayed child: %v: %s", err, stderr[n])
		}
	}
	// All K preceding exchanges finish BEFORE this reader starts. Only the
	// next publication may count against its lifetime, irrespective of delays.
	old, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	publishResultFixture(t, directory, "latest whole file")
	if left := removeInterruptedCollections("", directory, time.Now().Add(2*interruptedCollectionAge)); len(left) != 0 {
		t.Fatal(left)
	}
	readHeldResult(t, old, "child whole file")
	assertResultRetentionAfterGrace(t, directory, resultRetainedGenerations)
}

func TestResultPublicationOrderSurvivesClockRollback(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	publishResultFixture(t, directory, "old whole file")
	future := time.Now().Add(time.Hour)
	retiredResultFixture(t, directory, future, "earlier retirement")
	publishResultFixture(t, directory, "new whole file")
	entries, err := os.ReadDir(filepath.Dir(directory))
	if err != nil {
		t.Fatal(err)
	}
	var newest string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), retiredResultPrefix(directory)) && entry.Name() > newest {
			newest = entry.Name()
		}
	}
	if got := readFile(t, filepath.Dir(directory), newest, "final-message.md"); got != "old whole file" {
		t.Fatalf("wall-clock rollback reordered publication: %q", got)
	}
}
