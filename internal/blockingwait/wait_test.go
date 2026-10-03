package blockingwait

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	for _, args := range [][]string{
		{"--timeout", "1s"}, {"--wait", "--timeout", "0s"}, {"--wait", "--timeout", "-1s"},
		{"--wait", "--timeout", "bad"}, {"--wait", "--timeout"}, {"--wait", "--wait"},
		{"--wait", "--timeout", "1s", "--timeout=2s"},
	} {
		if _, _, err := Parse(args); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
	clean, opts, err := Parse([]string{"run", "--timeout=2s", "--json", "--wait"})
	if err != nil || !opts.Enabled || opts.Timeout != 2*time.Second || len(clean) != 2 || clean[0] != "run" || clean[1] != "--json" {
		t.Fatalf("clean=%v opts=%v err=%v", clean, opts, err)
	}
}

func TestRunDeadlineCoversQuery(t *testing.T) {
	err := Run(context.Background(), time.Millisecond, func(ctx context.Context) (bool, error) {
		<-ctx.Done()
		return false, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
}

func TestRunCancellationAndReattach(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := Run(ctx, 0, func(context.Context) (bool, error) { calls++; cancel(); return false, nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if err := Run(context.Background(), 0, func(context.Context) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
}

func TestPollBackoffAndError(t *testing.T) {
	var times []time.Time
	err := poll(context.Background(), time.Millisecond, 2*time.Millisecond, func(context.Context) (bool, error) {
		times = append(times, time.Now())
		return len(times) == 4, nil
	})
	if err != nil || len(times) != 4 {
		t.Fatalf("err=%v times=%v", err, times)
	}
	// The first delay is 1ms, then capped at 2ms. Timers may run late.
	for i := 1; i < len(times); i++ {
		minimum := time.Millisecond
		if i > 1 {
			minimum = 2 * time.Millisecond
		}
		if times[i].Sub(times[i-1]) < minimum {
			t.Fatalf("delay %d was %s", i, times[i].Sub(times[i-1]))
		}
	}
	sentinel := errors.New("disconnected")
	calls := 0
	err = Run(context.Background(), 0, func(context.Context) (bool, error) { calls++; return false, sentinel })
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRunSIGINT(t *testing.T) {
	if os.Getenv("STEWARD_WAIT_SIGINT_HELPER") == "1" {
		err := Run(context.Background(), 0, func(context.Context) (bool, error) { fmt.Println("ready"); return false, nil })
		if !errors.Is(err, context.Canceled) {
			os.Exit(2)
		}
		os.Exit(0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRunSIGINT$")
	cmd.Env = append(os.Environ(), "STEWARD_WAIT_SIGINT_HELPER=1")
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	scanner := bufio.NewScanner(pipe)
	if !scanner.Scan() || scanner.Text() != "ready" {
		t.Fatal("child never entered wait")
	}
	if err = cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err != nil {
		t.Fatalf("SIGINT waiter did not stop cleanly: %v", err)
	}
}
