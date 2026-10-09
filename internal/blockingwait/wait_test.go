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

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
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

func TestRunOpenEndedRetriesTransientTransport(t *testing.T) {
	for _, class := range []backlogadmin.TransportClass{backlogadmin.ClassUnavailable, backlogadmin.ClassTimeout} {
		t.Run(string(class), func(t *testing.T) {
			calls := 0
			err := Run(context.Background(), 0, func(context.Context) (bool, error) {
				calls++
				if calls == 1 {
					return false, fmt.Errorf("wrapped: %w", &backlogadmin.TransportError{Class: class, Err: context.DeadlineExceeded})
				}
				return true, nil
			})
			if err != nil || calls != 2 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestRunOpenEndedRetriesUntilInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := Run(ctx, 0, func(context.Context) (bool, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return false, &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable}
	})
	if calls != 2 || !errors.Is(err, context.Canceled) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRunOpenEndedRefusesPermanentTransportErrors(t *testing.T) {
	for _, class := range []backlogadmin.TransportClass{backlogadmin.ClassAuthentication, backlogadmin.ClassClientConfiguration, backlogadmin.ClassProtocol, backlogadmin.ClassRejected} {
		calls := 0
		failure := &backlogadmin.TransportError{Class: class}
		err := Run(context.Background(), 0, func(context.Context) (bool, error) { calls++; return false, failure })
		if calls != 1 || !errors.Is(err, failure) {
			t.Fatalf("class=%s calls=%d err=%v", class, calls, err)
		}
	}
}

func TestLongWaitDelayIsCappedAndJittered(t *testing.T) {
	if maximumDelay != 10*time.Second {
		t.Fatalf("cap=%s", maximumDelay)
	}
	seen := map[time.Duration]bool{}
	for i := 0; i < 100; i++ {
		delay := jitteredDelay(maximumDelay)
		if delay < 8*time.Second || delay > maximumDelay {
			t.Fatalf("delay=%s", delay)
		}
		seen[delay] = true
	}
	if len(seen) < 2 {
		t.Fatal("long waits have no jitter")
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
	ctx, cancel := context.WithTimeout(context.Background(), testtiming.Bound(10*time.Second))
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
