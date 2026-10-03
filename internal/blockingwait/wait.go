// Package blockingwait implements read-only CLI waits. It owns the sole polling
// loop, bounded backoff, timeout and interrupt handling; it never cancels work.
package blockingwait

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// Options defaults to waiting indefinitely. An explicit timeout must be positive.
type Options struct {
	Enabled bool
	Timeout time.Duration
}

// Parse removes wait flags while preserving every other argument.
func Parse(args []string) ([]string, Options, error) {
	var opts Options
	var clean []string
	timeoutSet := false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--wait":
			if opts.Enabled {
				return nil, opts, errors.New("--wait may only be specified once")
			}
			opts.Enabled = true
		case args[i] == "--timeout" || strings.HasPrefix(args[i], "--timeout="):
			if timeoutSet {
				return nil, opts, errors.New("--timeout may only be specified once")
			}
			timeoutSet = true
			value, equals := strings.CutPrefix(args[i], "--timeout=")
			if !equals {
				i++
				if i == len(args) {
					return nil, opts, errors.New("--timeout needs a positive duration")
				}
				value = args[i]
			}
			duration, err := time.ParseDuration(value)
			if err != nil || duration <= 0 {
				return nil, opts, fmt.Errorf("--timeout needs a positive duration, got %q", value)
			}
			opts.Timeout = duration
		default:
			clean = append(clean, args[i])
		}
	}
	if timeoutSet && !opts.Enabled {
		return nil, opts, errors.New("--timeout requires --wait")
	}
	return clean, opts, nil
}

const (
	initialDelay = 100 * time.Millisecond
	maximumDelay = 10 * time.Second
)

// Run probes immediately, then backs off from 100ms to 10s with jitter.
// Open-ended waits retry unavailable and transport timeout errors; other query
// errors end the wait, allowing read-only reattachment to the same ID.
// The timeout covers queries as well as delays. SIGINT and caller cancellation
// stop only this waiter. The last successful query remains owned by the caller.
func Run(ctx context.Context, timeout time.Duration, probe func(context.Context) (bool, error)) error {
	if timeout < 0 {
		return errors.New("wait timeout cannot be negative")
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if timeout == 0 {
		query := probe
		probe = func(ctx context.Context) (bool, error) {
			done, err := query(ctx)
			switch backlogadmin.ClassOf(err) {
			case backlogadmin.ClassUnavailable, backlogadmin.ClassTimeout:
				return false, nil
			default:
				return done, err
			}
		}
	}
	return poll(ctx, initialDelay, maximumDelay, probe)
}

// Keep the early probes fast and spread long waits over 80–100% of the
// capped delay, including at the cap so concurrent clients do not synchronize.
func jitteredDelay(delay time.Duration) time.Duration {
	if delay < time.Second {
		return delay
	}
	return delay - time.Duration(rand.Int64N(int64(delay/5)+1))
}

func poll(ctx context.Context, delay, maximum time.Duration, probe func(context.Context) (bool, error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := probe(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		timer := time.NewTimer(jitteredDelay(delay))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < maximum {
			delay *= 2
			if delay > maximum {
				delay = maximum
			}
		}
	}
}
