package t3api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RuntimeState mirrors userdata/server-runtime.json, which the T3 server
// writes on start and removes on a clean shutdown.
type RuntimeState struct {
	Version   int    `json:"version"`
	PID       int    `json:"pid"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Origin    string `json:"origin"`
	DevURL    string `json:"devUrl"`
	StartedAt string `json:"startedAt"`
}

// UserDataDir returns <dataDir>/userdata.
func UserDataDir(dataDir string) string {
	return filepath.Join(dataDir, "userdata")
}

// ProviderLogDir returns the directory T3 writes provider event logs to.
func ProviderLogDir(dataDir string) string {
	return filepath.Join(UserDataDir(dataDir), "logs", "provider")
}

// RuntimeStatePath returns the server-runtime.json path.
func RuntimeStatePath(dataDir string) string {
	return filepath.Join(UserDataDir(dataDir), "server-runtime.json")
}

// ReadRuntimeState reads server-runtime.json. It returns os.ErrNotExist
// when the server has not written one.
func ReadRuntimeState(dataDir string) (*RuntimeState, error) {
	raw, err := os.ReadFile(RuntimeStatePath(dataDir))
	if err != nil {
		return nil, err
	}
	var st RuntimeState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("decode %s: %w", RuntimeStatePath(dataDir), err)
	}
	if st.Origin == "" {
		return nil, errors.New("server-runtime.json has no origin")
	}
	return &st, nil
}

// DiscoverURL resolves the server base URL: the explicit URL when given,
// otherwise the origin recorded in server-runtime.json.
func DiscoverURL(explicit, dataDir string) (string, error) {
	if strings.TrimSpace(explicit) != "" {
		return strings.TrimRight(strings.TrimSpace(explicit), "/"), nil
	}
	st, err := ReadRuntimeState(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("no T3 server URL configured and %s does not exist (is the T3 server running?)", RuntimeStatePath(dataDir))
		}
		return "", err
	}
	origin := strings.TrimRight(st.Origin, "/")
	// A wildcard bind is recorded as 127.0.0.1 already; keep whatever the
	// server wrote so that a custom host works too.
	return origin, nil
}

// DiscoveryRetry bounds how long AwaitURL waits for the T3 server's runtime
// state. A zero Timeout makes one attempt, as DiscoverURL does.
type DiscoveryRetry struct {
	Timeout time.Duration
	// InitialDelay and MaxDelay shape the backoff between attempts; zero
	// means one second and fifteen seconds.
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// AwaitURL is DiscoverURL for a process that may start before the T3 server
// has written its runtime state, as a service does at boot: t3code is a
// simple unit, so ordering after it does not mean it is listening. The state
// is read again with backoff until it is usable or retry.Timeout has passed;
// an explicit URL needs no state and returns at once. Every read error is
// retried, because a server that is starting may also be half way through
// writing the file. retried, when not nil, is told each failure and the delay
// before the next attempt.
func AwaitURL(ctx context.Context, explicit, dataDir string, retry DiscoveryRetry, retried func(err error, delay time.Duration)) (string, error) {
	delay, maxDelay := retry.InitialDelay, retry.MaxDelay
	if delay <= 0 {
		delay = time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 15 * time.Second
	}
	deadline := time.Now().Add(retry.Timeout)
	for {
		url, err := DiscoverURL(explicit, dataDir)
		if err == nil || retry.Timeout <= 0 {
			return url, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", fmt.Errorf("%w; waited %s", err, retry.Timeout)
		}
		if delay > remaining {
			delay = remaining
		}
		if retried != nil {
			retried(err, delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", fmt.Errorf("%w; stopped waiting: %w", err, ctx.Err())
		case <-timer.C:
		}
		if delay *= 2; delay > maxDelay {
			delay = maxDelay
		}
	}
}
