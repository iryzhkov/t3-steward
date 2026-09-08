package t3api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// CommandToken mints tokens by running a command that prints one on stdout.
// The default command is `t3 auth session issue --token-only --ttl <ttl>`,
// which writes the session straight into T3's own auth database, so no
// long-lived secret is stored by the watchdog.
type CommandToken struct {
	Argv []string
	// TTL is the lifetime to request and the refresh interval. The token is
	// refreshed at 80% of it.
	TTL     time.Duration
	Timeout time.Duration

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// DefaultTokenArgv builds the argv for the t3 CLI.
func DefaultTokenArgv(t3Binary string, ttl time.Duration) []string {
	return []string{t3Binary, "auth", "session", "issue", "--token-only",
		"--ttl", fmt.Sprintf("%dm", int(ttl.Minutes())), "--label", "t3-quota-watchdog"}
}

// Token implements TokenSource.
func (c *CommandToken) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Now().Before(c.expiresAt) {
		return c.token, nil
	}
	if len(c.Argv) == 0 {
		return "", errors.New("token command is empty")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, c.Argv[0], c.Argv[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("token command %q failed: %w: %s", c.Argv[0], err, strings.TrimSpace(stderr.String()))
	}
	// The t3 CLI may print log lines before the token when logging is not
	// silenced; the token is the last non-empty line.
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	token := strings.TrimSpace(lines[len(lines)-1])
	if token == "" {
		return "", fmt.Errorf("token command %q printed nothing", c.Argv[0])
	}
	c.token = token
	ttl := c.TTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	c.expiresAt = time.Now().Add(time.Duration(float64(ttl) * 0.8))
	return token, nil
}

// Invalidate implements TokenSource.
func (c *CommandToken) Invalidate() {
	c.mu.Lock()
	c.token = ""
	c.expiresAt = time.Time{}
	c.mu.Unlock()
}

// FindT3Binary locates the t3 CLI: the explicit path, PATH, then the usual
// per-user install locations of mise and npm.
func FindT3Binary(explicit string) (string, error) {
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("t3 binary %q: %w", explicit, err)
		}
		return explicit, nil
	}
	name := "t3"
	if runtime.GOOS == "windows" {
		name = "t3.cmd"
	}
	if p, err := exec.LookPath(name); err == nil {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", errors.New("t3 CLI not found on PATH")
	}
	candidates := []string{
		filepath.Join(home, ".local", "share", "mise", "shims", "t3"),
		filepath.Join(home, ".local", "share", "mise", "installs", "node", "latest", "bin", "t3"),
		filepath.Join(home, ".local", "bin", "t3"),
		filepath.Join(home, ".npm-global", "bin", "t3"),
		filepath.Join(home, ".volta", "bin", "t3"),
		"/usr/local/bin/t3",
		"/opt/homebrew/bin/t3",
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && !info.IsDir() {
			return c, nil
		}
	}
	return "", errors.New("t3 CLI not found on PATH or in the usual install locations; set t3.t3_binary")
}
