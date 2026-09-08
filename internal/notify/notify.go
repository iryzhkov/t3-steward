// Package notify sends best-effort desktop notifications.
package notify

import (
	"context"
	"log/slog"
	"os/exec"
	"runtime"
	"time"
)

// Notifier sends desktop notifications when enabled and a backend exists.
type Notifier struct {
	Enabled bool
	Logger  *slog.Logger
}

// Send shows a notification. Failures are logged at debug level only.
func (n Notifier) Send(ctx context.Context, title, body string) {
	if !n.Enabled {
		return
	}
	logger := n.Logger
	if logger == nil {
		logger = slog.Default()
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "linux":
		path, err := exec.LookPath("notify-send")
		if err != nil {
			return
		}
		cmd = exec.CommandContext(cctx, path, "--app-name=t3-steward", title, body)
	case "darwin":
		path, err := exec.LookPath("osascript")
		if err != nil {
			return
		}
		script := "display notification " + quoteAppleScript(body) + " with title " + quoteAppleScript(title)
		cmd = exec.CommandContext(cctx, path, "-e", script)
	default:
		return
	}
	if err := cmd.Run(); err != nil {
		logger.Debug("desktop notification failed", "err", err)
	}
}

func quoteAppleScript(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"', '\\':
			out = append(out, '\\', s[i])
		default:
			out = append(out, s[i])
		}
	}
	return string(append(out, '"'))
}
