package daemon

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// messageFields are available to the warn and drain templates.
type messageFields struct {
	LimitName   string
	UsedPercent string
	ResetsAt    string
	GracePeriod string
	Window      string
	Provider    string
}

func renderMessage(tmpl string, snap domain.QuotaSnapshot, grace time.Duration, now time.Time) (string, error) {
	t, err := template.New("message").Option("missingkey=error").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("parse message template: %w", err)
	}
	fields := messageFields{
		LimitName:   snap.LimitName,
		UsedPercent: fmt.Sprintf("%.0f", snap.UsedPercent),
		ResetsAt:    describeReset(snap.ResetsAt, now),
		GracePeriod: grace.Round(time.Second).String(),
		Window:      snap.Key.Window,
		Provider:    snap.Key.ProviderInstanceID,
	}
	if fields.LimitName == "" {
		fields.LimitName = snap.Key.String()
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, fields); err != nil {
		return "", fmt.Errorf("render message template: %w", err)
	}
	return strings.TrimSpace(buf.String()), nil
}

// describeReset renders a reset time with a relative hint, or a neutral
// phrase when the provider reported none.
func describeReset(resetsAt *time.Time, now time.Time) string {
	if resetsAt == nil {
		return "an unknown time (the provider reported no reset time)"
	}
	local := resetsAt.In(now.Location())
	remaining := resetsAt.Sub(now).Round(time.Minute)
	if remaining <= 0 {
		return local.Format("2006-01-02 15:04 MST") + " (already passed)"
	}
	return fmt.Sprintf("%s (in %s)", local.Format("2006-01-02 15:04 MST"), humanDuration(remaining))
}

func humanDuration(d time.Duration) string {
	if d < time.Minute {
		return "under a minute"
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	switch {
	case h == 0:
		return fmt.Sprintf("%dm", m)
	case h < 48:
		return fmt.Sprintf("%dh %dm", h, m)
	default:
		return fmt.Sprintf("%dd %dh", h/24, h%24)
	}
}
