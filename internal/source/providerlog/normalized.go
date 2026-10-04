package providerlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// normalizedWindow mirrors T3 0.0.45's ServerProviderUsageWindow.
// Pointers distinguish absent/null required values from real zero readings.
type normalizedWindow struct {
	ID                 *string         `json:"id"`
	Kind               *string         `json:"kind"`
	Label              *string         `json:"label"`
	UsedPercent        *float64        `json:"usedPercent"`
	ResetsAt           json.RawMessage `json:"resetsAt"`
	WindowDurationMins json.RawMessage `json:"windowDurationMins"`
}

func decodeNormalized(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// normalizeWindows preserves sparse updates and window identity. Kind labels
// a window; it never supplies a missing duration. Unsupported populated limits
// are errors rather than silently discarded quota.
func normalizeWindows(raw json.RawMessage, provider string, base domain.QuotaSnapshot) ([]domain.QuotaSnapshot, error) {
	limitID := ""
	switch provider {
	case "codex":
		limitID = "codex"
	case "claudeAgent":
		limitID = "claude"
	default:
		return nil, fmt.Errorf("unsupported normalized quota provider %q", provider)
	}
	var update struct {
		Windows json.RawMessage `json:"windows"`
	}
	if err := decodeNormalized(raw, &update); err != nil {
		return nil, fmt.Errorf("decode normalized limits: %w", err)
	}
	if len(update.Windows) == 0 || bytes.Equal(bytes.TrimSpace(update.Windows), []byte("null")) {
		return nil, errors.New("normalized limits have no windows array")
	}
	var windows []normalizedWindow
	if err := decodeNormalized(update.Windows, &windows); err != nil {
		return nil, fmt.Errorf("decode normalized windows: %w", err)
	}
	seen := make(map[string]bool)
	out := make([]domain.QuotaSnapshot, 0, len(windows))
	for i, w := range windows {
		if w.ID == nil || strings.TrimSpace(*w.ID) == "" || strings.TrimSpace(*w.ID) != *w.ID ||
			w.Label == nil || strings.TrimSpace(*w.Label) == "" || strings.TrimSpace(*w.Label) != *w.Label ||
			w.Kind == nil || w.UsedPercent == nil || math.IsNaN(*w.UsedPercent) || math.IsInf(*w.UsedPercent, 0) || *w.UsedPercent < 0 || *w.UsedPercent > 100 {
			return nil, fmt.Errorf("invalid normalized window at index %d", i)
		}
		switch *w.Kind {
		case "session", "weekly", "monthly", "other":
		default:
			return nil, fmt.Errorf("unsupported normalized window kind at index %d", i)
		}
		if seen[*w.ID] {
			return nil, fmt.Errorf("duplicate normalized window at index %d", i)
		}
		seen[*w.ID] = true
		s := base
		s.Key.LimitID = limitID
		s.Key.Window = *w.ID
		s.LimitName = *w.Label
		s.UsedPercent = *w.UsedPercent
		if provider == "claudeAgent" {
			s.ModelSelector = claudeModelSelector(*w.ID)
		}
		if len(w.ResetsAt) > 0 {
			var value string
			if bytes.Equal(bytes.TrimSpace(w.ResetsAt), []byte("null")) || json.Unmarshal(w.ResetsAt, &value) != nil {
				return nil, fmt.Errorf("invalid normalized reset at index %d", i)
			}
			reset, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return nil, fmt.Errorf("invalid normalized reset at index %d", i)
			}
			reset = reset.UTC()
			s.ResetsAt = &reset
		}
		if len(w.WindowDurationMins) > 0 {
			var mins int64
			if bytes.Equal(bytes.TrimSpace(w.WindowDurationMins), []byte("null")) || json.Unmarshal(w.WindowDurationMins, &mins) != nil || mins < 0 || mins > math.MaxInt64/int64(time.Minute) {
				return nil, fmt.Errorf("invalid normalized duration at index %d", i)
			}
			s.WindowDuration = time.Duration(mins) * time.Minute
		}
		out = append(out, s)
	}
	return out, nil
}
