// Package providerlog reads T3's provider event logs and turns
// account.rate-limits.updated records into quota snapshots.
//
// T3 writes one NDJSON-style file per thread under
// <data_dir>/userdata/logs/provider/events.<thread>.log with lines of the form
//
//	[2026-09-08T16:45:14.478Z] CANON: {"type":"account.rate-limits.updated",...}
//
// Files rotate by rename (events.<thread>.log.1 ... .10). Only CANON lines
// of that one event type are parsed; everything else is skipped without
// being decoded.
package providerlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// EventType is the canonical runtime event the watchdog consumes.
const EventType = "account.rate-limits.updated"

const canonMarker = "] CANON: "

// ErrNotRateLimit marks a line that is well-formed but not a rate-limit event.
var ErrNotRateLimit = errors.New("not a rate-limit event")

// record is the subset of a canonical runtime event the watchdog reads.
type record struct {
	Type               string          `json:"type"`
	EventID            string          `json:"eventId"`
	Provider           string          `json:"provider"`
	ProviderInstanceID string          `json:"providerInstanceId"`
	ThreadID           string          `json:"threadId"`
	CreatedAt          string          `json:"createdAt"`
	Payload            json.RawMessage `json:"payload"`
}

type payload struct {
	RateLimits json.RawMessage `json:"rateLimits"`
}

// ParseLine parses one log line. It returns ErrNotRateLimit for lines of
// other kinds so that callers can skip them cheaply.
func ParseLine(line string) ([]domain.QuotaSnapshot, error) {
	if !strings.Contains(line, EventType) {
		return nil, ErrNotRateLimit
	}
	idx := strings.Index(line, canonMarker)
	if idx < 0 || !strings.HasPrefix(line, "[") {
		return nil, ErrNotRateLimit
	}
	body := line[idx+len(canonMarker):]
	observedAt, _ := time.Parse(time.RFC3339Nano, line[1:idx])
	return ParseJSON([]byte(body), observedAt)
}

// ParseJSON parses one canonical event body. fallbackObservedAt is used when
// the record carries no createdAt.
func ParseJSON(body []byte, fallbackObservedAt time.Time) ([]domain.QuotaSnapshot, error) {
	var rec record
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	if rec.Type != EventType {
		return nil, ErrNotRateLimit
	}
	var p payload
	if err := json.Unmarshal(rec.Payload, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	observedAt := fallbackObservedAt
	if rec.CreatedAt != "" {
		if t, err := time.Parse(time.RFC3339Nano, rec.CreatedAt); err == nil {
			observedAt = t
		}
	}
	if observedAt.IsZero() {
		return nil, errors.New("event has no timestamp")
	}
	instance := rec.ProviderInstanceID
	if instance == "" {
		instance = rec.Provider
	}
	if instance == "" {
		return nil, errors.New("event has no provider instance")
	}
	base := domain.QuotaSnapshot{
		Key:           domain.BucketKey{ProviderInstanceID: instance},
		ObservedAt:    observedAt,
		SourceEventID: rec.EventID,
		ThreadID:      rec.ThreadID,
	}
	var snaps []domain.QuotaSnapshot
	var err error
	switch {
	case looksLikeClaude(p.RateLimits):
		snaps, err = normalizeClaude(p.RateLimits, base)
	case looksLikeCodex(p.RateLimits):
		snaps, err = normalizeCodex(p.RateLimits, base)
	default:
		return nil, fmt.Errorf("unrecognized rate-limit payload from provider %q", rec.Provider)
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Key.Window < snaps[j].Key.Window })
	return snaps, nil
}

func looksLikeClaude(raw json.RawMessage) bool {
	var probe struct {
		RateLimitInfo json.RawMessage `json:"rate_limit_info"`
	}
	return json.Unmarshal(raw, &probe) == nil && len(probe.RateLimitInfo) > 0
}

func looksLikeCodex(raw json.RawMessage) bool {
	var probe struct {
		RateLimits json.RawMessage `json:"rateLimits"`
		Primary    json.RawMessage `json:"primary"`
		Secondary  json.RawMessage `json:"secondary"`
		LimitID    json.RawMessage `json:"limitId"`
	}
	if json.Unmarshal(raw, &probe) != nil {
		return false
	}
	return len(probe.RateLimits) > 0 || len(probe.Primary) > 0 || len(probe.Secondary) > 0 || len(probe.LimitID) > 0
}

// --- Codex ---------------------------------------------------------------

type codexWindow struct {
	ResetsAt           *int64   `json:"resetsAt"`
	UsedPercent        *float64 `json:"usedPercent"`
	WindowDurationMins *float64 `json:"windowDurationMins"`
}

type codexSpendControl struct {
	Limit            string   `json:"limit"`
	RemainingPercent *float64 `json:"remainingPercent"`
	ResetsAt         *int64   `json:"resetsAt"`
	Used             string   `json:"used"`
}

type codexSnapshot struct {
	LimitID              *string            `json:"limitId"`
	LimitName            *string            `json:"limitName"`
	PlanType             *string            `json:"planType"`
	Primary              *codexWindow       `json:"primary"`
	Secondary            *codexWindow       `json:"secondary"`
	IndividualLimit      *codexSpendControl `json:"individualLimit"`
	RateLimitReachedType *string            `json:"rateLimitReachedType"`
	SpendControlReached  *bool              `json:"spendControlReached"`
}

// normalizeCodex handles the app-server notification, which T3 forwards
// verbatim. The notification is an envelope {rateLimits: snapshot}, and T3
// wraps it once more, so the snapshot sits at payload.rateLimits.rateLimits.
// Every field is optional: the app-server sends sparse updates, and an
// absent window simply produces no snapshot for that bucket.
func normalizeCodex(raw json.RawMessage, base domain.QuotaSnapshot) ([]domain.QuotaSnapshot, error) {
	var envelope struct {
		RateLimits json.RawMessage `json:"rateLimits"`
	}
	inner := raw
	if err := json.Unmarshal(raw, &envelope); err == nil && len(envelope.RateLimits) > 0 {
		inner = envelope.RateLimits
	}
	var snap codexSnapshot
	if err := json.Unmarshal(inner, &snap); err != nil {
		return nil, fmt.Errorf("decode codex rate limits: %w", err)
	}
	limitID := "codex"
	if snap.LimitID != nil && *snap.LimitID != "" {
		limitID = *snap.LimitID
	} else if snap.LimitName != nil && *snap.LimitName != "" {
		limitID = *snap.LimitName
	}
	limitName := limitID
	if snap.LimitName != nil && *snap.LimitName != "" {
		limitName = *snap.LimitName
	}
	if snap.PlanType != nil && *snap.PlanType != "" && limitName == limitID {
		limitName = fmt.Sprintf("%s (%s plan)", limitID, *snap.PlanType)
	}
	reached := snap.RateLimitReachedType != nil && *snap.RateLimitReachedType != ""

	var out []domain.QuotaSnapshot
	add := func(window string, w *codexWindow) {
		if w == nil || w.UsedPercent == nil {
			return
		}
		s := base
		s.Key.LimitID = limitID
		s.Key.Window = window
		s.LimitName = fmt.Sprintf("%s %s window", limitName, window)
		s.UsedPercent = clampPercent(*w.UsedPercent)
		if reached && s.UsedPercent < 100 && window == domain.WindowPrimary {
			// The provider says the limit is reached even if the rounded
			// percentage is below 100.
			s.UsedPercent = 100
		}
		if w.WindowDurationMins != nil {
			s.WindowDuration = time.Duration(*w.WindowDurationMins * float64(time.Minute))
		}
		if w.ResetsAt != nil && *w.ResetsAt > 0 {
			t := time.Unix(*w.ResetsAt, 0).UTC()
			s.ResetsAt = &t
		}
		out = append(out, s)
	}
	add(domain.WindowPrimary, snap.Primary)
	add(domain.WindowSecondary, snap.Secondary)
	if sc := snap.IndividualLimit; sc != nil && sc.RemainingPercent != nil {
		s := base
		s.Key.LimitID = limitID
		s.Key.Window = domain.WindowSpending
		s.LimitName = fmt.Sprintf("%s spending limit", limitName)
		s.UsedPercent = clampPercent(100 - *sc.RemainingPercent)
		if snap.SpendControlReached != nil && *snap.SpendControlReached {
			s.UsedPercent = 100
		}
		if sc.ResetsAt != nil && *sc.ResetsAt > 0 {
			t := time.Unix(*sc.ResetsAt, 0).UTC()
			s.ResetsAt = &t
		}
		out = append(out, s)
	}
	return out, nil
}

// --- Claude --------------------------------------------------------------

type claudeWindow struct {
	Utilization *float64 `json:"utilization"`
	ResetsAt    *int64   `json:"resetsAt"`
}

type claudeInfo struct {
	Status         string                  `json:"status"`
	ResetsAt       *int64                  `json:"resetsAt"`
	RateLimitType  string                  `json:"rateLimitType"`
	Utilization    *float64                `json:"utilization"`
	UnifiedWindows map[string]claudeWindow `json:"unifiedWindows"`
}

// normalizeClaude handles the Claude Agent SDK rate_limit_event. Newer CLIs
// report every window in unifiedWindows with utilization as a 0..1 fraction;
// older ones report one flat window named by rateLimitType with utilization
// documented as 0..100.
func normalizeClaude(raw json.RawMessage, base domain.QuotaSnapshot) ([]domain.QuotaSnapshot, error) {
	var evt struct {
		Info claudeInfo `json:"rate_limit_info"`
	}
	if err := json.Unmarshal(raw, &evt); err != nil {
		return nil, fmt.Errorf("decode claude rate limits: %w", err)
	}
	info := evt.Info
	var out []domain.QuotaSnapshot
	if len(info.UnifiedWindows) > 0 {
		// Decide the scale once for the whole map: values above 1 mean the
		// provider switched to percentages.
		fraction := true
		for _, w := range info.UnifiedWindows {
			if w.Utilization != nil && *w.Utilization > 1 {
				fraction = false
			}
		}
		for name, w := range info.UnifiedWindows {
			if w.Utilization == nil {
				continue
			}
			s := claudeSnapshot(base, name)
			used := *w.Utilization
			if fraction {
				used *= 100
			}
			s.UsedPercent = clampPercent(used)
			if w.ResetsAt != nil && *w.ResetsAt > 0 {
				t := time.Unix(*w.ResetsAt, 0).UTC()
				s.ResetsAt = &t
			}
			if info.Status == "rejected" && name == info.RateLimitType {
				s.UsedPercent = 100
			}
			out = append(out, s)
		}
		return out, nil
	}
	if info.RateLimitType == "" {
		return nil, errors.New("claude rate limit event has neither unifiedWindows nor rateLimitType")
	}
	s := claudeSnapshot(base, info.RateLimitType)
	switch {
	case info.Utilization != nil:
		s.UsedPercent = clampPercent(*info.Utilization)
	case info.Status == "rejected":
		s.UsedPercent = 100
	case info.Status == "allowed_warning":
		// The provider warns without a number; do not predict, record the
		// warning as the warn threshold neighbourhood is unknown.
		return nil, errors.New("claude rate limit event reports a warning without utilization")
	default:
		return nil, errors.New("claude rate limit event has no utilization")
	}
	if info.Status == "rejected" {
		s.UsedPercent = 100
	}
	if info.ResetsAt != nil && *info.ResetsAt > 0 {
		t := time.Unix(*info.ResetsAt, 0).UTC()
		s.ResetsAt = &t
	}
	return []domain.QuotaSnapshot{s}, nil
}

var claudeWindowDurations = map[string]time.Duration{
	"five_hour": 5 * time.Hour,
	"seven_day": 7 * 24 * time.Hour,
}

func claudeSnapshot(base domain.QuotaSnapshot, window string) domain.QuotaSnapshot {
	s := base
	s.Key.LimitID = "claude"
	s.Key.Window = window
	s.LimitName = "Claude " + strings.ReplaceAll(window, "_", " ")
	s.ModelSelector = claudeModelSelector(window)
	for prefix, d := range claudeWindowDurations {
		if strings.HasPrefix(window, prefix) {
			s.WindowDuration = d
		}
	}
	return s
}

// claudeModelSelector derives the model scope from a window name:
// "seven_day_opus" applies to threads whose model contains "opus".
// Account-wide windows ("five_hour", "seven_day") and overage windows
// return an empty selector.
func claudeModelSelector(window string) string {
	for prefix := range claudeWindowDurations {
		if strings.HasPrefix(window, prefix+"_") {
			rest := strings.TrimPrefix(window, prefix+"_")
			if rest == "" || strings.Contains(rest, "overage") || strings.Contains(rest, "oauth") {
				return ""
			}
			return rest
		}
	}
	return ""
}

func clampPercent(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}
