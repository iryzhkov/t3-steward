package providerlog

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// UsageEventType is the canonical event carrying token counts.
const UsageEventType = "thread.token-usage.updated"

// ErrNotUsage marks a well-formed line that is not a usage event of a
// shape the watchdog understands.
var ErrNotUsage = errors.New("not a usable token-usage event")

// ParseUsageLine extracts token usage samples from one log line.
//
// Claude: the `claude/result` message that ends a turn carries
// `modelUsage`, one entry per model the turn used (subagents included), so
// each entry becomes a sample with an exact model. Per-call
// `message_delta` events are ignored to avoid double counting.
//
// Codex: every `thread/tokenUsage/updated` notification carries the last
// call's counts in `tokenUsage.last`; the model is left empty and filled in
// from the thread's model selection by the caller.
func ParseUsageLine(line string) ([]domain.UsageSample, error) {
	if !strings.Contains(line, UsageEventType) {
		return nil, ErrNotUsage
	}
	idx := strings.Index(line, canonMarker)
	if idx < 0 || !strings.HasPrefix(line, "[") {
		return nil, ErrNotUsage
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, line[1:idx])
	return ParseUsageJSON([]byte(line[idx+len(canonMarker):]), observedAt)
}

type usageRecord struct {
	Type               string `json:"type"`
	EventID            string `json:"eventId"`
	Provider           string `json:"provider"`
	ProviderInstanceID string `json:"providerInstanceId"`
	ThreadID           string `json:"threadId"`
	CreatedAt          string `json:"createdAt"`
	Raw                struct {
		Method  string          `json:"method"`
		Payload json.RawMessage `json:"payload"`
	} `json:"raw"`
}

type claudeModelUsage struct {
	InputTokens              int64   `json:"inputTokens"`
	OutputTokens             int64   `json:"outputTokens"`
	CacheReadInputTokens     int64   `json:"cacheReadInputTokens"`
	CacheCreationInputTokens int64   `json:"cacheCreationInputTokens"`
	CostUSD                  float64 `json:"costUSD"`
	CanonicalModel           string  `json:"canonicalModel"`
}

type codexTokenUsage struct {
	TokenUsage struct {
		Last *struct {
			CacheWriteInputTokens int64 `json:"cacheWriteInputTokens"`
			CachedInputTokens     int64 `json:"cachedInputTokens"`
			InputTokens           int64 `json:"inputTokens"`
			OutputTokens          int64 `json:"outputTokens"`
		} `json:"last"`
		Total *struct {
			TotalTokens int64 `json:"totalTokens"`
		} `json:"total"`
	} `json:"tokenUsage"`
}

// ParseUsageJSON parses one canonical event body.
func ParseUsageJSON(body []byte, fallbackObservedAt time.Time) ([]domain.UsageSample, error) {
	var rec usageRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	if rec.Type != UsageEventType {
		return nil, ErrNotUsage
	}
	observedAt := fallbackObservedAt
	if t, err := time.Parse(time.RFC3339Nano, rec.CreatedAt); err == nil {
		observedAt = t
	}
	if observedAt.IsZero() {
		return nil, errors.New("event has no timestamp")
	}
	instance := rec.ProviderInstanceID
	if instance == "" {
		instance = rec.Provider
	}
	base := domain.UsageSample{ProviderInstanceID: instance, ThreadID: rec.ThreadID, ObservedAt: observedAt, SourceEventID: rec.EventID}
	switch rec.Raw.Method {
	case "claude/result":
		// One sample per model for the whole turn: exact model split.
		var p struct {
			ModelUsage map[string]claudeModelUsage `json:"modelUsage"`
		}
		if err := json.Unmarshal(rec.Raw.Payload, &p); err != nil || len(p.ModelUsage) == 0 {
			return nil, ErrNotUsage
		}
		var out []domain.UsageSample
		for model, u := range p.ModelUsage {
			s := base
			s.Kind = domain.UsageKindTurn
			s.Model = model
			if u.CanonicalModel != "" {
				s.Model = u.CanonicalModel
			}
			s.SourceEventID = rec.EventID + "#" + model
			s.InputTokens = u.InputTokens
			s.CacheWriteTokens = u.CacheCreationInputTokens
			s.CacheReadTokens = u.CacheReadInputTokens
			s.OutputTokens = u.OutputTokens
			s.CostUSD = u.CostUSD
			out = append(out, s)
		}
		return out, nil
	case "claude/stream_event/message_delta":
		// One sample per API call, subagent calls included: exact timing,
		// model left to the thread's selection.
		var p struct {
			Event struct {
				Usage *struct {
					InputTokens              int64 `json:"input_tokens"`
					CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
					CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
					OutputTokens             int64 `json:"output_tokens"`
				} `json:"usage"`
			} `json:"event"`
		}
		if err := json.Unmarshal(rec.Raw.Payload, &p); err != nil || p.Event.Usage == nil {
			return nil, ErrNotUsage
		}
		u := p.Event.Usage
		s := base
		s.Kind = domain.UsageKindCall
		s.InputTokens = u.InputTokens
		s.CacheWriteTokens = u.CacheCreationInputTokens
		s.CacheReadTokens = u.CacheReadInputTokens
		s.OutputTokens = u.OutputTokens
		return []domain.UsageSample{s}, nil
	case "thread/tokenUsage/updated":
		var p codexTokenUsage
		if err := json.Unmarshal(rec.Raw.Payload, &p); err != nil || p.TokenUsage.Last == nil {
			return nil, ErrNotUsage
		}
		last := p.TokenUsage.Last
		s := base
		s.Kind = domain.UsageKindCall
		// Codex reports cached tokens as part of inputTokens.
		s.InputTokens = last.InputTokens - last.CachedInputTokens
		if s.InputTokens < 0 {
			s.InputTokens = 0
		}
		s.CacheWriteTokens = last.CacheWriteInputTokens
		s.CacheReadTokens = last.CachedInputTokens
		s.OutputTokens = last.OutputTokens
		if p.TokenUsage.Total != nil {
			s.CumulativeTokens = p.TokenUsage.Total.TotalTokens
		}
		return []domain.UsageSample{s}, nil
	default:
		return nil, ErrNotUsage
	}
}

// ScanFile parses every rate-limit observation and usage sample in one log
// file, in order. It is used by the report backfill.
func ScanFile(path string) ([]domain.QuotaSnapshot, []domain.UsageSample, error) {
	f, err := openFile(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	var snaps []domain.QuotaSnapshot
	var usage []domain.UsageSample
	scanner := newScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, EventType) {
			if s, err := ParseLine(line); err == nil {
				snaps = append(snaps, s...)
			}
			continue
		}
		if strings.Contains(line, UsageEventType) {
			if u, err := ParseUsageLine(line); err == nil {
				usage = append(usage, u...)
			}
		}
	}
	return snaps, usage, scanner.Err()
}

func openFile(path string) (*os.File, error) { return os.Open(path) }

func newScanner(f *os.File) *bufio.Scanner {
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 1<<20), 16<<20)
	return s
}
