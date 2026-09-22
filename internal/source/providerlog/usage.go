package providerlog

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// UsageEventType is the canonical event carrying token counts.
const UsageEventType = "thread.token-usage.updated"

// ErrNotUsage marks a well-formed line that is not a usage event.
var ErrNotUsage = errors.New("not a token-usage event")

// ErrUnsupportedUsage marks an identified usage event whose representation is unknown.
var ErrUnsupportedUsage = errors.New("unsupported token-usage representation")

// ParseUsageLine extracts token usage samples from one valid log line. Callers
// needing coverage evidence for malformed or unsupported records use
// ParseUsageEvidenceLine.
func ParseUsageLine(line string) ([]domain.UsageSample, error) {
	if !strings.Contains(line, UsageEventType) {
		return nil, ErrNotUsage
	}
	idx := strings.Index(line, canonMarker)
	if idx < 0 || !strings.HasPrefix(line, "[") {
		return nil, errors.New("usage event has malformed canonical envelope")
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, line[1:idx])
	return ParseUsageJSON([]byte(line[idx+len(canonMarker):]), observedAt)
}

// ParseUsageEvidenceLine returns either measured samples or one sanitized
// diagnostic sample. It never retains raw event content or parser error text.
func ParseUsageEvidenceLine(line string) ([]domain.UsageSample, error) {
	if !strings.Contains(line, UsageEventType) {
		return nil, ErrNotUsage
	}
	usage, err := ParseUsageLine(line)
	if err == nil {
		return usage, nil
	}
	code := "malformed"
	if errors.Is(err, ErrUnsupportedUsage) {
		code = "unsupported"
	}
	var rec usageRecord
	idx := strings.Index(line, canonMarker)
	if idx >= 0 {
		_ = json.Unmarshal([]byte(line[idx+len(canonMarker):]), &rec)
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, rec.CreatedAt)
	if observedAt.IsZero() && idx > 1 {
		observedAt, _ = time.Parse(time.RFC3339Nano, line[1:idx])
	}
	instance := rec.ProviderInstanceID
	if instance == "" {
		instance = rec.Provider
	}
	sum := sha256.Sum256([]byte(line))
	return []domain.UsageSample{{
		ProviderInstanceID: instance,
		ThreadID:           rec.ThreadID,
		ObservedAt:         observedAt,
		SourceEventID:      "diagnostic-" + hex.EncodeToString(sum[:16]),
		Kind:               domain.UsageKindDiagnostic,
		DiagnosticCode:     code,
	}}, nil
}

type usageRecord struct {
	Type               string `json:"type"`
	EventID            string `json:"eventId"`
	Provider           string `json:"provider"`
	ProviderInstanceID string `json:"providerInstanceId"`
	ThreadID           string `json:"threadId"`
	CreatedAt          string `json:"createdAt"`
	TurnID             string `json:"turnId"`
	TurnID2            string `json:"turn_id"`
	Incarnation        string `json:"incarnation"`
	Sequence           int64  `json:"sequence"`
	Raw                struct {
		Method      string          `json:"method"`
		Payload     json.RawMessage `json:"payload"`
		TurnID      string          `json:"turnId"`
		TurnID2     string          `json:"turn_id"`
		Incarnation string          `json:"incarnation"`
		Sequence    int64           `json:"sequence"`
	} `json:"raw"`
}

type optionalUsageCounts struct {
	InputTokens              *int64 `json:"inputTokens"`
	OutputTokens             *int64 `json:"outputTokens"`
	CacheReadInputTokens     *int64 `json:"cacheReadInputTokens"`
	CacheCreationInputTokens *int64 `json:"cacheCreationInputTokens"`
	CacheWriteInputTokens    *int64 `json:"cacheWriteInputTokens"`
	CachedInputTokens        *int64 `json:"cachedInputTokens"`
}

type claudeModelUsage struct {
	optionalUsageCounts
	CostUSD        *float64 `json:"costUSD"`
	CanonicalModel string   `json:"canonicalModel"`
}

type codexTokenUsage struct {
	TokenUsage struct {
		Last  *optionalUsageCounts `json:"last"`
		Total *struct {
			TotalTokens int64 `json:"totalTokens"`
		} `json:"total"`
	} `json:"tokenUsage"`
}

type usageCausalProbe struct {
	TurnID      string `json:"turnId"`
	TurnID2     string `json:"turn_id"`
	SessionID   string `json:"sessionId"`
	SessionID2  string `json:"session_id"`
	Incarnation string `json:"incarnation"`
	Sequence    int64  `json:"sequence"`
}

func causalUsageIdentity(rec usageRecord) (string, string, int64) {
	boundary, incarnation, sequence := rec.TurnID, rec.Incarnation, rec.Sequence
	if boundary == "" {
		boundary = rec.TurnID2
	}
	if boundary == "" {
		boundary = rec.Raw.TurnID
	}
	if boundary == "" {
		boundary = rec.Raw.TurnID2
	}
	if incarnation == "" {
		incarnation = rec.Raw.Incarnation
	}
	if sequence == 0 {
		sequence = rec.Raw.Sequence
	}
	var probe usageCausalProbe
	_ = json.Unmarshal(rec.Raw.Payload, &probe)
	if boundary == "" {
		boundary = probe.TurnID
	}
	if boundary == "" {
		boundary = probe.TurnID2
	}
	if incarnation == "" {
		incarnation = probe.Incarnation
		if incarnation == "" {
			incarnation = probe.SessionID
		}
		if incarnation == "" {
			incarnation = probe.SessionID2
		}
	}
	if sequence == 0 {
		sequence = probe.Sequence
	}
	return boundary, incarnation, sequence
}

func applyOptionalCounts(sample *domain.UsageSample, counts optionalUsageCounts, codex bool) {
	if counts.InputTokens != nil {
		sample.InputTokens = *counts.InputTokens
		sample.FieldPresence |= domain.UsageFieldInput
	}
	if counts.CacheCreationInputTokens != nil {
		sample.CacheWriteTokens = *counts.CacheCreationInputTokens
		sample.FieldPresence |= domain.UsageFieldCacheWrite
	} else if counts.CacheWriteInputTokens != nil {
		sample.CacheWriteTokens = *counts.CacheWriteInputTokens
		sample.FieldPresence |= domain.UsageFieldCacheWrite
	}
	if counts.CacheReadInputTokens != nil {
		sample.CacheReadTokens = *counts.CacheReadInputTokens
		sample.FieldPresence |= domain.UsageFieldCacheRead
	} else if counts.CachedInputTokens != nil {
		sample.CacheReadTokens = *counts.CachedInputTokens
		sample.FieldPresence |= domain.UsageFieldCacheRead
	}
	if counts.OutputTokens != nil {
		sample.OutputTokens = *counts.OutputTokens
		sample.FieldPresence |= domain.UsageFieldOutput
	}
	if codex && counts.InputTokens != nil && counts.CachedInputTokens != nil {
		sample.InputTokens -= *counts.CachedInputTokens
		if sample.InputTokens < 0 {
			sample.InputTokens = 0
		}
	}
}

// ParseUsageJSON parses one canonical usage event body.
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
	boundary, incarnation, sequence := causalUsageIdentity(rec)
	base := domain.UsageSample{
		ProviderInstanceID: instance, ThreadID: rec.ThreadID, ObservedAt: observedAt,
		SourceEventID: rec.EventID, BoundaryID: boundary, Incarnation: incarnation, Sequence: sequence,
	}
	switch rec.Raw.Method {
	case "claude/result":
		var p struct {
			ModelUsage map[string]claudeModelUsage `json:"modelUsage"`
		}
		if err := json.Unmarshal(rec.Raw.Payload, &p); err != nil || len(p.ModelUsage) == 0 {
			return nil, errors.New("claude result has no model usage")
		}
		models := make([]string, 0, len(p.ModelUsage))
		for model := range p.ModelUsage {
			models = append(models, model)
		}
		sort.Strings(models)
		out := make([]domain.UsageSample, 0, len(models))
		for _, model := range models {
			u := p.ModelUsage[model]
			sample := base
			sample.Kind = domain.UsageKindTurn
			sample.Model = model
			if u.CanonicalModel != "" {
				sample.Model = u.CanonicalModel
			}
			sample.SourceEventID = rec.EventID + "#" + model
			applyOptionalCounts(&sample, u.optionalUsageCounts, false)
			if u.CostUSD != nil {
				sample.CostUSD = *u.CostUSD
				sample.CostReported = true
			}
			out = append(out, sample)
		}
		return out, nil
	case "claude/stream_event/message_delta":
		var p struct {
			Event struct {
				Usage *struct {
					InputTokens              *int64 `json:"input_tokens"`
					CacheCreationInputTokens *int64 `json:"cache_creation_input_tokens"`
					CacheReadInputTokens     *int64 `json:"cache_read_input_tokens"`
					OutputTokens             *int64 `json:"output_tokens"`
				} `json:"usage"`
			} `json:"event"`
		}
		if err := json.Unmarshal(rec.Raw.Payload, &p); err != nil || p.Event.Usage == nil {
			return nil, errors.New("claude delta has no usage")
		}
		sample := base
		sample.Kind = domain.UsageKindCall
		counts := optionalUsageCounts{
			InputTokens: p.Event.Usage.InputTokens, CacheCreationInputTokens: p.Event.Usage.CacheCreationInputTokens,
			CacheReadInputTokens: p.Event.Usage.CacheReadInputTokens, OutputTokens: p.Event.Usage.OutputTokens,
		}
		applyOptionalCounts(&sample, counts, false)
		return []domain.UsageSample{sample}, nil
	case "thread/tokenUsage/updated":
		var p codexTokenUsage
		if err := json.Unmarshal(rec.Raw.Payload, &p); err != nil || p.TokenUsage.Last == nil {
			return nil, errors.New("codex update has no last usage")
		}
		sample := base
		sample.Kind = domain.UsageKindCall
		applyOptionalCounts(&sample, *p.TokenUsage.Last, true)
		if p.TokenUsage.Total != nil {
			sample.CumulativeTokens = p.TokenUsage.Total.TotalTokens
		}
		return []domain.UsageSample{sample}, nil
	default:
		return nil, ErrUnsupportedUsage
	}
}

// ScanFile parses every rate-limit observation and all measured or diagnostic
// usage evidence in one log file, in order.
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
			if sample, parseErr := ParseLine(line); parseErr == nil {
				snaps = append(snaps, sample...)
			}
			continue
		}
		if strings.Contains(line, UsageEventType) {
			if evidence, parseErr := ParseUsageEvidenceLine(line); parseErr == nil {
				usage = append(usage, evidence...)
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
