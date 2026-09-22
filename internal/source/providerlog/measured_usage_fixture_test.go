package providerlog

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestMeasuredUsageProviderFixtures(t *testing.T) {
	tests := []struct {
		name       string
		file       string
		wantKind   string
		wantThread string
		wantEvent  string
		wantModel  string
		wantInput  int64
		wantCache  int64
		wantOutput int64
		wantTotal  int64
	}{
		{name: "Claude turn", file: "measured-usage-claude.json", wantKind: domain.UsageKindTurn,
			wantThread: "thread-claude", wantEvent: "claude-event#claude-sonnet",
			wantModel: "claude-sonnet-4", wantInput: 11, wantCache: 5, wantOutput: 7},
		{name: "Codex call", file: "measured-usage-codex.json", wantKind: domain.UsageKindCall,
			wantThread: "thread-codex", wantEvent: "codex-event",
			wantInput: 17, wantCache: 13, wantOutput: 9, wantTotal: 54},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join("testdata", tt.file))
			if err != nil {
				t.Fatal(err)
			}
			got, err := ParseUsageJSON(body, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 {
				t.Fatalf("samples = %d, want 1", len(got))
			}
			sample := got[0]
			if sample.Kind != tt.wantKind || sample.ThreadID != tt.wantThread ||
				sample.SourceEventID != tt.wantEvent || sample.Model != tt.wantModel ||
				sample.InputTokens != tt.wantInput || sample.CacheReadTokens != tt.wantCache ||
				sample.OutputTokens != tt.wantOutput || sample.CumulativeTokens != tt.wantTotal ||
				sample.FieldPresence != domain.UsageFieldsAll {
				t.Fatalf("sample = %#v", sample)
			}
		})
	}
}

func TestMeasuredUsageClaudeModelsAreDeterministicAndCostEvidenceIsExplicit(t *testing.T) {
	body := []byte(`{"type":"thread.token-usage.updated","eventId":"turn","provider":"claude","threadId":"thread","createdAt":"2026-09-22T18:00:00Z","raw":{"method":"claude/result","payload":{"modelUsage":{"z-subagent":{"inputTokens":3,"outputTokens":4,"costUSD":0.2},"a-parent":{"inputTokens":1,"outputTokens":2}}}}}`)
	got, err := ParseUsageJSON(body, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Model != "a-parent" || got[1].Model != "z-subagent" {
		t.Fatalf("model order = %#v", got)
	}
	if got[0].CostReported || !got[1].CostReported || got[1].CostUSD != .2 {
		t.Fatalf("cost evidence = %#v", got)
	}
}

func TestMeasuredUsageMalformedAndUnsupportedEvidenceIsNotAcceptedAsZero(t *testing.T) {
	if _, err := ParseUsageJSON([]byte(`{"type":"thread.token-usage.updated"`), time.Time{}); err == nil {
		t.Fatal("malformed usage was accepted")
	}
	unsupported := []byte(`{"type":"thread.token-usage.updated","eventId":"future","provider":"future","threadId":"thread","createdAt":"2026-09-22T18:00:00Z","raw":{"method":"future/usage","payload":{}}}`)
	if _, err := ParseUsageJSON(unsupported, time.Time{}); !errors.Is(err, ErrUnsupportedUsage) {
		t.Fatalf("unsupported method error = %v", err)
	}
	missing := []byte(`{"type":"thread.token-usage.updated","eventId":"missing","provider":"codex","threadId":"thread","raw":{"method":"thread/tokenUsage/updated","payload":{"tokenUsage":{"last":{"inputTokens":1}}}}}`)
	if _, err := ParseUsageJSON(missing, time.Time{}); err == nil {
		t.Fatal("missing timestamp was accepted as zero-time evidence")
	}
}

func TestUsageEvidenceSanitizesDiagnosticsAndCarriesPresenceAndCausality(t *testing.T) {
	secret := "credential-super-secret"
	malformed := `[2026-09-22T18:00:00Z] CANON: {"type":"thread.token-usage.updated","payload":"` + secret + `"`
	diagnostics, err := ParseUsageEvidenceLine(malformed)
	if err != nil || len(diagnostics) != 1 || diagnostics[0].Kind != domain.UsageKindDiagnostic ||
		diagnostics[0].DiagnosticCode != "malformed" {
		t.Fatalf("malformed diagnostic = %#v, %v", diagnostics, err)
	}
	raw, _ := json.Marshal(diagnostics)
	if strings.Contains(string(raw), secret) {
		t.Fatalf("diagnostic retained source content: %s", raw)
	}

	line := `[2026-09-22T18:00:00Z] CANON: {"type":"thread.token-usage.updated","eventId":"partial","provider":"codex","threadId":"thread","createdAt":"2026-09-22T18:00:00Z","turnId":"turn-7","sequence":4,"raw":{"method":"thread/tokenUsage/updated","payload":{"sessionId":"session-2","tokenUsage":{"last":{"inputTokens":9},"total":{"totalTokens":9}}}}}`
	usage, err := ParseUsageEvidenceLine(line)
	if err != nil || len(usage) != 1 {
		t.Fatalf("partial usage = %#v, %v", usage, err)
	}
	if usage[0].FieldPresence != domain.UsageFieldInput || usage[0].BoundaryID != "turn-7" ||
		usage[0].Incarnation != "session-2" || usage[0].Sequence != 4 {
		t.Fatalf("presence/causality = %#v", usage[0])
	}

	unsupportedLine := `[2026-09-22T18:00:00Z] CANON: {"type":"thread.token-usage.updated","eventId":"future","provider":"future","threadId":"thread","createdAt":"2026-09-22T18:00:00Z","raw":{"method":"future/usage","payload":{}}}`
	unsupportedEvidence, err := ParseUsageEvidenceLine(unsupportedLine)
	if err != nil || len(unsupportedEvidence) != 1 || unsupportedEvidence[0].DiagnosticCode != "unsupported" {
		t.Fatalf("unsupported diagnostic = %#v, %v", unsupportedEvidence, err)
	}
}
