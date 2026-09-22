package providerlog

import (
	"os"
	"path/filepath"
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
				sample.OutputTokens != tt.wantOutput || sample.CumulativeTokens != tt.wantTotal {
				t.Fatalf("sample = %#v", sample)
			}
		})
	}
}
