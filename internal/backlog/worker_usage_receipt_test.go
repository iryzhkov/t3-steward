package backlog

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// A rejected usage sample is kept nowhere on the coordinator, so the one log
// line per batch must name each sample well enough to find it on the worker,
// and a replayed batch must not repeat it.
func TestReceiveWorkerUsageLogsRejectedSamplesOncePerBatch(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	sample := func(id, thread string, tokens int64) domain.UsageSample {
		return domain.UsageSample{
			ProviderInstanceID: "claudeAgent", ThreadID: thread, Model: "claude-haiku-4-5", ObservedAt: now,
			SourceEventID: id, Kind: domain.UsageKindCall, FieldPresence: domain.UsageFieldsAll, InputTokens: tokens,
		}
	}
	batch := []domain.UsageSample{sample("good", "thread-a", 3), sample("bad-1", "thread-b", -1), sample("bad-2", "thread-c", -2)}

	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	if err := receiveWorkerUsage(ctx, store, "worker-a", batch); err != nil {
		t.Fatal(err)
	}
	output := logged.String()
	if lines := strings.Count(output, "worker usage samples rejected and acknowledged"); lines != 1 {
		t.Fatalf("rejection lines = %d, want one per batch: %s", lines, output)
	}
	for _, want := range []string{"bad-1", "thread-b", "bad-2", "thread-c", "claudeAgent", "claude-haiku-4-5", "negative token count", "rejected=2", "stored=1"} {
		if !strings.Contains(output, want) {
			t.Fatalf("rejection line lacks %q: %s", want, output)
		}
	}

	logged.Reset()
	if err := receiveWorkerUsage(ctx, store, "worker-a", batch); err != nil {
		t.Fatal(err)
	}
	if output := logged.String(); strings.Contains(output, "rejected") {
		t.Fatalf("a replayed batch logged its rejections again: %s", output)
	}
}
