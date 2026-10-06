package providerlog

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func sampleLine(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(string(b), "\n")[0]
}

func TestClaudeNativeGolden(t *testing.T) {
	line := sampleLine(t, "native-allowed-sample.log")
	snaps, err := ParseLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 2 {
		t.Fatalf("snapshots = %+v", snaps)
	}
	for _, s := range snaps {
		if s.UsedPercent != 1 || s.Key.ProviderInstanceID != "claudeAgent" || s.Key.LimitID != "claude" ||
			s.SourceEventID != "fec25330-7377-4676-8038-cc1a19277f43" ||
			!s.ObservedAt.Equal(time.Date(2026, 10, 6, 6, 1, 6, 684000000, time.UTC)) {
			t.Fatalf("snapshot = %+v", s)
		}
	}
	if snaps[0].Key.Window != "five_hour" || snaps[1].Key.Window != "seven_day" {
		t.Fatal(snaps)
	}
	for _, unrelated := range []string{
		strings.Replace(line, "claude/rate_limit_event", "claude/message", 1),
		strings.Replace(line, "\"provider\":\"claudeAgent\"", "\"provider\":\"codex\"", 1),
	} {
		if _, err := ParseLine(unrelated); !errors.Is(err, ErrNotRateLimit) {
			t.Fatalf("unrelated: %v", err)
		}
	}
	if _, err := ParseLine("[2026-10-06T06:01:06Z] NTIVE: {\"event\":"); err == nil || errors.Is(err, ErrNotRateLimit) {
		t.Fatalf("malformed native: %v", err)
	}
}

func TestClaudeCanonGolden(t *testing.T) {
	snaps, err := ParseLine(sampleLine(t, "canon-warning-sample.log"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"five_hour": 3, "seven_day": 96, "seven_day_overage_included": 37}
	if len(snaps) != len(want) {
		t.Fatal(snaps)
	}
	for _, s := range snaps {
		if s.UsedPercent != want[s.Key.Window] {
			t.Fatal(s)
		}
	}
}

// The two T3 event IDs differ; both copies must use the SDK UUID so the
// daemon's persistent per-window event deduplication recognizes them.
func TestClaudeNativeCanonIdentity(t *testing.T) {
	line := sampleLine(t, "canon-warning-sample.log")
	var rec map[string]json.RawMessage
	if err := json.Unmarshal([]byte(strings.SplitN(line, canonMarker, 2)[1]), &rec); err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(rec["raw"], &raw); err != nil {
		t.Fatal(err)
	}
	native := "[2026-10-04T03:14:49.833Z] NTIVE: {\"event\":{\"id\":\"e90641fc-2d2f-4a0c-89d6-b6f3a7e7c010\",\"provider\":\"claudeAgent\",\"method\":\"claude/rate_limit_event\",\"createdAt\":\"2026-10-04T03:14:49.832Z\",\"payload\":" + string(raw.Payload) + "}}"
	canonSnaps, err := ParseLine(line)
	if err != nil {
		t.Fatal(err)
	}
	nativeSnaps, err := ParseLine(native)
	if err != nil {
		t.Fatal(err)
	}
	if len(canonSnaps) != len(nativeSnaps) {
		t.Fatal(nativeSnaps)
	}
	for i, c := range canonSnaps {
		if c.SourceEventID != nativeSnaps[i].SourceEventID {
			t.Fatalf("canon=%s native=%s", c.SourceEventID, nativeSnaps[i].SourceEventID)
		}
	}
}

func TestClaudeNativeNormalizedCanonIdentity(t *testing.T) {
	body := []byte(`{"type":"account.rate-limits.updated","eventId":"t3-id","provider":"claudeAgent","createdAt":"2026-10-06T06:01:06.684Z","raw":{"method":"claude/rate_limit_event","payload":{"uuid":"sdk-id"}},"payload":{"limits":{"windows":[{"id":"seven_day","kind":"weekly","label":"Claude weekly","usedPercent":1}]}}}`)
	snaps, err := ParseJSON(body, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 || snaps[0].SourceEventID != "sdk-id" {
		t.Fatalf("normalized copy=%+v", snaps)
	}
}

func TestClaudeNativeLiveAndBootstrap(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/events.native.log"
	appendFile(t, path, sampleLine(t, "native-allowed-sample.log")+"\n")
	tailer := NewTailer(Options{Dir: dir}, newMemPositions())
	out := make(chan domain.QuotaSnapshot, 8)
	tailer.scan(t.Context(), out)
	if len(out) != 2 {
		t.Fatalf("live snapshots=%d", len(out))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if snaps := tailer.tailSnapshots(path, info.Size()); len(snaps) != 2 {
		t.Fatalf("bootstrap=%+v", snaps)
	}
}
