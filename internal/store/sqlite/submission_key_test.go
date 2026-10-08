package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A new submission key is held to the rule the client applies offline, and
// every refusal is worded the same, so that a key the client refuses is never
// one the coordinator would have accepted, or the other way round.
func TestReserveSubmissionRefusesUnsafeKeysInTheClientWords(t *testing.T) {
	store, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	for _, key := range []string{"", " padded", strings.Repeat("k", 257), strings.Repeat("k", 400), "key\x00", "key\x1b[2J", "ke\ny"} {
		proposed := submissionRecordFixture(key, submissionDigestFixture("unsafe"), now)
		if _, _, err := store.ReserveSubmission(context.Background(), proposed); err == nil || err.Error() != domain.IdempotencyKeyRule {
			t.Errorf("key %q: err = %v, want %q", key, err, domain.IdempotencyKeyRule)
		}
	}
	proposed := submissionRecordFixture(strings.Repeat("k", 256), submissionDigestFixture("safe"), now)
	if _, _, err := store.ReserveSubmission(context.Background(), proposed); err != nil {
		t.Fatalf("256-byte key refused: %v", err)
	}
}

// A record written before the control-character rule still decodes: only the
// length and trim rule it was written under applies when it is read back.
func TestStoredSubmissionWithLegacyKeyStillDecodes(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	record := submissionRecordFixture("legacy\x01key", submissionDigestFixture("legacy"), now)
	if err := validateSubmissionRecord(record, false); err != nil {
		t.Fatalf("legacy record refused on read: %v", err)
	}
}
