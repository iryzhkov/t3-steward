package sqlitetest

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// SeedHistoricQuarantine writes the quarantine marker that retired Markdown
// intake left behind for key into the migrated database at path. The store no
// longer creates markers; it only lists them and releases them on an operator's
// authenticated, audited instruction, so tests of those paths seed the
// historical row directly in the shape the retired writer stored it. The
// writer's submission-quarantined audit event is not seeded: nothing reads it.
func SeedHistoricQuarantine(path, key, digest, reason string, at time.Time) error {
	if strings.TrimSpace(key) != key || key == "" || len(sqlite.QuarantineKey(key)) > 256 {
		return errors.New("historic quarantine key must be trimmed, nonempty and fit the 256-character record key")
	}
	if raw, err := hex.DecodeString(digest); err != nil || len(raw) != 32 {
		return errors.New("historic quarantine digest must be a SHA-256 hex string")
	}
	if strings.TrimSpace(reason) != reason || reason == "" || at.IsZero() {
		return errors.New("historic quarantine requires a trimmed reason and a time")
	}
	record := domain.SubmissionRecord{
		Key: sqlite.QuarantineKey(key), Digest: digest, State: domain.SubmissionQuarantined,
		Reason: reason, CreatedAt: at.UTC(),
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode historic quarantine %q: %w", key, err)
	}
	dsn := (&url.URL{Scheme: "file", Path: path}).String() + "?mode=rw&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open state database: %w", err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		INSERT INTO coordinator_submissions(
			key, digest, workflow_id, run_id, state, created_at, accepted_at, record
		) VALUES (?, ?, '', '', ?, ?, NULL, ?)
	`, record.Key, record.Digest, record.State,
		record.CreatedAt.Format(time.RFC3339Nano), raw); err != nil {
		return fmt.Errorf("seed historic quarantine %q: %w", key, err)
	}
	return nil
}
