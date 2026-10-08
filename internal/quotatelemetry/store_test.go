package quotatelemetry

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRetentionPrunesByAgeAndRowCapInBatches(t *testing.T) {
	ctx := context.Background()
	path := testStorePath(t)
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := testBase
	var events []Event
	for index := range 30 {
		at := now.Add(-time.Duration(index) * time.Hour)
		if index >= 20 {
			at = now.Add(-31*24*time.Hour - time.Duration(index)*time.Minute)
		}
		events = append(events, Event{SchemaVersion: SchemaVersion, EventID: fmt.Sprintf("e-%02d", index), Kind: KindRecorder, At: at,
			Recorder: &RecorderNote{State: "started"}})
	}
	if err := store.Append(ctx, events); err != nil {
		t.Fatal(err)
	}

	// Ten events are older than the age limit; the row cap of 15 removes the
	// five oldest of the rest. Batches of 4 rows, at most 2 transactions.
	limits := pruneLimits{maxAge: 30 * 24 * time.Hour, maxRows: 15, batch: 4, maxTransactions: 2}
	result, err := store.prune(ctx, now, limits)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 8 || result.Transactions != 2 || result.Complete {
		t.Fatalf("first prune = %+v; want 8 rows in 2 transactions, incomplete", result)
	}
	limits.maxTransactions = 20
	result, err = store.prune(ctx, now, limits)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 7 || !result.Complete {
		t.Fatalf("second prune = %+v; want the remaining 2 old and 5 over the cap", result)
	}
	remaining := allEvents(t, path, now)
	if len(remaining) != 15 {
		t.Fatalf("remaining = %d; want 15", len(remaining))
	}
	for _, event := range remaining {
		var index int
		fmt.Sscanf(event.EventID, "e-%d", &index)
		if index >= 15 {
			t.Fatalf("kept %s; the oldest must go first", event.EventID)
		}
	}
	info, err := store.Info(ctx)
	if err != nil || info.Rows != 15 || info.OldestRetained == nil || !info.OldestRetained.Equal(now.Add(-14*time.Hour)) {
		t.Fatalf("info = %+v, %v; want 15 rows, oldest retained 14h ago", info, err)
	}

	// A long record is capped at 4 KiB with its long strings cut.
	huge := Event{SchemaVersion: SchemaVersion, EventID: "huge", Kind: KindRecorder, At: now,
		Recorder: &RecorderNote{State: "gap", LastError: strings.Repeat("e", 10000), Reason: strings.Repeat("r", 10000)}}
	if err := store.Append(ctx, []Event{huge}); err != nil {
		t.Fatal(err)
	}
	var size int
	if err := store.db.QueryRowContext(ctx, `SELECT length(record) FROM events WHERE event_id = 'huge'`).Scan(&size); err != nil {
		t.Fatal(err)
	}
	if size > MaxRecordBytes {
		t.Fatalf("record is %d bytes; want at most %d", size, MaxRecordBytes)
	}
}

func TestTelemetryStorePermissionsAndReadOnlyOpen(t *testing.T) {
	ctx := context.Background()
	path := testStorePath(t)
	if _, err := OpenReader(path); err == nil || !strings.Contains(err.Error(), "no quota telemetry store") {
		t.Fatalf("read open of a missing store = %v", err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("the read path created the store directory: %v", err)
	}
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []Event{{SchemaVersion: SchemaVersion, EventID: "one", Kind: KindRecorder, At: testBase,
		Recorder: &RecorderNote{State: "started"}}}); err != nil {
		t.Fatal(err)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil || dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode = %v, %v; want 0700", dirInfo.Mode().Perm(), err)
	}
	for _, file := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(file)
		if err != nil {
			if file != path && os.IsNotExist(err) {
				continue
			}
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %v; want 0600", file, info.Mode().Perm())
		}
	}
	var journal string
	if err := store.db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("journal mode = %q, %v; want wal", journal, err)
	}

	reader, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Append(ctx, []Event{{SchemaVersion: SchemaVersion, EventID: "two", Kind: KindRecorder, At: testBase,
		Recorder: &RecorderNote{State: "started"}}}); err == nil {
		t.Fatal("the read path appended an event")
	}
	if _, err := reader.db.ExecContext(ctx, `DELETE FROM events`); err == nil {
		t.Fatal("the read path deleted events")
	}
	reader.Close()

	if _, err := store.db.ExecContext(ctx, `UPDATE meta SET value = '2' WHERE key = 'schemaVersion'`); err != nil {
		t.Fatal(err)
	}
	store.Close()
	if _, err := OpenReader(path); err == nil || !strings.Contains(err.Error(), "schema version 2") || !strings.Contains(err.Error(), "version 1") {
		t.Fatalf("read open of a newer schema = %v; want a refusal naming both versions", err)
	}
	if _, err := OpenStore(path); err == nil || !strings.Contains(err.Error(), "schema version 2") {
		t.Fatalf("recorder open of a newer schema = %v; want a refusal", err)
	}
}
