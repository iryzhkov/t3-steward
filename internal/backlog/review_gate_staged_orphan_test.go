package backlog

import (
	"testing"
)

// orphanStaging writes what an earlier attempt that died between its staging
// record and its staged ref leaves in a worker's store: a record of the same
// commit, and no ref.
func orphanStaging(t *testing.T, worker commitWorker, produced producedCommit, attemptID string) {
	t.Helper()
	orphan := produced.provenance
	orphan.StagedAttempt = attemptID
	if err := writeCommitRecord(worker.refs.stagedPath("run-1", "task-producer", attemptID, "repair"), orphan); err != nil {
		t.Fatal(err)
	}
}

// Review round 2, finding 2: Stage writes its record before it copies its
// ref, so a crash leaves an orphaned record. A later attempt that staged the
// same commit completely must still be what a judge inspects and what an
// accepted consumer publishes, on the producing worker itself.
func TestAnOrphanedEarlierStagingDoesNotMaskALaterOne(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "judge"
		if accepted {
			name = "accepted"
		}
		t.Run(name, func(t *testing.T) {
			repository := newGitFixture(t)
			storage := t.TempDir()
			worker := newCommitWorker(t, storage)
			produced := produceCommit(t, worker, repository, storage, produceOptions{reviewGated: true})
			orphanStaging(t, worker, produced, "attempt-0")
			delivery := produced.delivery(t, nil)
			prepared, err := consumeReviewed(t, worker, produced, &delivery, "attempt-consumer", accepted)
			if err != nil {
				t.Fatalf("an orphaned attempt-0 record masks attempt-1's complete staging: %v", err)
			}
			if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", CampaignRef("run-1", "task-producer", "repair")); got != produced.commit {
				t.Fatalf("consumer fetched %s, want %s", got, produced.commit)
			}
			_, err = worker.refs.Resolve("run-1", "task-producer", "repair")
			if accepted != (err == nil) {
				t.Fatalf("accepted %v: Resolve error %v", accepted, err)
			}
		})
	}
}

// The same crash on a consuming worker: an import that wrote attempt-0's
// record and died before its ref, then a later consumer delivered attempt-1's
// complete staging. The import of attempt-1 succeeds, and the fetch that
// follows must read attempt-1's ref rather than the orphan's missing one.
func TestAnOrphanedImportDoesNotMaskALaterStagingImport(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "judge"
		if accepted {
			name = "accepted"
		}
		t.Run(name, func(t *testing.T) {
			repository := newGitFixture(t)
			storage := t.TempDir()
			produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{reviewGated: true})
			consumer := newCommitWorker(t, storage)
			orphanStaging(t, consumer, produced, "attempt-0")
			delivery := produced.delivery(t, nil)
			prepared, err := consumeReviewed(t, consumer, produced, &delivery, "attempt-consumer", accepted)
			if err != nil {
				t.Fatalf("an orphaned imported record masks the later import: %v", err)
			}
			if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", CampaignRef("run-1", "task-producer", "repair")); got != produced.commit {
				t.Fatalf("consumer fetched %s, want %s", got, produced.commit)
			}
		})
	}
}

// A staging record from before the attempt was recorded names none. It is
// matched against every attempt's usable staging, so an orphan sorted first
// is skipped rather than returned.
func TestARecordWithoutAnAttemptSkipsAnOrphanedStaging(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	worker := newCommitWorker(t, storage)
	produced := produceCommit(t, worker, repository, storage, produceOptions{reviewGated: true})
	orphanStaging(t, worker, produced, "attempt-0")
	legacy := produced.provenance
	legacy.StagedAttempt = ""
	produced.record = forgedOutput(t, produced, "repair", legacy)
	prepared, err := consumeReviewed(t, worker, produced, nil, "attempt-judge", false)
	if err != nil {
		t.Fatalf("an orphaned attempt-0 record masks attempt-1's staging for a record without an attempt: %v", err)
	}
	if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", CampaignRef("run-1", "task-producer", "repair")); got != produced.commit {
		t.Fatalf("judge fetched %s, want %s", got, produced.commit)
	}
	if _, err := worker.refs.Resolve("run-1", "task-producer", "repair"); err == nil {
		t.Fatal("a judge published the staged commit")
	}
}
