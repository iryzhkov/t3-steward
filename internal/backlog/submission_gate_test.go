package backlog

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// blockingValidator records concurrency: it reports the highest number of
// validations that were ever in flight at the same time.
type blockingValidator struct {
	release chan struct{}
	mu      sync.Mutex
	active  int
	peak    int
	err     error
}

func (v *blockingValidator) ValidatePermanent(context.Context, Manifest) error {
	v.mu.Lock()
	v.active++
	if v.active > v.peak {
		v.peak = v.active
	}
	v.mu.Unlock()
	<-v.release
	v.mu.Lock()
	v.active--
	v.mu.Unlock()
	return v.err
}

func (v *blockingValidator) peakConcurrency() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.peak
}

// TestPermanentValidationDoesNotHoldThePublicationLock is the queueing
// regression.
//
// The validation dials every candidate worker over SSH, serially, with a
// per-candidate timeout. Running it under the publication mutex made every
// submission on the coordinator wait for one slow fleet.
func TestPermanentValidationDoesNotHoldThePublicationLock(t *testing.T) {
	validator := &blockingValidator{release: make(chan struct{})}
	service := gateService(t)
	service.Permanent = validator
	bundle := validBundle(t)

	started := sync.WaitGroup{}
	started.Add(2)
	for index := range 2 {
		go func() {
			defer started.Done()
			_, _ = service.SubmitDirectory(context.Background(), DirectorySubmission{
				IdempotencyKey: "campaign-" + string(rune('a'+index)),
				BundleDir:      bundle,
			})
		}()
	}
	// Both validations have to be able to run at once. Under the lock the
	// second could not begin until the first had finished ingesting.
	deadline := time.After(10 * time.Second)
	for validator.peakConcurrency() < 2 {
		select {
		case <-deadline:
			close(validator.release)
			started.Wait()
			t.Fatalf("peak concurrent validations = %d, want 2: the publication lock is still held across validation",
				validator.peakConcurrency())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(validator.release)
	started.Wait()
}

// gateService builds a submission service on a real store, so these tests
// exercise ordering against the persistence the production path uses.
func gateService(t *testing.T) *SubmissionService {
	t.Helper()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	storage := filepath.Join(t.TempDir(), "bundles")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	return &SubmissionService{
		StorageRoot: storage, Store: store,
		MaxBytes: 4 << 20, MaxFiles: 1000,
	}
}

// TestAuditRecordsOnlyWhatIsActuallySubmitted states that a refused submission
// leaves no audit line claiming it was submitted without a client-side check.
func TestAuditRecordsOnlyWhatIsActuallySubmitted(t *testing.T) {
	var audited []SubmissionAudit
	service := gateService(t)
	service.Permanent = refusingPermanent{err: errors.New("this campaign can never run as written")}
	service.Audit = func(_ context.Context, audit SubmissionAudit) { audited = append(audited, audit) }
	_, err := service.SubmitDirectory(context.Background(), DirectorySubmission{
		IdempotencyKey: "refused-1", BundleDir: validBundle(t),
		Principal: "local:1000", Unverified: true, UnverifiedReason: "coordinator rebuild",
	})
	if err == nil {
		t.Fatal("the refusal did not happen")
	}
	if len(audited) != 0 {
		t.Fatalf("a refused submission was audited as submitted: %+v", audited)
	}

	service.Permanent = nil
	if _, err := service.SubmitDirectory(context.Background(), DirectorySubmission{
		IdempotencyKey: "accepted-1", BundleDir: validBundle(t),
		Principal: "local:1000", Unverified: true, UnverifiedReason: "coordinator rebuild",
	}); err != nil {
		t.Fatal(err)
	}
	if len(audited) != 1 {
		t.Fatalf("an accepted submission produced %d audit records, want 1", len(audited))
	}
	if !audited[0].Unverified || audited[0].UnverifiedReason != "coordinator rebuild" ||
		audited[0].Principal != "local:1000" {
		t.Fatalf("audit record = %+v", audited[0])
	}
}

type refusingPermanent struct{ err error }

func (p refusingPermanent) ValidatePermanent(context.Context, Manifest) error { return p.err }
