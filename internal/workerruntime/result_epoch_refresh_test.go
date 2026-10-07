package workerruntime

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// reopenCustodyAtEpoch reopens a custody store as a restarted worker would
// after the coordinator moved to a new epoch.
func reopenCustodyAtEpoch(t *testing.T, store *CustodyStore, epoch int64) *CustodyStore {
	t.Helper()
	config := store.config
	config.CoordinatorEpoch = epoch
	next, err := OpenCustodyStore(config)
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func resultReceiptPath(t *testing.T, store *CustodyStore, directory string) string {
	t.Helper()
	return filepath.Join(store.config.Root, directory, "upload-assignment-1-result.json")
}

// A result receipt published under an earlier coordinator epoch stays this
// worker's durable result after a same-assignment replay refreshes only the
// package's coordinator epoch, whether the receipt is still in the outbox or
// already acknowledged. Publication then leaves the receipt untouched.
func TestResultDurableAcceptsAReceiptFromAnEarlierCoordinatorEpoch(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(fmt.Sprintf("acknowledged=%v", acknowledged), func(t *testing.T) {
			root := t.TempDir()
			old := testCustodyStore(t, root, func() time.Time { return runtimeTestNow })
			pkg := testPackage()
			result := PublishedResult{FinalMessage: "done", ThreadArchive: []byte("{}")}
			if err := old.PublishResult(context.Background(), pkg, result); err != nil {
				t.Fatal(err)
			}
			directory := "outbox"
			if acknowledged {
				if err := old.AcknowledgeUpload("upload-assignment-1-result"); err != nil {
					t.Fatal(err)
				}
				directory = "acknowledged"
			}
			before, err := os.ReadFile(resultReceiptPath(t, old, directory))
			if err != nil {
				t.Fatal(err)
			}
			next := reopenCustodyAtEpoch(t, old, old.config.CoordinatorEpoch+1)
			pkg.CoordinatorEpoch++
			durable, err := next.ResultDurable(pkg)
			if err != nil || !durable {
				t.Fatalf("durable result lost after epoch refresh: durable=%v err=%v", durable, err)
			}
			if err := next.PublishResult(context.Background(), pkg, result); err != nil {
				t.Fatalf("publication after epoch refresh: %v", err)
			}
			after, err := os.ReadFile(resultReceiptPath(t, next, directory))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("immutable receipt changed after epoch refresh: %v", err)
			}
		})
	}
}

// Evidence from a future coordinator epoch is still refused by an older one,
// and a package newer than the store's own authority is refused before any
// receipt is read.
func TestResultDurableStillRefusesFutureAuthority(t *testing.T) {
	root := t.TempDir()
	store := testCustodyStore(t, root, func() time.Time { return runtimeTestNow })
	future := reopenCustodyAtEpoch(t, store, store.config.CoordinatorEpoch+1)
	pkg := testPackage()
	pkg.CoordinatorEpoch = future.config.CoordinatorEpoch
	if err := future.PublishResult(context.Background(), pkg, PublishedResult{FinalMessage: "done", ThreadArchive: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	older := testPackage()
	if durable, err := store.ResultDurable(older); err == nil || durable {
		t.Fatalf("older coordinator accepted a future-epoch receipt: durable=%v err=%v", durable, err)
	}
	if durable, err := store.ResultDurable(pkg); err == nil || durable {
		t.Fatalf("store accepted a package from a future epoch: durable=%v err=%v", durable, err)
	}
}

// A worker that published its result and restarted before recording the
// collection, under a coordinator that has since moved to a new epoch and
// replayed the same assignment, completes from the result it already holds:
// no second verification, and the receipt is unchanged.
func TestCollectionReplayCompletesFromDurableResultAfterCoordinatorEpochRefresh(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(fmt.Sprintf("acknowledged=%v", acknowledged), func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatal(err)
			}
			directory := "outbox"
			if acknowledged {
				if err := f.custody.AcknowledgeUpload("upload-assignment-1-result"); err != nil {
					t.Fatal(err)
				}
				directory = "acknowledged"
			}
			before, err := os.ReadFile(resultReceiptPath(t, f.custody, directory))
			if err != nil {
				t.Fatal(err)
			}
			// The crash came after publication and before the journal recorded
			// the completed collection.
			if err := f.runtime.markPhase("assignment-1", PhaseCollecting, "", f.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			const epoch = 10
			f.custody = reopenCustodyAtEpoch(t, f.custody, epoch)
			f.driver.Publisher = f.custody
			journal, err := OpenJournal(filepath.Join(f.root, "journal"), "normandy", "worker-1", epoch)
			if err != nil {
				t.Fatal(err)
			}
			config := testConfig(func() time.Time { return runtimeTestNow })
			config.CoordinatorEpoch = epoch
			if f.runtime, err = New(config, journal, f.driver); err != nil {
				t.Fatal(err)
			}
			offer := testOffer(t)
			offer.Assignment.LeaseToken = "lease-2"
			f.pkg.CoordinatorEpoch = epoch
			if offer.Package, err = workerproto.BuildExecutionPackageManifest(f.pkg); err != nil {
				t.Fatal(err)
			}
			claims, err := f.runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}})
			if err != nil || len(claims.Claims) != 1 {
				t.Fatalf("same-assignment replay: claims=%+v err=%v", claims, err)
			}
			if got := f.record(t).Package.Package.CoordinatorEpoch; got != epoch {
				t.Fatalf("replay left the package at coordinator epoch %d", got)
			}
			if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatalf("collection replay after epoch refresh: %v", err)
			}
			if got := f.record(t).Phase; got != PhaseCompleted || f.process.calls != 1 {
				t.Fatalf("phase=%s verifications=%d; want completed from the durable result", got, f.process.calls)
			}
			after, err := os.ReadFile(resultReceiptPath(t, f.custody, directory))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("immutable receipt changed: %v", err)
			}
		})
	}
}
