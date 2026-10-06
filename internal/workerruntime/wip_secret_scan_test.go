package workerruntime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A failed result's work-in-progress bundle passes through the result secret
// scan like any other bundle. The scan decodes it against the attempt
// workspace, so a failure collected without a T3 outcome still uploads a clean
// bundle, and a bundle whose uncommitted work holds an execution credential
// stays on the worker while the failure itself publishes with its reason.
func TestFailedResultWorkInProgressBundlePassesTheSecretScan(t *testing.T) {
	secret := "synthetic-wip-bundle-credential-0123456789"
	for _, tc := range []struct {
		name     string
		content  string
		uploaded bool
	}{
		{"clean work is uploaded", "new\n", true},
		{"work holding a credential stays on the worker", "token=" + secret + "\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWIPFixture(t, commitOutputs)
			writeTestFile(t, filepath.Join(f.workspace, "c.txt"), tc.content)
			custody := &capturingCustody{CustodyStore: testCustodyStore(t, filepath.Join(t.TempDir(), "custody"), func() time.Time { return runtimeTestNow })}
			custody.config.SecretScan.StaticCanaries = []string{secret}
			f.driver.Publisher = custody
			f.driver.T3 = &recordingT3{thread: &domain.Thread{ID: "thread-1", TurnID: "turn-3", TurnState: "completed"}, archive: []byte("{}")}
			const reason = "preparation failed 3 times"
			if err := f.driver.CollectFailure(context.Background(), f.pkg, f.workspace, reason); err != nil {
				t.Fatal(err)
			}
			if found := uploadsWorkInProgressBundle(t, custody.CustodyStore); found != tc.uploaded {
				t.Fatalf("bundle uploaded = %v, want %v", found, tc.uploaded)
			}
			last := custody.results[len(custody.results)-1]
			if failure := last.Finalized.Completion.Failure; !strings.HasPrefix(failure, reason) {
				t.Fatalf("the failure reason was not published: %q", failure)
			}
			if !tc.uploaded && len(last.WorkInProgressBundle) != 0 {
				t.Fatal("the published result carries the refused bundle")
			}
		})
	}
}
