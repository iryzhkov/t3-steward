package workerruntime

import (
	"os"
	"path/filepath"
	"testing"
)

// The worker half of the same rule: the bytes under testdata/fleet are what
// UpKeeper's fleet component writes to a host, and this side is what reads them.
// See internal/config/fleet_projection_test.go for why a carried fixture that
// nothing executes is worth nothing.
func TestTheRenderedWorkerProjectionIsAcceptedByItsLoader(t *testing.T) {
	for host, name := range map[string]string{
		"homelab":    "projection-worker-homelab.json",
		"omarchy-pc": "projection-worker-omarchy-pc.json",
	} {
		t.Run(host, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "fleet", name))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			bootstrap, digest, err := DecodeWorkerBootstrap(raw)
			if err != nil {
				t.Fatalf("the fleet component renders a document this loader refuses: %v", err)
			}
			if bootstrap.WorkerID != host {
				t.Errorf("worker identity is %q, want %q", bootstrap.WorkerID, host)
			}
			if bootstrap.CredentialRef != "secretref:f02-protocol/"+host {
				t.Errorf("credential reference is %q", bootstrap.CredentialRef)
			}
			if digest == "" {
				t.Error("no digest was reported for the bootstrap")
			}
			// A worker credential must not satisfy the admin namespace, and the
			// projection must not be the vehicle that blurs them.
			if bootstrap.CoordinatorID == "" || bootstrap.Transport != "ssh" {
				t.Errorf("unexpected coordinator or transport: %+v", bootstrap)
			}
		})
	}
}
