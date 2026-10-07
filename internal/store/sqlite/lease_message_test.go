package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Self-review of fix round 1: a renew or release by the wrong holder said
// "refused:" twice.
func TestLeaseMismatchMessageSaysRefusedOnce(t *testing.T) {
	s, _ := leaseFixture(t)
	ctx := context.Background()
	if r, err := s.ExecuteLease(ctx, domain.LeaseRequest{Action: "acquire", Name: "repo:x/main", OwnerThread: "a", Principal: "admin", Reason: "r", RequestID: "one", TTL: time.Hour}); err != nil || r.Code != 0 {
		t.Fatalf("acquire: %+v %v", r, err)
	}
	for i, action := range []string{"renew", "release"} {
		r, err := s.ExecuteLease(ctx, domain.LeaseRequest{Action: action, Name: "repo:x/main", OwnerThread: "b", Principal: "admin", RequestID: fmtLeaseID(i + 10), TTL: time.Hour, Token: 1})
		if err != nil || r.Code != 10 || strings.Count(r.Message, "refused:") != 1 || !strings.Contains(r.Message, "holder or fencing token does not match") || !strings.Contains(r.Message, "held by thread a") {
			t.Fatalf("%s: %+v %v", action, r, err)
		}
	}
}

func TestHeldLeaseMessageOmitsEmptyPlan(t *testing.T) {
	expires := time.Date(2026, 10, 7, 5, 10, 0, 0, time.UTC)
	lease := domain.Lease{Name: "repo:x/main", OwnerThread: "thread-a", Reason: "integrate", ExpiresAt: expires}
	got := heldLeaseMessage(lease)
	want := `refused: repo:x/main is held by thread thread-a ("integrate") until 2026-10-07T05:10:00Z`
	if got != want {
		t.Fatalf("without plan:\n got %s\nwant %s", got, want)
	}
	lease.Plan = "jocasta:abc@3"
	got = heldLeaseMessage(lease)
	want = `refused: repo:x/main is held by thread thread-a (plan jocasta:abc@3, "integrate") until 2026-10-07T05:10:00Z`
	if got != want {
		t.Fatalf("with plan:\n got %s\nwant %s", got, want)
	}
	if strings.Contains(heldLeaseMessage(domain.Lease{Name: "release:x", OwnerThread: "t", Reason: "r"}), "plan ,") {
		t.Fatal("empty plan rendered")
	}
	// A plan carrying quotes or separators is quoted, so a holder cannot forge
	// the reason field of the refusal other threads read.
	lease.Plan = `p", "forged`
	got = heldLeaseMessage(lease)
	want = `refused: repo:x/main is held by thread thread-a (plan "p\", \"forged", "integrate") until 2026-10-07T05:10:00Z`
	if got != want {
		t.Fatalf("forgeable plan:\n got %s\nwant %s", got, want)
	}
}
