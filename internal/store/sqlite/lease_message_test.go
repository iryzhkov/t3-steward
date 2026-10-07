package sqlite

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

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
}
