package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestLeaseRenewReleaseReplayAndReadPurity(t *testing.T) {
	s, now := leaseFixture(t)
	ctx := context.Background()
	req := domain.LeaseRequest{Action: "acquire", Name: "repo:s/main", OwnerThread: "a", Principal: "admin", Reason: "work", RequestID: "acq"}
	a, err := s.ExecuteLease(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Lease.ExpiresAt.Equal(now.Add(2 * time.Hour)) {
		t.Fatal("default TTL")
	}
	req.Action = "renew"
	req.Token = a.Lease.Token
	req.RequestID = "renew"
	req.TTL = time.Hour
	r, err := s.ExecuteLease(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(10 * time.Minute)
	replay, err := s.ExecuteLease(ctx, req)
	if err != nil || !replay.Replay || !replay.Lease.ExpiresAt.Equal(r.Lease.ExpiresAt) {
		t.Fatalf("renew replay %+v %v", replay, err)
	}
	req.Action = "release"
	req.RequestID = "release"
	r, err = s.ExecuteLease(ctx, req)
	if err != nil || !r.Lease.Released {
		t.Fatalf("release %+v %v", r, err)
	}
	req.Action = "acquire"
	req.RequestID = "next"
	req.OwnerThread = "b"
	newer, err := s.ExecuteLease(ctx, req)
	if err != nil || newer.Lease.Token != 2 {
		t.Fatalf("next %+v %v", newer, err)
	}
	req.Action = "release"
	req.RequestID = "release"
	req.OwnerThread = "a"
	replay, err = s.ExecuteLease(ctx, req)
	if err != nil || !replay.Replay || replay.Lease.Token != 1 || !replay.Lease.Released {
		t.Fatalf("release replay %+v %v", replay, err)
	}
	for _, action := range []string{"check", "show", "list"} {
		read := domain.LeaseRequest{Action: action, Name: "repo:s/main", OwnerThread: "b"}
		if action == "list" {
			read.Name = ""
		}
		response, err := s.ExecuteLease(ctx, read)
		if err != nil || response.Code != 0 {
			t.Fatalf("read %+v %v", response, err)
		}
	}
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM coordinator_lease_receipts").Scan(&count); err != nil || count != 4 {
		t.Fatalf("reads wrote receipts %d %v", count, err)
	}
	live, err := s.ExecuteLease(ctx, domain.LeaseRequest{Action: "check", Name: "repo:s/main", OwnerThread: "b"})
	if err != nil || live.Lease.Token != 2 || live.Code != 0 {
		t.Fatal("old release replay affected new holder")
	}
}
