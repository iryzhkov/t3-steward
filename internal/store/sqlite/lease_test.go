package sqlite

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func leaseFixture(t *testing.T) (*Store, *time.Time) {
	t.Helper()
	s, err := openMigratedFixture(filepath.Join(t.TempDir(), "leases.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	return s, &now
}

func TestLeaseLifecycleFencingAndAudit(t *testing.T) {
	s, now := leaseFixture(t)
	ctx := context.Background()
	n := 0
	call := func(action, owner string, token int64, force bool) domain.LeaseResponse {
		t.Helper()
		n++
		r, err := s.ExecuteLease(ctx, domain.LeaseRequest{Action: action, Name: "repo:Steward/main", OwnerThread: owner, Principal: "admin", Reason: "integrate", Plan: "jocasta:plan@1", RequestID: fmtLeaseID(n), TTL: time.Hour, Token: token, Force: force})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	a := call("acquire", "thread-a", 0, false)
	if a.Code != 0 || a.Lease.Token != 1 || a.Lease.Principal != "admin" {
		t.Fatalf("acquire: %+v", a)
	}
	again := call("acquire", "thread-a", 0, false)
	if again.Lease.Token != 1 || !again.Lease.ExpiresAt.Equal(a.Lease.ExpiresAt) {
		t.Fatal("reacquire extended lease")
	}
	other := call("acquire", "thread-b", 0, false)
	if other.Code != 10 || other.Lease.OwnerThread != "thread-a" || other.Message == "" {
		t.Fatalf("other: %+v", other)
	}
	for _, action := range []string{"renew", "release"} {
		if call(action, "thread-b", 1, false).Code != 10 {
			t.Fatal("wrong holder accepted")
		}
		if call(action, "thread-a", 2, false).Code != 10 {
			t.Fatal("wrong token accepted")
		}
	}
	*now = now.Add(10 * time.Minute)
	renewed := call("renew", "thread-a", 1, false)
	if renewed.Code != 0 || renewed.Lease.Token != 1 || !renewed.Lease.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("renew: %+v", renewed)
	}
	if call("release", "thread-a", 1, false).Code != 0 {
		t.Fatal("release")
	}
	if call("check", "thread-a", 0, false).Code != 11 {
		t.Fatal("released still live")
	}
	b := call("acquire", "thread-b", 0, false)
	if b.Lease.Token != 2 {
		t.Fatal("release reused token")
	}
	forced := call("release", "thread-c", 0, true)
	if forced.Code != 0 || !forced.Lease.Released {
		t.Fatal("forced release")
	}
	var audit string
	if err := s.db.QueryRow("SELECT record FROM coordinator_lease_receipts WHERE request_id = ?", fmtLeaseID(n)).Scan(&audit); err != nil {
		t.Fatal(err)
	}
	if !stringsLeaseContains(audit, "thread-c") || !stringsLeaseContains(audit, "admin") {
		t.Fatal("forcing identity not audited")
	}
	c := call("acquire", "thread-a", 0, false)
	*now = c.Lease.ExpiresAt
	if call("renew", "thread-a", c.Lease.Token, false).Code != 10 {
		t.Fatal("expired renewed")
	}
	if call("release", "thread-a", c.Lease.Token, false).Code != 10 {
		t.Fatal("expired released")
	}
	if call("check", "thread-a", 0, false).Code != 11 {
		t.Fatal("expired live")
	}
	d := call("acquire", "thread-b", 0, false)
	if d.Lease.Token != 4 {
		t.Fatalf("expiry token: %+v", d)
	}
	if call("check", "thread-b", 0, false).Code != 0 || call("check", "thread-a", 0, false).Code != 10 {
		t.Fatal("check verdict")
	}
}
func fmtLeaseID(n int) string               { return fmt.Sprintf("request-%d", n) }
func stringsLeaseContains(s, v string) bool { return strings.Contains(s, v) }

func TestLeaseReceiptsPersistReplayAndRefusal(t *testing.T) {
	s, now := leaseFixture(t)
	ctx := context.Background()
	req := domain.LeaseRequest{Action: "acquire", Name: "release:upkeeper/manifest", OwnerThread: "a", Principal: "admin", Reason: "release", RequestID: "one", TTL: time.Hour}
	first, err := s.ExecuteLease(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Hour)
	replay, err := s.ExecuteLease(ctx, req)
	if err != nil || !replay.Replay || replay.Lease.Token != first.Lease.Token || !replay.Lease.ExpiresAt.Equal(first.Lease.ExpiresAt) {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	req.Reason = "changed"
	if _, err = s.ExecuteLease(ctx, req); err == nil {
		t.Fatal("digest conflict accepted")
	}
	req.Reason = "release"
	req.RequestID = "two"
	if _, err = s.ExecuteLease(ctx, req); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "refusal"
	req.OwnerThread = "b"
	refused, err := s.ExecuteLease(ctx, req)
	if err != nil || refused.Code != 10 {
		t.Fatalf("refusal %v %v", refused, err)
	}
	*now = now.Add(2 * time.Hour)
	replay, err = s.ExecuteLease(ctx, req)
	if err != nil || replay.Code != 10 || !replay.Replay {
		t.Fatalf("refusal replay %+v %v", replay, err)
	}
	path := s.path
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replay, err = reopened.ExecuteLease(ctx, req)
	if err != nil || replay.Code != 10 || !replay.Replay {
		t.Fatalf("restart replay %+v %v", replay, err)
	}
}

func TestLeaseConcurrentAcquisition(t *testing.T) {
	s, _ := leaseFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan domain.LeaseResponse, 2)
	errs := make(chan error, 2)
	for _, owner := range []string{"a", "b"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			r, e := s.ExecuteLease(ctx, domain.LeaseRequest{Action: "acquire", Name: "repo:steward/main", OwnerThread: owner, Principal: "admin", Reason: "race", RequestID: owner, TTL: time.Hour})
			results <- r
			errs <- e
		}(owner)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	successes, refusals := 0, 0
	for r := range results {
		if r.Code == 0 {
			successes++
		}
		if r.Code == 10 {
			refusals++
		}
	}
	if successes != 1 || refusals != 1 {
		t.Fatalf("success=%d refusal=%d", successes, refusals)
	}
}

func TestLeaseMigrationV42FromBaseAndFresh(t *testing.T) {
	for _, base := range []bool{false, true} {
		t.Run(fmt.Sprint(base), func(t *testing.T) {
			s, err := open(filepath.Join(t.TempDir(), "state.db"), true)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if base {
				if err = s.migrateThrough(37); err != nil {
					t.Fatal(err)
				}
			}
			if err = s.Migrate(); err != nil {
				t.Fatal(err)
			}
			var version int
			if err = s.db.QueryRow("SELECT MAX(version) FROM schema_version").Scan(&version); err != nil || version != 42 {
				t.Fatalf("version %d %v", version, err)
			}
			r, err := s.ExecuteLease(context.Background(), domain.LeaseRequest{Action: "acquire", Name: "repo:steward/main", OwnerThread: "a", Principal: "admin", Reason: "test", RequestID: "one"})
			if err != nil || r.Code != 0 {
				t.Fatalf("migration acquire %+v %v", r, err)
			}
		})
	}
}

func TestLeaseTokenOverflowFailsClosed(t *testing.T) {
	s, _ := leaseFixture(t)
	raw := fmt.Sprintf("{\"name\":\"repo:s/main\",\"token\":%d,\"released\":true}", int64(math.MaxInt64))
	if _, err := s.db.Exec("INSERT INTO coordinator_leases(name,record) VALUES(?,?)", "repo:s/main", raw); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecuteLease(context.Background(), domain.LeaseRequest{Action: "acquire", Name: "repo:s/main", OwnerThread: "a", Principal: "admin", Reason: "test", RequestID: "one"}); err == nil {
		t.Fatal("token wrapped")
	}
}
