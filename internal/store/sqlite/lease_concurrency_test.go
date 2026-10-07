package sqlite

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
)

func TestLeaseConcurrentConnectionsAndAtomicReceipt(t *testing.T) {
	first, _ := leaseFixture(t)
	second, err := Open(first.path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.now = first.now
	results := make(chan domain.LeaseResponse, 2)
	errs := make(chan error, 2)
	start := make(chan struct{})
	for i, s := range []*Store{first, second} {
		go func(i int, s *Store) {
			<-start
			r, e := s.ExecuteLease(context.Background(), domain.LeaseRequest{Action: "acquire", Name: "repo:s/main", OwnerThread: fmt.Sprint(i), Principal: "admin", Reason: "race", RequestID: fmt.Sprint(i)})
			results <- r
			errs <- e
		}(i, s)
	}
	close(start)
	wins := 0
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if (<-results).Code == 0 {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("winners %d", wins)
	}
	var count int
	if err = first.db.QueryRow("SELECT count(*) FROM coordinator_lease_receipts").Scan(&count); err != nil || count != 2 {
		t.Fatalf("receipts %d %v", count, err)
	}
	// If the durable receipt cannot be inserted, the holder transition must
	// roll back too, permitting a retry to acquire token 1.
	if _, err = first.db.Exec("CREATE TRIGGER fail_lease_receipt BEFORE INSERT ON coordinator_lease_receipts BEGIN SELECT RAISE(ABORT,'test crash'); END;"); err != nil {
		t.Fatal(err)
	}
	req := domain.LeaseRequest{Action: "acquire", Name: "release:s/manifest", OwnerThread: "a", Principal: "admin", Reason: "release", RequestID: "crash"}
	if _, err = first.ExecuteLease(context.Background(), req); err == nil {
		t.Fatal("receipt failure accepted")
	}
	var record string
	if err = first.db.QueryRow("SELECT record FROM coordinator_leases WHERE name=?", req.Name).Scan(&record); err == nil {
		t.Fatal("holder survived failed receipt")
	}
	if _, err = first.db.Exec("DROP TRIGGER fail_lease_receipt"); err != nil {
		t.Fatal(err)
	}
	r, err := first.ExecuteLease(context.Background(), req)
	if err != nil || r.Lease.Token != 1 {
		t.Fatalf("retry %+v %v", r, err)
	}
}
