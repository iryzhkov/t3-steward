package main

import (
	"context"
	"errors"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A rejected wake is over: nothing will reach the thread through it. A wait
// that was rejected before it settled must not be listed as still waiting.
func TestARejectedWaitIsListedAsOver(t *testing.T) {
	row := nodeWaitRow(domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread"}, Delivery: "rejected"})
	if !row.Settled {
		t.Fatalf("a rejected wait is listed as live: %+v", row)
	}
}

type countingRejecter struct{ calls int }

func (r *countingRejecter) RejectThreadWakes(context.Context, string) (int, error) {
	r.calls++
	return 1, errors.New("must not be called")
}

// A dry-run archive deletes nothing, so it must end no wake either, whatever
// its control answered to the delete.
func TestADryRunArchiveRejectsNoWake(t *testing.T) {
	rejecter := &countingRejecter{}
	if err := rejectDeletedThreadWakes(context.Background(), true, rejecter, nil, "thread"); err != nil {
		t.Fatal(err)
	}
	if rejecter.calls != 0 {
		t.Fatalf("a dry-run archive ended wakes (%d calls)", rejecter.calls)
	}
	if err := rejectDeletedThreadWakes(context.Background(), false, rejecter, nil, "thread"); err == nil || rejecter.calls != 1 {
		t.Fatalf("a real delete did not end the wakes: calls=%d err=%v", rejecter.calls, err)
	}
}
