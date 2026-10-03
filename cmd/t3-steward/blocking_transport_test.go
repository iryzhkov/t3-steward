package main

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestBlockingWaitPreservesTransportTimeoutCode(t *testing.T) {
	f := newTaskResultFixture(t)
	cli := f.cli()
	cli.query = func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		return backlogadmin.Response{}, &backlogadmin.TransportError{Class: backlogadmin.ClassTimeout, Operation: "query", Err: context.DeadlineExceeded}
	}
	err := cli.run(context.Background(), []string{"run-1", "--wait", "--timeout", "1s"})
	if err == nil || exitCodeFor(err) != 6 {
		t.Fatalf("err=%v exit=%d want transport timeout 6", err, exitCodeFor(err))
	}
}
