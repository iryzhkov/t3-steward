package coordinatormetrics

import (
	"context"
	"net"
	"testing"
	"time"
)

type cancellingLoader struct {
	entered chan context.Context
}

func (l cancellingLoader) LoadMetricsSnapshot(ctx context.Context) (Snapshot, error) {
	l.entered <- ctx
	<-ctx.Done()
	return Snapshot{}, ctx.Err()
}

func TestListenerCloseCancelsPendingSnapshotAndReleasesBind(t *testing.T) {
	entered := make(chan context.Context, 1)
	listener, err := Start(context.Background(), "127.0.0.1:0", cancellingLoader{entered}, time.Minute, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	address := listener.listener.Addr().String()
	var loadCtx context.Context
	select {
	case loadCtx = <-entered:
	case <-time.After(time.Second):
		listener.Close()
		t.Fatal("initial snapshot never started")
	}
	deadline, bounded := loadCtx.Deadline()
	if !bounded || time.Until(deadline) > LoadTimeout {
		listener.Close()
		t.Fatal("native read has no bounded deadline", deadline)
	}
	closed := make(chan error, 1)
	go func() { closed <- listener.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not cancel the pending native read")
	}
	if loadCtx.Err() == nil {
		t.Fatal("read context survived listener cleanup")
	}
	conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Fatal("closed producer retained its loopback listener")
	}
}
