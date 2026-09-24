package workerruntime

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestNativeUserInputDefersTaskCollectionUntilAnswered(t *testing.T) {
	pkg := testPackage()
	control := &recordingT3{thread: &domain.Thread{
		ID: pkg.Identity.ThreadID, TurnID: "turn-1", TurnState: "completed",
		HasPendingUserInput: true,
	}}
	driver := &LocalDriver{T3: control}
	observed, err := driver.ObserveThread(context.Background(), pkg)
	if err != nil || observed != backlog.DispatchThreadActive {
		t.Fatalf("pending input observation: state=%q err=%v", observed, err)
	}
	state, turnID, err := driver.ObserveThreadTurn(context.Background(), pkg)
	if err != nil || state != backlog.DispatchThreadActive || turnID != "turn-1" {
		t.Fatalf("pending input: state=%q turn=%q err=%v", state, turnID, err)
	}
	control.thread.HasPendingUserInput = false
	state, turnID, err = driver.ObserveThreadTurn(context.Background(), pkg)
	if err != nil || state != backlog.DispatchThreadStopped || turnID != "turn-1" {
		t.Fatalf("resolved input: state=%q turn=%q err=%v", state, turnID, err)
	}
}
