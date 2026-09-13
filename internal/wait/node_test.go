package wait

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type nativeMemory struct {
	w       domain.NodeWait
	settles int
}

func (s *nativeMemory) SaveWait(context.Context, Wait) error                    { return nil }
func (s *nativeMemory) ListWaits(context.Context, string) ([]Wait, error)       { return nil, nil }
func (s *nativeMemory) RecordAction(context.Context, domain.ActionRecord) error { return nil }
func (s *nativeMemory) SettleNodeWaits(context.Context, time.Time) error        { s.settles++; return nil }
func (s *nativeMemory) ListNodeWaits(context.Context) ([]domain.NodeWait, error) {
	return []domain.NodeWait{s.w}, nil
}
func (s *nativeMemory) TransitionNodeWake(_ context.Context, id, from, to string, _ time.Time) (bool, error) {
	if s.w.Request.ID != id || s.w.Delivery != from {
		return false, nil
	}
	s.w.Delivery = to
	return true, nil
}

type nativeControl struct {
	sends int
	seen  bool
	fail  bool
}

func (c *nativeControl) GetThread(context.Context, string) (*domain.Thread, error) {
	return &domain.Thread{ID: "thread"}, nil
}
func (c *nativeControl) ResumeThread(context.Context, domain.Thread, string) error {
	panic("native wake used unsafe legacy send")
}
func (c *nativeControl) ObserveNodeWake(context.Context, string, string) (bool, error) {
	return c.seen, nil
}
func (c *nativeControl) SendNodeWake(context.Context, domain.Thread, string, string) error {
	c.sends++
	if c.fail {
		return errors.New("response lost")
	}
	return nil
}
func TestNativeWakeHeldRestartLostResponseAndObservation(t *testing.T) {
	now := time.Now()
	store := &nativeMemory{w: domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw-test", ThreadID: "thread"}, Host: "host", SettledAt: &now, Observation: &domain.NodeObservation{ExitCode: 0}, DeliveryID: "token", Delivery: "pending"}}
	control := &nativeControl{fail: true}
	runner := New(store, control, slog.New(slog.NewTextHandler(io.Discard, nil)))
	runner.NodeHost = "host"
	runner.NodeDryRun = true
	runner.Tick(context.Background(), nil, nil)
	runner.Tick(context.Background(), nil, nil)
	if store.w.Delivery != "held" || control.sends != 0 {
		t.Fatal(store.w, control.sends)
	}
	// Recreate the process and turn only native delivery on.
	runner = New(store, control, nil)
	runner.NodeHost = "host"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 || store.w.Delivery != "recovery-required" {
		t.Fatal(control.sends, store.w.Delivery)
	}
	runner = New(store, control, nil)
	runner.NodeHost = "host"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 || store.w.Delivery != "recovery-required" {
		t.Fatal("ambiguous absence rearmed send")
	}
	control.seen = true
	runner.Tick(context.Background(), nil, nil)
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 || store.w.Delivery != "delivered" {
		t.Fatal(control.sends, store.w.Delivery)
	}
}
func TestNativeWakeWrongHostNeverDispatches(t *testing.T) {
	now := time.Now()
	store := &nativeMemory{w: domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw", ThreadID: "thread"}, Host: "other", SettledAt: &now, Observation: &domain.NodeObservation{ExitCode: 0}, Delivery: "pending"}}
	control := &nativeControl{}
	runner := New(store, control, nil)
	runner.NodeHost = "here"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 0 {
		t.Fatal("woke a different host's thread")
	}
}
