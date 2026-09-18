package wait

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// cancelledStore answers every call with the context's cancellation, as the
// SQLite store does once the daemon's context is cancelled at shutdown.
type cancelledStore struct{}

func (cancelledStore) SaveWait(ctx context.Context, _ Wait) error { return ctx.Err() }
func (cancelledStore) ListWaits(ctx context.Context, _ string) ([]Wait, error) {
	return nil, ctx.Err()
}
func (cancelledStore) RecordAction(ctx context.Context, _ domain.ActionRecord) error {
	return ctx.Err()
}
func (cancelledStore) SettleNodeWaits(ctx context.Context, _ time.Time) error { return ctx.Err() }
func (cancelledStore) ListNodeWaits(ctx context.Context) ([]domain.NodeWait, error) {
	return nil, ctx.Err()
}
func (cancelledStore) TransitionNodeWake(ctx context.Context, _, _, _ string, _ time.Time) (bool, error) {
	return false, ctx.Err()
}
func (cancelledStore) PendingSupervisionEscalations(ctx context.Context) ([]domain.SupervisionEscalationDelivery, error) {
	return nil, ctx.Err()
}
func (cancelledStore) TransitionSupervisionOutboxRow(ctx context.Context, _, _, _ string, _ time.Time) (bool, error) {
	return false, ctx.Err()
}
func (cancelledStore) SettleTaskWait(ctx context.Context, _ string, _ domain.TaskWaitResult, _ time.Time) (domain.TaskWait, error) {
	return domain.TaskWait{}, ctx.Err()
}
func (cancelledStore) ExpireTaskWaits(ctx context.Context, _ time.Time) ([]domain.TaskWait, error) {
	return nil, ctx.Err()
}
func (cancelledStore) WakeTaskWaits(ctx context.Context, _ time.Time) ([]domain.TaskWaitWakeContext, error) {
	return nil, ctx.Err()
}
func (cancelledStore) TaskWakesAwaitingDelivery(ctx context.Context, _ time.Time) ([]domain.TaskWaitWakeContext, error) {
	return nil, ctx.Err()
}
func (cancelledStore) TransitionTaskWake(ctx context.Context, _, _, _ string, _ time.Time) (bool, error) {
	return false, ctx.Err()
}

type cancelledControl struct{}

func (cancelledControl) GetThread(ctx context.Context, _ string) (*domain.Thread, error) {
	return nil, ctx.Err()
}
func (cancelledControl) ResumeThread(ctx context.Context, _ domain.Thread, _ string) error {
	return ctx.Err()
}
func (cancelledControl) ObserveNodeWake(ctx context.Context, _, _ string) (bool, error) {
	return false, ctx.Err()
}
func (cancelledControl) SendNodeWake(ctx context.Context, _ domain.Thread, _, _ string) error {
	return ctx.Err()
}

// recordingHandler keeps every record's level and message.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// F-11: a shutdown cancels the daemon's context while a tick is in flight, and
// every store call then returns "context canceled". Those are not operational
// faults and must not be logged at ERROR; the coordinator's boundary cycle
// already reports them at INFO as shutdown.
func TestTickLogsCancelledContextAsShutdownNotError(t *testing.T) {
	handler := &recordingHandler{}
	runner := New(cancelledStore{}, cancelledControl{}, slog.New(handler))
	runner.NodeHost = "host"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Each loop is exercised on its own so that an early return in Tick cannot
	// hide a site that still logs at ERROR.
	runner.tickNodes(ctx)
	runner.tickSupervisionEscalations(ctx)
	runner.tickTaskWaits(ctx, nil)
	runner.Tick(ctx, nil, nil)

	var shutdown int
	for _, r := range handler.records {
		if r.Level >= slog.LevelError {
			t.Errorf("cancelled context logged at %s: %s", r.Level, r.Message)
		}
		if r.Level == slog.LevelInfo && strings.HasPrefix(r.Message, "shutting down: ") {
			shutdown++
		}
	}
	if shutdown < 4 {
		t.Errorf("expected one INFO shutdown record per loop, got %d", shutdown)
	}
}

func TestLogFailureKeepsOtherErrorsAtError(t *testing.T) {
	handler := &recordingHandler{}
	logFailure(context.Background(), slog.New(handler), "settle task-bound wait", context.DeadlineExceeded, "wait", "w1")
	if len(handler.records) != 1 || handler.records[0].Level != slog.LevelError || handler.records[0].Message != "settle task-bound wait" {
		t.Fatalf("unexpected records %+v", handler.records)
	}
}
