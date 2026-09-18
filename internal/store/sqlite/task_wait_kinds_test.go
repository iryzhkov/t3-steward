package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The coordinator's expiry path records --or-timeout the same way the local
// runner does: outcome timed-out, exit 0, no expiry contradiction recorded,
// and the wake trailer says or-timeout=true. Without the flag, expiry is
// today's structured failure.
func TestExpireTaskWaitsHonoursOrTimeout(t *testing.T) {
	for _, orTimeout := range []bool{true, false} {
		ctx := context.Background()
		store, attempt, now := taskWaitFixture(t)
		registration := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
		registration.Kind = domain.WaitKindTime
		registration.Condition = "time at " + now.Add(2*time.Hour).UTC().Format(time.RFC3339)
		registration.OrTimeout = orTimeout
		wait, err := store.RegisterTaskWait(ctx, registration, now)
		if err != nil {
			t.Fatal(err)
		}
		if wait.Kind != domain.WaitKindTime || wait.OrTimeout != orTimeout {
			t.Fatalf("registered = %+v", wait)
		}
		expired, err := store.ExpireTaskWaits(ctx, now.Add(2*time.Hour))
		if err != nil || len(expired) != 1 {
			t.Fatalf("expired=%v err=%v", expired, err)
		}
		result := expired[0].Result
		if result == nil || result.Outcome != domain.TaskWaitTimedOut {
			t.Fatalf("or-timeout=%v: result = %+v", orTimeout, result)
		}
		if (result.ExitCode == 0) != orTimeout {
			t.Fatalf("or-timeout=%v: exit = %d", orTimeout, result.ExitCode)
		}
		events, err := store.ListTaskWaitReconciliations(ctx)
		if err != nil {
			t.Fatal(err)
		}
		recorded := false
		for _, event := range events {
			if event.Kind == domain.TaskWaitReconciliationExpired && event.WaitID == wait.ID {
				recorded = true
			}
		}
		if recorded == orTimeout {
			t.Fatalf("or-timeout=%v: expiry contradiction recorded=%v", orTimeout, recorded)
		}
	}
}
