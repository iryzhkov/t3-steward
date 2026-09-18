package wait

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// With --or-timeout the deadline is a normal outcome: the wake says
// outcome=timed-out with or-timeout=true and exit 0, and the prose calls it a
// reached deadline. Without it, today's behaviour: the same outcome as a
// failure wake with exit 2. Both a time wait and a shell wait.
func TestOrTimeoutMakesTheDeadlineANormalOutcome(t *testing.T) {
	for _, kind := range []domain.WaitKind{domain.WaitKindShell, domain.WaitKindTime} {
		for _, orTimeout := range []bool{true, false} {
			store := &memStore{waits: map[string]Wait{}}
			control := &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1"}}}
			runner := New(store, control, nil)
			now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
			runner.SetClock(func() time.Time { return now })
			runner.Exec = func(context.Context, Wait) (string, int, error) { return "not yet", 1, nil }
			at := now.Add(2 * time.Hour)
			w := Wait{
				ID: "w1", ThreadID: "t1", Name: "slow", Kind: kind, Command: []string{"check"},
				Every: 30 * time.Second, Timeout: 10 * time.Minute, Wake: WakeEach, OrTimeout: orTimeout,
				Status: StatusWaiting, CreatedAt: now, LastExit: 1,
			}
			if kind == domain.WaitKindTime {
				w.Command, w.At = nil, &at
			}
			_ = store.SaveWait(context.Background(), w)
			now = now.Add(11 * time.Minute)
			runner.Tick(context.Background(), nil, nil)
			runner.Tick(context.Background(), nil, nil)
			if len(control.texts) != 1 {
				t.Fatalf("%s or-timeout=%v: texts = %v", kind, orTimeout, control.texts)
			}
			parsed, ok := ParseWakeTrailer(control.texts[0])
			if !ok || parsed["outcome"] != "timed-out" || parsed["kind"] != string(kind) {
				t.Fatalf("%s or-timeout=%v: trailer = %v", kind, orTimeout, parsed)
			}
			if (parsed["or-timeout"] == "true") != orTimeout {
				t.Fatalf("%s or-timeout=%v: trailer = %v", kind, orTimeout, parsed)
			}
			saved := store.waits["w1"]
			// A normal outcome reads as exit 0; today's timeout keeps the last
			// not-yet exit, which is never 0.
			if (saved.LastExit == 0) != orTimeout {
				t.Fatalf("%s or-timeout=%v: last exit = %d", kind, orTimeout, saved.LastExit)
			}
			if strings.Contains(control.texts[0], "deadline reached") != orTimeout {
				t.Fatalf("%s or-timeout=%v: prose = %q", kind, orTimeout, control.texts[0])
			}
			if result := taskWaitResult(saved, now); result.Outcome != domain.TaskWaitTimedOut || (result.ExitCode == 0) != orTimeout {
				t.Fatalf("%s or-timeout=%v: coordinator result = %+v", kind, orTimeout, result)
			}
		}
	}
}
