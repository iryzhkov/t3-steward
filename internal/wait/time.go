package wait

import (
	"context"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// minTimeInterval is the shortest poll interval of a time wait, and the bound
// on how late after the instant the wake can be observed by the runner.
const minTimeInterval = 30 * time.Second

func init() {
	kindRunners[domain.WaitKindTime] = (*Runner).runTimeOnce
	kindConditions[domain.WaitKindTime] = func(w Wait) string {
		if w.At == nil {
			return "Time: (no instant)"
		}
		return "Time: at " + w.At.UTC().Format(time.RFC3339)
	}
}

// TimeInterval is the poll interval of a time wait with the given time left:
// the backoff ceiling while the instant is far, then the remaining time
// itself so the last poll lands on the instant, never below 30 s.
func TimeInterval(remaining, ceiling time.Duration) time.Duration {
	if ceiling <= 0 {
		ceiling = 10 * time.Minute
	}
	switch {
	case remaining < minTimeInterval:
		return minTimeInterval
	case remaining > ceiling:
		return ceiling
	default:
		return remaining
	}
}

// runTimeOnce is one poll of a time wait: "not yet" until the instant, with
// the next poll scheduled from the remaining time, then met with the instant
// in the trailer. runOnce records the poll and saves the row.
func (*Runner) runTimeOnce(_ context.Context, w *Wait, now time.Time) {
	if w.At == nil {
		w.LastExit = 2
		w.settle(StatusFailed, "the time wait has no instant", now, nil)
		return
	}
	if now.Before(*w.At) {
		w.LastExit = 1
		w.Interval = TimeInterval(w.At.Sub(now), w.maxInterval())
		return
	}
	w.LastExit = 0
	w.settle(StatusMet, "the instant passed", now, map[string]string{"at": w.At.UTC().Format(time.RFC3339)})
}
