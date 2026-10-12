package wait

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type unsupportedGroupStore struct {
	nativeGroupMemory
	claims int
}

func (s *unsupportedGroupStore) ClaimNodeWakeGroup(context.Context, string, string, string, string, []string, time.Time) (bool, error) {
	s.claims++
	return false, ErrNodeGroupClaimUnsupported
}

func TestOlderCoordinatorRefusingTheGroupClaimFallsBack(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		now := time.Now()
		store := &unsupportedGroupStore{}
		for _, id := range []string{"a", "b", "c"} {
			request := domain.NodeWaitRequest{ID: id, ThreadID: "thread", Target: domain.NodeRef{RunID: "r", TaskID: id}}
			if grouped {
				request.Group = "all"
				request.Wake = domain.WakeAll
			}
			store.waits = append(store.waits, domain.NodeWait{Request: request, Host: "host", CreatedAt: now, SettledAt: &now,
				Observation: &domain.NodeObservation{Progress: domain.ProgressSucceeded, ExitCode: 0}, Delivery: "pending", DeliveryID: id})
		}
		control := &nativeControl{}
		runner := New(store, control, nil)
		runner.NodeHost = "host"
		runner.tickNodes(context.Background())
		want := 3
		if grouped {
			want = 1
		}
		if store.claims != want || control.sends != want {
			t.Fatal(grouped, store.claims, control.sends)
		}
		for i, w := range store.waits {
			delivery := "sending"
			if grouped && i > 0 {
				delivery = "delivered"
			}
			if w.Delivery != delivery {
				t.Fatal(grouped, w)
			}
		}
	}
}
