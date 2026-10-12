package backlog

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestFleetCoordinatorSnapshotRenewalTelemetryFreshness(t *testing.T) {
	for _, interval := range []time.Duration{20 * time.Second, 30 * time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			store, db := snapshotChangeStore(t)
			snapshot, _ := snapshotChangeClaimed(t, store)
			// A prior renewal reply arrives one second after its exchange.
			snapshot = snapshotChangeAt(snapshot, snapshot.Sequence+1, time.Second)
			snapshotChangeCheck(t, store.SaveWorkerSnapshot(context.Background(), snapshot))
			now := coordinatorTestTime
			coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return now }, LeaseRenewInterval: interval}
			previous := snapshotChangeRead(t, db, snapshot.WorkerID)
			sequence := snapshot.Sequence
			for tick := 1; tick <= 18; tick++ {
				offset := time.Duration(tick) * 10 * time.Second
				now = coordinatorTestTime.Add(offset)
				sequence++
				incoming := snapshotChangeAt(snapshot, sequence, offset)
				reply := snapshotChangeAt(incoming, sequence+1, offset+time.Second)
				// Renewal replies carry no resource observation.
				reply.Inventory.Telemetry = nil
				report, transport := snapshotChangeExchange(t, coordinator, incoming, &reply)
				row := snapshotChangeRead(t, db, snapshot.WorkerID)
				var stored domain.WorkerSnapshot
				snapshotChangeCheck(t, json.Unmarshal([]byte(row.record), &stored))
				if stored.Inventory.Telemetry == nil {
					t.Fatal("stored telemetry missing")
				}
				age := now.Sub(stored.Inventory.Telemetry.ObservedAt)
				if age > 30*time.Second {
					t.Fatalf("tick %s: stored telemetry age %s exceeds half the snapshot validity", offset, age)
				}
				if row.audits != previous.audits {
					t.Fatalf("unchanged exchange added worker audit: got %d want %d", row.audits, previous.audits)
				}
				if len(report.Renewed) != 0 {
					if len(transport.renewals) != 1 || transport.renewals[0].WorkerSequence != snapshot.Sequence {
						t.Fatalf("renewal did not consume stored sequence %d: %+v", snapshot.Sequence, transport.renewals)
					}
					if stored.Sequence != reply.Sequence || !stored.ObservedAt.Equal(reply.ObservedAt) {
						t.Fatalf("renewal reply was not persisted: %+v", stored)
					}
					snapshotChangeAssertSnapshot(t, row, report.Snapshot)
					if row.writes != previous.writes+1 || row.renewals != previous.renewals+1 {
						t.Fatalf("renewal writes: got %+v, previous %+v", row, previous)
					}
					sequence++
				} else if row != previous {
					t.Fatalf("idle exchange changed stored row: got %+v, previous %+v", row, previous)
				}
				snapshot = stored
				previous = row
			}
		})
	}
}
