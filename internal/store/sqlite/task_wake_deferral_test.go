package sqlite

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// governedWakeFixture is taskWaitFixture with the parked attempt's assignment
// governed: a provider route in quota pool "pool", a worker with slots
// executor slots, a fresh open admission and a settled wait. It returns the
// cutoffs a coordinator pass would supply.
func governedWakeFixture(t *testing.T, slots, poolLimit int) (*Store, domain.Attempt, time.Time, TaskWakeCutoffs) {
	t.Helper()
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assignment := records.Assignments[0]
	assignment.WorkerEpoch = "worker-epoch-1"
	assignment.Project = "project"
	assignment.Route = domain.ProviderRoute{ProviderInstanceID: "provider", Model: "model", QuotaPoolID: "pool"}
	assignment.ExecutorDemand = &domain.ResourceDemand{}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Assignments: []domain.Assignment{assignment},
		Workflows:   []domain.Workflow{{ID: "w", Project: "project"}},
		QuotaPools:  []domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionOpen, MaxConcurrent: poolLimit}},
	}); err != nil {
		t.Fatal(err)
	}
	auth := taskWakeAuthorizationFixture(t, store, now, "pool")
	snapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("load snapshot: count=%d err=%v", len(snapshots), err)
	}
	snapshot := snapshots[0]
	snapshot.Inventory.Allocatable = domain.AllocatableCapacity{ExecutorSlots: slots}
	writeTaskWakeSnapshotFixture(t, store, snapshot)
	if err := store.CommitQuotaAdmissionTransitions(ctx, []domain.QuotaAdmissionTransition{{
		Record: domain.QuotaAdmissionRecord{
			QuotaPoolID: "pool", Revision: 1, Admission: domain.AdmissionOpen,
			ObservedAt: now, AppliedAt: now, Reason: "fresh fixture admission",
		},
	}}); err != nil {
		t.Fatal(err)
	}
	return store, attempt, now, TaskWakeCutoffs{AdmissionValidAfter: now.Add(-time.Second), AuthorizedWorkers: auth}
}

// holdingAttempt saves a running attempt of its own task that holds one slot
// of worker and one of the fixture's quota pool.
func holdingAttempt(t *testing.T, store *Store, id, worker string, now time.Time) (domain.Attempt, domain.Assignment) {
	t.Helper()
	attempt := domain.Attempt{
		ID: id, WorkflowRunID: "r", TaskID: "task-" + id, Number: 1, AssignmentID: "assignment-" + id,
		ThreadID: "thread-" + id, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: attempt.AssignmentID, AttemptID: id, WorkerID: worker, WorkerEpoch: "worker-epoch-1", Project: "project",
		Epoch: 1, State: domain.AssignmentClaimed, ThreadID: attempt.ThreadID,
		LeaseToken: "lease-" + id, DispatchToken: "dispatch-" + id,
		Route:          domain.ProviderRoute{ProviderInstanceID: "provider", Model: "model", QuotaPoolID: "pool"},
		ExecutorDemand: &domain.ResourceDemand{}, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		Tasks:    []domain.Task{{ID: attempt.TaskID, Name: attempt.TaskID, WorkflowID: "w"}},
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	return attempt, assignment
}

func parkAndSettle(t *testing.T, store *Store, attempt domain.Attempt, request string, at time.Time) domain.TaskWait {
	t.Helper()
	ctx := context.Background()
	attempt = loadAttempt(t, store, attempt.ID)
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, request, domain.WakeEach), at)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	return wait
}

func loadTaskWait(t *testing.T, store *Store, id string) domain.TaskWait {
	t.Helper()
	waits, err := store.ListTaskWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, wait := range waits {
		if wait.ID == id {
			return wait
		}
	}
	t.Fatalf("wait %q is missing", id)
	return domain.TaskWait{}
}

// A settled wake on a full worker waits with a typed reason. It is never
// failed, the reason does not churn while the cause stands, and the wake is
// applied, and the reason cleared, once the worker has room.
func TestWakeOnFullWorkerWaitsWithTypedReason(t *testing.T) {
	ctx := context.Background()
	store, attempt, now, cutoffs := governedWakeFixture(t, 1, 5)
	holder, holderAssignment := holdingAttempt(t, store, "holder", "worker", now)
	wait := parkAndSettle(t, store, attempt, "full-worker", now)
	settled := now.Add(2 * time.Second)

	for pass := 0; pass < 2; pass++ {
		wakes, err := store.WakeTaskWaitsBefore(ctx, settled.Add(time.Duration(pass)*time.Minute), cutoffs)
		if err != nil || len(wakes) != 0 {
			t.Fatalf("pass %d: wakes=%+v err=%v, want the wake held for the full worker", pass, wakes, err)
		}
		parked := loadAttempt(t, store, attempt.ID)
		if parked.Progress != domain.ProgressWaitingExternal || parked.Control != domain.ControlWaitingExternal {
			t.Fatalf("pass %d: a deferred wake changed the attempt to %s/%s", pass, parked.Progress, parked.Control)
		}
		deferral := loadTaskWait(t, store, wait.ID).WakeDeferral
		if deferral == nil || deferral.Code != domain.WakeDeferredExecutorCapacity || deferral.WorkerID != "worker" {
			t.Fatalf("pass %d: deferral = %+v, want %s on worker", pass, deferral, domain.WakeDeferredExecutorCapacity)
		}
		if !deferral.ObservedAt.Equal(settled) {
			t.Fatalf("pass %d: deferral observed at %s, want the first deferral time %s kept", pass, deferral.ObservedAt, settled)
		}
	}

	holder.Progress, holder.Control = domain.ProgressSucceeded, domain.ControlStopped
	holderAssignment.State = domain.AssignmentCompleted
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{holder}, Assignments: []domain.Assignment{holderAssignment},
	}); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaitsBefore(ctx, settled.Add(5*time.Minute), cutoffs)
	if err != nil || len(wakes) != 1 {
		t.Fatalf("wakes=%+v err=%v, want the deferred wake applied once the slot is free", wakes, err)
	}
	if got := loadAttempt(t, store, attempt.ID); got.Control != domain.ControlResuming {
		t.Fatalf("woken attempt control = %s, want resuming", got.Control)
	}
	if woken := loadTaskWait(t, store, wait.ID); woken.WakeDeferral != nil || !woken.Woken() {
		t.Fatalf("woken wait kept deferral %+v (woken=%v)", woken.WakeDeferral, woken.Woken())
	}
}

func TestWakeDeferralNamesEveryCause(t *testing.T) {
	cases := []struct {
		name    string
		arrange func(t *testing.T, store *Store, now time.Time, cutoffs *TaskWakeCutoffs)
		want    string
	}{
		{
			name: "pool at its concurrency limit",
			arrange: func(t *testing.T, store *Store, now time.Time, _ *TaskWakeCutoffs) {
				holdingAttempt(t, store, "pool-holder", "other-worker", now)
			},
			want: domain.WakeDeferredPoolConcurrency,
		},
		{
			// The worker holding the parked workspace is gone: no current
			// authorized snapshot. The wake waits for it rather than failing.
			name: "worker gone",
			arrange: func(_ *testing.T, _ *Store, _ time.Time, cutoffs *TaskWakeCutoffs) {
				cutoffs.AuthorizedWorkers = map[string]TaskWakeWorkerAuthorization{}
			},
			want: domain.WakeDeferredWorkerUnavailable,
		},
		{
			name: "older ordinary work on the worker",
			arrange: func(_ *testing.T, _ *Store, now time.Time, cutoffs *TaskWakeCutoffs) {
				cutoffs.Worker = map[string]TaskWakeCutoff{"worker": {ReadyAt: now, AttemptID: "ordinary"}}
			},
			want: domain.WakeDeferredOlderWork,
		},
		{
			name: "stale admission",
			arrange: func(_ *testing.T, _ *Store, now time.Time, cutoffs *TaskWakeCutoffs) {
				cutoffs.AdmissionValidAfter = now.Add(time.Hour)
			},
			want: domain.WakeDeferredQuotaAdmission,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store, attempt, now, cutoffs := governedWakeFixture(t, 4, 1)
			wait := parkAndSettle(t, store, attempt, "cause", now)
			test.arrange(t, store, now, &cutoffs)
			wakes, err := store.WakeTaskWaitsBefore(ctx, now.Add(2*time.Second), cutoffs)
			if err != nil || len(wakes) != 0 {
				t.Fatalf("wakes=%+v err=%v, want held", wakes, err)
			}
			if deferral := loadTaskWait(t, store, wait.ID).WakeDeferral; deferral == nil || deferral.Code != test.want || deferral.Detail == "" {
				t.Fatalf("deferral = %+v, want code %s with a detail", deferral, test.want)
			}
			if got := loadAttempt(t, store, attempt.ID); got.Progress.Terminal() || got.Control != domain.ControlWaitingExternal {
				t.Fatalf("deferred attempt became %s/%s; a deferred wake never fails", got.Progress, got.Control)
			}
		})
	}
}

// Resource-sized wakes release and reacquire CPU and memory independently of
// executor slots and pool slots: both slot limits have room in these fixtures.
func TestParkAndWakeRespectCPUAndMemoryReservations(t *testing.T) {
	for _, test := range []struct {
		name     string
		capacity domain.AllocatableCapacity
		detail   string
	}{
		{"cpu", domain.AllocatableCapacity{ExecutorSlots: 4, CPUUnits: 1, MemoryMB: 2048}, "cpu demand"},
		{"memory", domain.AllocatableCapacity{ExecutorSlots: 4, CPUUnits: 4, MemoryMB: 512}, "memory demand"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, parent, now, cutoffs := governedWakeFixture(t, 4, 4)
			holder, _ := holdingAttempt(t, store, "resource-holder", "worker", now)
			setWakeResources(t, store, test.capacity, domain.ResourceDemand{CPUUnits: 1, MemoryMB: 512})
			parentWait := parkAndSettle(t, store, parent, "resource-parent", now)
			wakeAt := now.Add(2 * time.Second)
			assertDeferred := func(waitID string) {
				t.Helper()
				wakes, err := store.WakeTaskWaitsBefore(ctx, wakeAt, cutoffs)
				if err != nil || len(wakes) != 0 {
					t.Fatalf("resource-constrained wake = %+v err=%v, want deferred", wakes, err)
				}
				deferral := loadTaskWait(t, store, waitID).WakeDeferral
				if deferral == nil || deferral.Code != domain.WakeDeferredExecutorCapacity ||
					!strings.Contains(deferral.Detail, test.detail) {
					t.Fatalf("deferral = %+v, want %s despite free executor and pool slots", deferral, test.detail)
				}
				assertSlotsWithinLimits(t, store, 0, 4, 4)
			}
			assertWoken := func(attemptID string) {
				t.Helper()
				wakes, err := store.WakeTaskWaitsBefore(ctx, wakeAt, cutoffs)
				if err != nil || len(wakes) != 1 || wakes[0].AttemptID != attemptID {
					t.Fatalf("wake = %+v err=%v, want only %s after the holder parks", wakes, err, attemptID)
				}
				if got := loadAttempt(t, store, attemptID); got.Control != domain.ControlResuming {
					t.Fatalf("woken control = %s, want resuming", got.Control)
				}
				assertSlotsWithinLimits(t, store, 1, 4, 4)
			}

			assertDeferred(parentWait.ID)
			// Parking a live holder releases its sizes before its wait settles.
			holder = loadAttempt(t, store, holder.ID)
			holderWait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(holder, "resource-holder", domain.WakeEach), wakeAt)
			if err != nil {
				t.Fatal(err)
			}
			assertWoken(parent.ID)
			// A resumed turn immediately reserves its sizes, before the worker
			// reports running, so the other settled wake cannot overcommit them.
			if _, err := store.SettleTaskWait(ctx, holderWait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, wakeAt); err != nil {
				t.Fatal(err)
			}
			assertDeferred(holderWait.ID)
			parent = loadAttempt(t, store, parent.ID)
			parent.Control, parent.Revision = domain.ControlRunning, parent.Revision+1
			if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{parent}}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RegisterTaskWait(ctx, taskWaitRegistration(parent, "resource-parent-again", domain.WakeEach), wakeAt); err != nil {
				t.Fatal(err)
			}
			assertWoken(holder.ID)
		})
	}
}

func setWakeResources(t *testing.T, store *Store, capacity domain.AllocatableCapacity, demand domain.ResourceDemand) {
	t.Helper()
	ctx := context.Background()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for index := range records.Assignments {
		frozen := demand
		records.Assignments[index].ExecutorDemand = &frozen
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: records.Assignments}); err != nil {
		t.Fatal(err)
	}
	snapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("load resource snapshot: count=%d err=%v", len(snapshots), err)
	}
	snapshot := snapshots[0]
	snapshot.Inventory.Allocatable = capacity
	writeTaskWakeSnapshotFixture(t, store, snapshot)
}

// The legacy pass without coordinator worker evidence fails closed on every
// governed wake; it has no evidence of why, so it records nothing.
func TestLegacyWakePassRecordsNoDeferral(t *testing.T) {
	store, attempt, now, _ := governedWakeFixture(t, 1, 1)
	wait := parkAndSettle(t, store, attempt, "legacy", now)
	if wakes, err := store.WakeTaskWaits(context.Background(), now.Add(2*time.Second)); err != nil || len(wakes) != 0 {
		t.Fatalf("wakes=%+v err=%v", wakes, err)
	}
	if deferral := loadTaskWait(t, store, wait.ID).WakeDeferral; deferral != nil {
		t.Fatalf("legacy pass recorded deferral %+v", deferral)
	}
}

// Parking releases the executor and pool slots and waking re-acquires them,
// and across any interleaving of park, settle, wake, resume and finish the
// worker's slots and the pool's limit are never exceeded. The sequence is
// pseudo-random with fixed seeds so a failure reproduces.
func TestParkAndWakeNeverExceedSlotsOrPoolLimit(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			t.Parallel()
			const slots, poolLimit = 2, 2
			ctx := context.Background()
			store, first, now, cutoffs := governedWakeFixture(t, slots, poolLimit)
			first = loadAttempt(t, store, first.ID)
			attempts := []string{first.ID}
			// One more holder fills both slots; four more parked attempts wait.
			holdingAttempt(t, store, "h1", "worker", now)
			attempts = append(attempts, "h1")
			for index := 0; index < 4; index++ {
				id := fmt.Sprintf("p%d", index)
				holder, _ := holdingAttempt(t, store, id, "worker", now)
				if _, err := store.RegisterTaskWait(ctx, taskWaitRegistration(holder, "initial-"+id, domain.WakeEach), now); err != nil {
					t.Fatal(err)
				}
				attempts = append(attempts, id)
			}
			setWakeResources(t, store, domain.AllocatableCapacity{
				ExecutorSlots: slots, CPUUnits: 3, MemoryMB: 1536,
			}, domain.ResourceDemand{CPUUnits: 1.5, MemoryMB: 768})
			assertSlotsWithinLimits(t, store, -1, slots, poolLimit)
			random := rand.New(rand.NewSource(seed))
			clock := now
			sequence, woken, deferred := 0, 0, 0
			for step := 0; step < 120; step++ {
				clock = clock.Add(time.Second)
				id := attempts[random.Intn(len(attempts))]
				attempt := loadAttempt(t, store, id)
				switch random.Intn(5) {
				case 0: // a running turn parks
					if attempt.Control == domain.ControlRunning {
						sequence++
						if _, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, fmt.Sprintf("park-%d", sequence), domain.WakeEach), clock); err != nil {
							t.Fatal(err)
						}
					}
				case 1: // a live wait settles
					waits, err := store.ListTaskWaits(ctx)
					if err != nil {
						t.Fatal(err)
					}
					for _, wait := range waits {
						if wait.AttemptID == id && wait.Live() {
							if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, clock); err != nil {
								t.Fatal(err)
							}
						}
					}
				case 2: // a coordinator wake pass
					wakes, err := store.WakeTaskWaitsBefore(ctx, clock, cutoffs)
					if err != nil {
						t.Fatal(err)
					}
					woken += len(wakes)
					waits, err := store.ListTaskWaits(ctx)
					if err != nil {
						t.Fatal(err)
					}
					for _, wait := range waits {
						if wait.WakeDeferral != nil && !wait.Woken() {
							deferred++
						}
					}
				case 3: // the worker observes a resumed turn running
					if attempt.Control == domain.ControlResuming {
						attempt.Control, attempt.Revision = domain.ControlRunning, attempt.Revision+1
						if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
							t.Fatal(err)
						}
					}
				case 4: // a running turn finishes and releases its assignment
					if attempt.Control == domain.ControlRunning {
						records, err := store.LoadCoordinatorRecords(ctx)
						if err != nil {
							t.Fatal(err)
						}
						for _, assignment := range records.Assignments {
							if assignment.ID == attempt.AssignmentID {
								assignment.State = domain.AssignmentCompleted
								attempt.Progress, attempt.Control = domain.ProgressSucceeded, domain.ControlStopped
								attempt.Revision++
								if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
									Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
								}); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
				}
				assertSlotsWithinLimits(t, store, step, slots, poolLimit)
			}
			// The sequence must have exercised both outcomes of a wake pass, or
			// the bound above says nothing about re-acquisition.
			if woken == 0 || deferred == 0 {
				t.Fatalf("sequence applied %d wakes and saw %d deferrals; it exercised too little", woken, deferred)
			}
		})
	}
}

func assertSlotsWithinLimits(t *testing.T, store *Store, step, slots, poolLimit int) {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	attempts := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attempts[attempt.ID] = attempt
	}
	executor, provider := 0, 0
	var cpu float64
	memory := 0
	for _, assignment := range records.Assignments {
		attempt, found := attempts[assignment.AttemptID]
		if !found || attempt.Progress.Terminal() {
			continue
		}
		if assignment.WorkerID == "worker" && domain.AssignmentOwnsExecutorCapacity(attempt, assignment) {
			executor++
			demand, known := domain.AssignmentExecutorDemand(attempt, assignment)
			if !known {
				t.Fatalf("step %d: resource reservation for %s is not frozen", step, assignment.ID)
			}
			cpu += demand.CPUUnits
			memory += demand.MemoryMB
		}
		if assignment.Route.QuotaPoolID == "pool" && assignment.State != domain.AssignmentCompleted &&
			assignment.State != domain.AssignmentReleased && attempt.Control.HoldsProviderSlot() {
			provider++
		}
	}
	snapshots, err := store.LoadWorkerSnapshots(context.Background())
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("step %d: snapshot count=%d err=%v", step, len(snapshots), err)
	}
	capacity := snapshots[0].Inventory.Allocatable
	if (capacity.CPUUnits > 0 && cpu > capacity.CPUUnits) ||
		(capacity.MemoryMB > 0 && memory > capacity.MemoryMB) {
		t.Fatalf("step %d: reserved CPU %v/%v and memory %d/%d", step, cpu, capacity.CPUUnits, memory, capacity.MemoryMB)
	}
	if executor > slots || provider > poolLimit {
		t.Fatalf("step %d: %d executor slots of %d and %d pool slots of %d are held", step, executor, slots, provider, poolLimit)
	}
}
