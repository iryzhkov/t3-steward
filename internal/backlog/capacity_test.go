package backlog

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

var capacityTestTime = time.Date(2026, 9, 13, 9, 0, 0, 0, time.UTC)

func TestExecutorCapacityAndProviderConcurrencyAreSeparateLimits(t *testing.T) {
	registry := mustRegistry(t, capacityPool("homelab", domain.CPUClassMedium, 4))

	request := capacityRequest("reservation-1", "homelab")
	request.ProviderAdmissionHeld = false
	if _, err := registry.Reserve(request); !errors.Is(err, ErrProviderAdmissionRequired) {
		t.Fatalf("attempt without provider admission = %v, want ErrProviderAdmissionRequired", err)
	}
	if outstanding := registry.Outstanding(); len(outstanding) != 0 {
		t.Fatalf("refused attempt still holds %#v", outstanding)
	}

	// Preflight opens no provider session, so it reserves executor and CPU
	// capacity without provider admission.
	preflight := capacityRequest("reservation-preflight", "homelab")
	preflight.Kind = ReservationKindPreflight
	preflight.ProviderAdmissionHeld = false
	if _, err := registry.Reserve(preflight); err != nil {
		t.Fatalf("preflight reservation: %v", err)
	}
	if free := registry.Snapshot("homelab").FreeSlots(); free != 3 {
		t.Fatalf("free slots after preflight = %d, want 3", free)
	}

	// An attempt that does hold provider admission still needs a slot, which
	// proves the two limits are enforced independently rather than one
	// standing in for the other.
	if _, err := registry.Reserve(capacityRequest("reservation-2", "homelab")); err != nil {
		t.Fatalf("admitted attempt reservation: %v", err)
	}
}

func TestExecutorReservationIsAtomicAndRespectsTheClassFloor(t *testing.T) {
	pool := capacityPool("normandy", domain.CPUClassLow, 2)
	pool.Allocatable.CPUUnits = 4
	pool.Allocatable.MemoryMB = 4096
	registry := mustRegistry(t, pool)

	oversized := capacityRequest("reservation-oversized", "normandy")
	oversized.Demand = domain.ResourceDemand{CPUUnits: 8}
	if _, err := registry.Reserve(oversized); !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("oversized reservation = %v, want ErrCapacityExhausted", err)
	}
	snapshot := registry.Snapshot("normandy")
	if snapshot.Reserved.ExecutorSlots != 0 || snapshot.Reserved.CPUUnits != 0 {
		t.Fatalf("refused reservation committed capacity: %#v", snapshot.Reserved)
	}
	for _, slot := range registry.Slots("normandy") {
		if slot.State != domain.ExecutorSlotFree || slot.FencingToken != "" {
			t.Fatalf("refused reservation left slot %#v", slot)
		}
	}

	build := capacityRequest("reservation-build", "normandy")
	build.Demand = domain.ResourceDemand{MinCPUClass: domain.CPUClassMedium, CPUUnits: 1}
	if _, err := registry.Reserve(build); !errors.Is(err, ErrCPUClassBelowMinimum) {
		t.Fatalf("build reservation on a low-class worker = %v, want ErrCPUClassBelowMinimum", err)
	}

	sized := capacityRequest("reservation-sized", "normandy")
	sized.Demand = domain.ResourceDemand{MinCPUClass: domain.CPUClassLow, CPUUnits: 3, MemoryMB: 2048}
	reservation, err := registry.Reserve(sized)
	if err != nil {
		t.Fatalf("sized reservation: %v", err)
	}
	if reservation.SlotFencingToken == "" || reservation.State != domain.ResourceReservationHeld {
		t.Fatalf("reservation = %#v", reservation)
	}
	if snapshot := registry.Snapshot("normandy"); snapshot.FreeCPUUnits() != 1 || snapshot.FreeMemoryMB() != 2048 {
		t.Fatalf("free capacity after reservation = %v cpu, %d MB", snapshot.FreeCPUUnits(), snapshot.FreeMemoryMB())
	}
}

func TestExecutorReleaseIsExactlyOnceOnEveryTerminalPath(t *testing.T) {
	pool := capacityPool("homelab", domain.CPUClassMedium, 4)
	pool.Allocatable.CPUUnits = 8
	pool.Allocatable.MemoryMB = 8192
	registry := mustRegistry(t, pool)

	reasons := domain.ResourceReleaseReasons()
	if len(reasons) != 4 {
		t.Fatalf("terminal release paths = %#v, want settlement, cancellation, failed preparation and lease recovery", reasons)
	}
	for index := range reasons {
		id := fmt.Sprintf("reservation-%d", index)
		request := capacityRequest(id, "homelab")
		request.Demand = domain.ResourceDemand{CPUUnits: 2, MemoryMB: 2048}
		if _, err := registry.Reserve(request); err != nil {
			t.Fatalf("reserve %q: %v", id, err)
		}
	}
	if snapshot := registry.Snapshot("homelab"); snapshot.FreeSlots() != 0 || snapshot.FreeCPUUnits() != 0 {
		t.Fatalf("capacity after four reservations = %#v", snapshot)
	}

	for index, reason := range reasons {
		id := fmt.Sprintf("reservation-%d", index)
		if index == 1 {
			// A cancelled attempt may already be running in its slot.
			if _, err := registry.Activate(id); err != nil {
				t.Fatalf("activate %q: %v", id, err)
			}
		}
		released, err := registry.Release(id, reason)
		if err != nil {
			t.Fatalf("release %q as %q: %v", id, string(reason), err)
		}
		if released.ReleaseReason != reason || !released.Released() || released.ReleasedAt == nil {
			t.Fatalf("released reservation = %#v", released)
		}

		// A second release on any path is refused rather than returning the
		// capacity twice, on this reason and on a different one.
		if _, err := registry.Release(id, reason); !errors.Is(err, ErrReservationReleased) {
			t.Fatalf("repeated release of %q = %v, want ErrReservationReleased", id, err)
		}
		if _, err := registry.Release(id, domain.ResourceReleaseSettled); !errors.Is(err, ErrReservationReleased) {
			t.Fatalf("crossed release of %q = %v, want ErrReservationReleased", id, err)
		}
		if _, err := registry.Activate(id); !errors.Is(err, ErrReservationReleased) {
			t.Fatalf("activation after release of %q = %v, want ErrReservationReleased", id, err)
		}
	}

	if _, err := registry.Release("reservation-0", "forgotten"); err == nil {
		t.Fatal("release with an unenumerated reason was accepted")
	}
	if _, err := registry.Release("reservation-missing", domain.ResourceReleaseSettled); !errors.Is(err, ErrUnknownReservation) {
		t.Fatalf("release of an unknown reservation = %v, want ErrUnknownReservation", err)
	}

	// No release was missed: nothing is outstanding, every slot is free, and
	// the worker's full allocatable capacity is available again.
	if outstanding := registry.Outstanding(); len(outstanding) != 0 {
		t.Fatalf("outstanding reservations = %#v", outstanding)
	}
	snapshot := registry.Snapshot("homelab")
	if snapshot.FreeSlots() != 4 || snapshot.FreeCPUUnits() != 8 || snapshot.FreeMemoryMB() != 8192 {
		t.Fatalf("capacity after every terminal path = %#v", snapshot)
	}
	for _, slot := range registry.Slots("homelab") {
		if slot.State != domain.ExecutorSlotFree || slot.FencingToken != "" || slot.ReservationID != "" {
			t.Fatalf("slot after release = %#v", slot)
		}
	}
}

func TestExecutorPoolsReachTheQualificationFloors(t *testing.T) {
	profiles := config.FleetWorkerProfiles()
	want := map[string]struct {
		class string
		slots int
	}{
		"normandy":   {config.CPUClassLow, 2},
		"homelab":    {config.CPUClassMedium, 4},
		"omarchy-pc": {config.CPUClassHigh, 8},
	}
	for workerID, expected := range want {
		profile, configured := profiles[workerID]
		if !configured || profile.CPUClass != expected.class || profile.Executors.Slots != expected.slots {
			t.Fatalf("fleet profile for %q = %#v, want class %q with %d slots",
				workerID, profile, expected.class, expected.slots)
		}

		pool := capacityPool(workerID, domain.CPUClass(profile.CPUClass), profile.Executors.Slots)
		registry := mustRegistry(t, pool)
		tokens := make(map[string]struct{}, profile.Executors.Slots)
		ordinals := make(map[int]struct{}, profile.Executors.Slots)
		for index := range profile.Executors.Slots {
			reservation, err := registry.Reserve(capacityRequest(fmt.Sprintf("%s-%d", workerID, index), workerID))
			if err != nil {
				t.Fatalf("%s concurrent attempt %d: %v", workerID, index+1, err)
			}
			if _, repeated := tokens[reservation.SlotFencingToken]; repeated {
				t.Fatalf("%s reused fencing token %q", workerID, reservation.SlotFencingToken)
			}
			if _, repeated := ordinals[reservation.SlotOrdinal]; repeated {
				t.Fatalf("%s reused slot ordinal %d", workerID, reservation.SlotOrdinal)
			}
			tokens[reservation.SlotFencingToken] = struct{}{}
			ordinals[reservation.SlotOrdinal] = struct{}{}
		}

		// The floor is what the pool must sustain, not a ceiling this code
		// imposes: the refusal comes from the configured slot count, and a
		// larger configuration admits more.
		overflow := capacityRequest(workerID+"-overflow", workerID)
		if _, err := registry.Reserve(overflow); !errors.Is(err, ErrCapacityExhausted) {
			t.Fatalf("%s attempt beyond its configured slots = %v, want ErrCapacityExhausted", workerID, err)
		}
		for index := range profile.Executors.Slots {
			if _, err := registry.Release(fmt.Sprintf("%s-%d", workerID, index), domain.ResourceReleaseSettled); err != nil {
				t.Fatalf("%s release %d: %v", workerID, index, err)
			}
		}
		if free := registry.Snapshot(workerID).FreeSlots(); free != profile.Executors.Slots {
			t.Fatalf("%s free slots after release = %d, want %d", workerID, free, profile.Executors.Slots)
		}
	}
}

func TestExecutorRegistryNeverOverCommitsUnderConcurrentReservation(t *testing.T) {
	const slots = 8
	registry := mustRegistry(t, capacityPool("omarchy-pc", domain.CPUClassHigh, slots))

	var wait sync.WaitGroup
	results := make(chan error, 4*slots)
	for index := range 4 * slots {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, err := registry.Reserve(capacityRequest(fmt.Sprintf("racing-%d", index), "omarchy-pc"))
			results <- err
		}(index)
	}
	wait.Wait()
	close(results)

	granted := 0
	for err := range results {
		switch {
		case err == nil:
			granted++
		case errors.Is(err, ErrCapacityExhausted):
		default:
			t.Fatalf("unexpected reservation error: %v", err)
		}
	}
	if granted != slots {
		t.Fatalf("granted reservations = %d, want exactly the %d configured slots", granted, slots)
	}
	if outstanding := registry.Outstanding(); len(outstanding) != slots {
		t.Fatalf("outstanding reservations = %d, want %d", len(outstanding), slots)
	}
}

func TestExecutorSlotReuseIssuesANewFencingToken(t *testing.T) {
	registry := mustRegistry(t, capacityPool("normandy", domain.CPUClassLow, 1))

	first, err := registry.Reserve(capacityRequest("first", "normandy"))
	if err != nil {
		t.Fatalf("first reservation: %v", err)
	}
	if _, err := registry.Release("first", domain.ResourceReleasePreparationFailed); err != nil {
		t.Fatalf("release first: %v", err)
	}
	second, err := registry.Reserve(capacityRequest("second", "normandy"))
	if err != nil {
		t.Fatalf("second reservation: %v", err)
	}
	if second.SlotOrdinal != first.SlotOrdinal {
		t.Fatalf("second reservation took ordinal %d, want the recycled %d", second.SlotOrdinal, first.SlotOrdinal)
	}
	if second.SlotFencingToken == first.SlotFencingToken {
		t.Fatalf("recycled slot kept fencing token %q", first.SlotFencingToken)
	}

	// The stale holder's release is still accounted to its own reservation
	// and cannot free the slot the new holder fenced.
	if _, err := registry.Release("first", domain.ResourceReleaseSettled); !errors.Is(err, ErrReservationReleased) {
		t.Fatalf("stale release = %v, want ErrReservationReleased", err)
	}
	if free := registry.Snapshot("normandy").FreeSlots(); free != 0 {
		t.Fatalf("free slots after a stale release = %d, want the new holder to keep the slot", free)
	}
}

func TestExecutorRegistryRejectsInvalidPools(t *testing.T) {
	if _, err := NewExecutorRegistry([]domain.ExecutorPool{capacityPool("normandy", domain.CPUClassLow, 0)}, capacityClock()); err == nil {
		t.Fatal("pool without executor slots was accepted")
	}
	duplicate := []domain.ExecutorPool{
		capacityPool("normandy", domain.CPUClassLow, 2),
		capacityPool("normandy", domain.CPUClassLow, 4),
	}
	if _, err := NewExecutorRegistry(duplicate, capacityClock()); err == nil {
		t.Fatal("two pools on one worker were accepted")
	}
	registry := mustRegistry(t, capacityPool("normandy", domain.CPUClassLow, 2))
	if _, err := registry.Reserve(capacityRequest("elsewhere", "gaming-pc")); !errors.Is(err, ErrUnknownExecutorPool) {
		t.Fatalf("reservation on an unconfigured worker = %v, want ErrUnknownExecutorPool", err)
	}
	if _, err := registry.Reserve(capacityRequest("twice", "normandy")); err != nil {
		t.Fatalf("first reservation: %v", err)
	}
	if _, err := registry.Reserve(capacityRequest("twice", "normandy")); !errors.Is(err, ErrDuplicateReservation) {
		t.Fatalf("repeated reservation ID = %v, want ErrDuplicateReservation", err)
	}
}

func capacityClock() func() time.Time { return func() time.Time { return capacityTestTime } }

func capacityPool(workerID string, class domain.CPUClass, slots int) domain.ExecutorPool {
	return domain.ExecutorPool{
		WorkerID: workerID, Name: "default", CatalogRevision: "catalog-1", CPUClass: class,
		Allocatable: domain.AllocatableCapacity{ExecutorSlots: slots},
	}
}

func capacityRequest(reservationID, workerID string) ReservationRequest {
	return ReservationRequest{
		ReservationID: reservationID, WorkerID: workerID,
		AssignmentID: reservationID + "-assignment", AttemptID: reservationID + "-attempt",
		LeaseToken: reservationID + "-lease", LeaseExpiresAt: capacityTestTime.Add(time.Hour),
		Kind: ReservationKindAttempt, ProviderAdmissionHeld: true,
	}
}

func mustRegistry(t *testing.T, pools ...domain.ExecutorPool) *ExecutorRegistry {
	t.Helper()
	registry, err := NewExecutorRegistry(pools, capacityClock())
	if err != nil {
		t.Fatalf("executor registry: %v", err)
	}
	return registry
}
