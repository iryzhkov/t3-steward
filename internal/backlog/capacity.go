package backlog

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Executor capacity errors. They are sentinels so that a caller, and a test of
// a terminal release path, can assert which limit refused a reservation.
var (
	ErrUnknownExecutorPool       = errors.New("executor pool is not configured for this worker")
	ErrDuplicateReservation      = errors.New("reservation identity is already in use")
	ErrUnknownReservation        = errors.New("reservation is not known")
	ErrReservationReleased       = errors.New("reservation was already released")
	ErrCapacityExhausted         = errors.New("worker has no free executor capacity")
	ErrCPUClassBelowMinimum      = errors.New("worker cpu class is below the required minimum")
	ErrProviderAdmissionRequired = errors.New("attempt requires provider admission as well as executor capacity")
)

// ReservationKind separates work that needs a provider session from work that
// does not.
//
// Executor capacity and provider-session concurrency are separate limits. An
// attempt must hold both before it starts, which is why ReservationKindAttempt
// refuses to reserve without provider admission. Preflight and bounded
// read-only context collection reserve executor and CPU capacity and never
// provider quota, because they open no provider session.
type ReservationKind string

const (
	ReservationKindAttempt   ReservationKind = "attempt"
	ReservationKindPreflight ReservationKind = "preflight"
)

// ReservationRequest asks for capacity on one worker for one unit of work.
type ReservationRequest struct {
	ReservationID  string
	WorkerID       string
	AssignmentID   string
	AttemptID      string
	LeaseToken     string
	LeaseExpiresAt time.Time
	Kind           ReservationKind
	Demand         domain.ResourceDemand
	// ProviderAdmissionHeld states that the caller already holds provider
	// admission for this attempt's route. The registry never reads a quota
	// pool itself: QuotaPool.MaxConcurrent remains the provider-session limit
	// and is enforced by the routing phase that owns it.
	ProviderAdmissionHeld bool
}

func (r ReservationRequest) validate() error {
	if strings.TrimSpace(r.ReservationID) != r.ReservationID || r.ReservationID == "" {
		return errors.New("reserve capacity: reservation ID must be nonempty and trimmed")
	}
	if strings.TrimSpace(r.WorkerID) == "" {
		return errors.New("reserve capacity: worker ID is required")
	}
	switch r.Kind {
	case ReservationKindAttempt, ReservationKindPreflight:
	default:
		return fmt.Errorf("reserve capacity: reservation kind %q must be attempt or preflight", string(r.Kind))
	}
	if err := r.Demand.Validate(); err != nil {
		return fmt.Errorf("reserve capacity: %w", err)
	}
	if r.Kind == ReservationKindAttempt && !r.ProviderAdmissionHeld {
		return fmt.Errorf("reserve capacity for %q: %w", r.ReservationID, ErrProviderAdmissionRequired)
	}
	return nil
}

// ExecutorRegistry owns the executor pools of the fleet, their independently
// fenced slots, and the reservations held against them. Every mutation is
// atomic: a reservation either acquires its slot and all of its sized capacity
// or changes nothing, and a release returns exactly that capacity exactly once.
//
// The registry is safe for concurrent use, because concurrent attempts on one
// worker are the point of an executor pool.
type ExecutorRegistry struct {
	mu            sync.Mutex
	now           func() time.Time
	pools         map[string]domain.ExecutorPool
	slots         map[string][]domain.ExecutorSlot
	reservations  map[string]domain.ResourceReservation
	tokenSequence uint64
}

// NewExecutorRegistry validates the configured pools and materializes their
// slots. A pool's slot count is configured capacity: it may be raised without
// changing this code, and the qualification floors are minima to sustain, not
// limits to enforce.
func NewExecutorRegistry(pools []domain.ExecutorPool, now func() time.Time) (*ExecutorRegistry, error) {
	if now == nil {
		now = time.Now
	}
	registry := &ExecutorRegistry{
		now:          now,
		pools:        make(map[string]domain.ExecutorPool, len(pools)),
		slots:        make(map[string][]domain.ExecutorSlot, len(pools)),
		reservations: make(map[string]domain.ResourceReservation),
	}
	for _, pool := range pools {
		if err := pool.Validate(); err != nil {
			return nil, fmt.Errorf("executor registry: %w", err)
		}
		if _, duplicate := registry.pools[pool.WorkerID]; duplicate {
			return nil, fmt.Errorf("executor registry: worker %q declares more than one executor pool", pool.WorkerID)
		}
		registry.pools[pool.WorkerID] = pool
		slots := make([]domain.ExecutorSlot, 0, pool.Allocatable.ExecutorSlots)
		for ordinal := range pool.Allocatable.ExecutorSlots {
			slots = append(slots, domain.ExecutorSlot{
				WorkerID: pool.WorkerID, PoolName: pool.Name, Ordinal: ordinal,
				State: domain.ExecutorSlotFree, UpdatedAt: now(),
			})
		}
		registry.slots[pool.WorkerID] = slots
	}
	return registry, nil
}

// Reserve atomically acquires one executor slot and the sized capacity a unit
// of work declared. On any refusal nothing is committed, so a failed placement
// leaves no capacity held and no slot fenced.
func (r *ExecutorRegistry) Reserve(request ReservationRequest) (domain.ResourceReservation, error) {
	if err := request.validate(); err != nil {
		return domain.ResourceReservation{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	pool, configured := r.pools[request.WorkerID]
	if !configured {
		return domain.ResourceReservation{}, fmt.Errorf("reserve capacity on worker %q: %w",
			request.WorkerID, ErrUnknownExecutorPool)
	}
	if _, exists := r.reservations[request.ReservationID]; exists {
		return domain.ResourceReservation{}, fmt.Errorf("reserve capacity %q: %w",
			request.ReservationID, ErrDuplicateReservation)
	}
	if request.Demand.MinCPUClass != "" &&
		(!pool.CPUClass.Valid() || pool.CPUClass.Compare(request.Demand.MinCPUClass) < 0) {
		return domain.ResourceReservation{}, fmt.Errorf(
			"reserve capacity on worker %q: cpu class %q against minimum %q: %w",
			request.WorkerID, string(pool.CPUClass), string(request.Demand.MinCPUClass), ErrCPUClassBelowMinimum)
	}

	snapshot := r.snapshotLocked(request.WorkerID)
	slotAt := -1
	for index, slot := range r.slots[request.WorkerID] {
		if slot.State == domain.ExecutorSlotFree {
			slotAt = index
			break
		}
	}
	switch {
	case slotAt < 0:
		return domain.ResourceReservation{}, fmt.Errorf(
			"reserve capacity on worker %q: all %d executor slots are occupied: %w",
			request.WorkerID, pool.Allocatable.ExecutorSlots, ErrCapacityExhausted)
	case request.Demand.CPUUnits > snapshot.FreeCPUUnits():
		return domain.ResourceReservation{}, fmt.Errorf(
			"reserve capacity on worker %q: %v free cpu units for a demand of %v: %w",
			request.WorkerID, snapshot.FreeCPUUnits(), request.Demand.CPUUnits, ErrCapacityExhausted)
	case request.Demand.MemoryMB > snapshot.FreeMemoryMB():
		return domain.ResourceReservation{}, fmt.Errorf(
			"reserve capacity on worker %q: %d MB free memory for a demand of %d MB: %w",
			request.WorkerID, snapshot.FreeMemoryMB(), request.Demand.MemoryMB, ErrCapacityExhausted)
	case request.Demand.ScratchMB > snapshot.FreeScratchMB():
		return domain.ResourceReservation{}, fmt.Errorf(
			"reserve capacity on worker %q: %d MB free scratch for a demand of %d MB: %w",
			request.WorkerID, snapshot.FreeScratchMB(), request.Demand.ScratchMB, ErrCapacityExhausted)
	}

	moment := r.now()
	r.tokenSequence++
	slot := r.slots[request.WorkerID][slotAt]
	slot.State = domain.ExecutorSlotReserved
	slot.FencingToken = fmt.Sprintf("%s-%d", slot.ID(), r.tokenSequence)
	slot.ReservationID = request.ReservationID
	slot.AssignmentID = request.AssignmentID
	slot.UpdatedAt = moment
	r.slots[request.WorkerID][slotAt] = slot

	reservation := domain.ResourceReservation{
		ID: request.ReservationID, WorkerID: request.WorkerID, PoolName: pool.Name,
		SlotOrdinal: slot.Ordinal, SlotFencingToken: slot.FencingToken,
		AssignmentID: request.AssignmentID, AttemptID: request.AttemptID,
		LeaseToken: request.LeaseToken, LeaseExpiresAt: request.LeaseExpiresAt,
		CPUUnits: request.Demand.CPUUnits, MemoryMB: request.Demand.MemoryMB,
		ScratchMB: request.Demand.ScratchMB,
		State:     domain.ResourceReservationHeld,
		CreatedAt: moment, UpdatedAt: moment,
	}
	if err := reservation.Validate(); err != nil {
		r.slots[request.WorkerID][slotAt] = freeSlot(slot, moment)
		return domain.ResourceReservation{}, fmt.Errorf("reserve capacity: %w", err)
	}
	r.reservations[reservation.ID] = reservation
	return reservation, nil
}

// Activate records that the worker claimed the assignment and began executing
// in the reserved slot. It changes occupancy, never capacity.
func (r *ExecutorRegistry) Activate(reservationID string) (domain.ResourceReservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reservation, known := r.reservations[reservationID]
	switch {
	case !known:
		return domain.ResourceReservation{}, fmt.Errorf("activate %q: %w", reservationID, ErrUnknownReservation)
	case reservation.Released():
		return domain.ResourceReservation{}, fmt.Errorf("activate %q: %w", reservationID, ErrReservationReleased)
	}
	moment := r.now()
	reservation.State = domain.ResourceReservationActive
	reservation.UpdatedAt = moment
	r.reservations[reservationID] = reservation
	r.updateSlotLocked(reservation, func(slot *domain.ExecutorSlot) {
		slot.State = domain.ExecutorSlotRunning
		slot.UpdatedAt = moment
	})
	return reservation, nil
}

// Release returns a reservation's capacity exactly once.
//
// The terminal paths are closed and enumerated by
// domain.ResourceReleaseReasons: settlement, cancellation, failed preparation
// and lease recovery. A release without one of those reasons is refused, and a
// second release of the same reservation is refused with ErrReservationReleased
// rather than returning the capacity twice.
func (r *ExecutorRegistry) Release(reservationID string, reason domain.ResourceReleaseReason) (domain.ResourceReservation, error) {
	if !reason.Valid() {
		return domain.ResourceReservation{}, fmt.Errorf(
			"release %q: %q is not one of the terminal release paths", reservationID, string(reason))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	reservation, known := r.reservations[reservationID]
	switch {
	case !known:
		return domain.ResourceReservation{}, fmt.Errorf("release %q: %w", reservationID, ErrUnknownReservation)
	case reservation.Released():
		return reservation, fmt.Errorf("release %q already released as %q: %w",
			reservationID, string(reservation.ReleaseReason), ErrReservationReleased)
	}

	moment := r.now()
	r.updateSlotLocked(reservation, func(slot *domain.ExecutorSlot) { *slot = freeSlot(*slot, moment) })
	reservation.State = domain.ResourceReservationReleased
	reservation.ReleaseReason = reason
	reservation.UpdatedAt = moment
	reservation.ReleasedAt = &moment
	r.reservations[reservationID] = reservation
	return reservation, nil
}

// Reservation reports one reservation, released or not, so that a terminal
// path is provable after the fact rather than inferred from free capacity.
func (r *ExecutorRegistry) Reservation(reservationID string) (domain.ResourceReservation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reservation, known := r.reservations[reservationID]
	return reservation, known
}

// Outstanding lists every reservation that still holds capacity, ordered by
// ID. An empty result after a batch is the evidence that no release was missed.
func (r *ExecutorRegistry) Outstanding() []domain.ResourceReservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	var outstanding []domain.ResourceReservation
	for _, reservation := range r.reservations {
		if !reservation.Released() {
			outstanding = append(outstanding, reservation)
		}
	}
	sort.Slice(outstanding, func(i, j int) bool { return outstanding[i].ID < outstanding[j].ID })
	return outstanding
}

// Slots reports one worker's slots in ordinal order.
func (r *ExecutorRegistry) Slots(workerID string) []domain.ExecutorSlot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]domain.ExecutorSlot(nil), r.slots[workerID]...)
}

// Snapshot reports one worker's capacity: its configured allocatable totals
// and what reservations currently hold. Observed pressure belongs to the
// worker's own inventory and is not invented here.
func (r *ExecutorRegistry) Snapshot(workerID string) domain.WorkerCapacitySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked(workerID)
}

func (r *ExecutorRegistry) snapshotLocked(workerID string) domain.WorkerCapacitySnapshot {
	pool := r.pools[workerID]
	snapshot := domain.WorkerCapacitySnapshot{
		WorkerID: workerID, CPUClass: pool.CPUClass,
		Allocatable: pool.Allocatable, ObservedAt: r.now(),
	}
	for _, slot := range r.slots[workerID] {
		if slot.Occupied() {
			snapshot.Reserved.ExecutorSlots++
		}
	}
	for _, reservation := range r.reservations {
		if reservation.WorkerID != workerID || reservation.Released() {
			continue
		}
		snapshot.Reserved.CPUUnits += reservation.CPUUnits
		snapshot.Reserved.MemoryMB += reservation.MemoryMB
		snapshot.Reserved.ScratchMB += reservation.ScratchMB
	}
	return snapshot
}

func (r *ExecutorRegistry) updateSlotLocked(reservation domain.ResourceReservation, mutate func(*domain.ExecutorSlot)) {
	slots := r.slots[reservation.WorkerID]
	for index := range slots {
		if slots[index].Ordinal != reservation.SlotOrdinal ||
			slots[index].FencingToken != reservation.SlotFencingToken {
			continue
		}
		mutate(&slots[index])
		return
	}
}

// freeSlot clears occupancy and the fencing token together. The next
// acquisition of this ordinal issues a new token, so a stale holder of the old
// token can never act on the recycled slot.
func freeSlot(slot domain.ExecutorSlot, moment time.Time) domain.ExecutorSlot {
	return domain.ExecutorSlot{
		WorkerID: slot.WorkerID, PoolName: slot.PoolName, Ordinal: slot.Ordinal,
		State: domain.ExecutorSlotFree, UpdatedAt: moment,
	}
}
