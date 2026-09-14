package domain

import (
	"fmt"
	"strings"
	"time"
)

// CPUClass is the operator-assigned performance class of a worker's CPU. It is
// an ordered value: low < medium < high.
//
// CPUClass is a static capability floor. It describes the useful per-core and
// build performance an operator decided a host has, and it is a user decision
// rather than a benchmark output. It is deliberately not allocatable capacity
// and not current pressure: the three facts have different owners and change
// rates, and none of them may be derived from another. CPUClass is also not
// TaskClass, which classifies required versus surplus admission.
type CPUClass string

const (
	CPUClassLow    CPUClass = "low"
	CPUClassMedium CPUClass = "medium"
	CPUClassHigh   CPUClass = "high"
)

// Rank orders the classes. An unknown or unset class ranks below every valid
// class, so a comparison on unvalidated input never claims a bad value is high.
func (c CPUClass) Rank() int {
	switch c {
	case CPUClassLow:
		return 1
	case CPUClassMedium:
		return 2
	case CPUClassHigh:
		return 3
	default:
		return 0
	}
}

// Valid reports whether the class is one of the three declared values.
func (c CPUClass) Valid() bool { return c.Rank() != 0 }

// Compare returns a negative number when c is the lower class, zero when the
// two are equal, and a positive number when c is the higher class.
func (c CPUClass) Compare(other CPUClass) int { return c.Rank() - other.Rank() }

// Validate accepts an unset class, because most tasks and some workers declare
// none, and rejects any value that is set but not one of the three.
func (c CPUClass) Validate() error {
	if c == "" || c.Valid() {
		return nil
	}
	return fmt.Errorf("cpu class %q must be low, medium or high", string(c))
}

// ResourceDemand is what one task needs in order to run. It is declared by the
// submitter, committed with the immutable task definition, and sized
// independently of eligibility, which is why it is a sibling of Placement
// rather than a member of it.
//
// MinCPUClass is a hard constraint. PreferredCPUClass only scores: a worker
// below the preferred class stays eligible and loses preference points.
type ResourceDemand struct {
	MinCPUClass       CPUClass `json:"minCpuClass,omitempty"`
	PreferredCPUClass CPUClass `json:"preferredCpuClass,omitempty"`
	CPUUnits          float64  `json:"cpuUnits,omitempty"`
	MemoryMB          int      `json:"memoryMb,omitempty"`
	ScratchMB         int      `json:"scratchMb,omitempty"`
}

// IsZero reports whether the task declared no demand at all. A task that
// declares nothing constrains nothing, so placement must not exclude a worker
// for capacity it was never asked to provide.
func (d ResourceDemand) IsZero() bool {
	return d.MinCPUClass == "" && d.PreferredCPUClass == "" &&
		d.CPUUnits == 0 && d.MemoryMB == 0 && d.ScratchMB == 0
}

// Validate rejects negative sizes, unknown classes and a preferred class below
// the declared floor, which would be a contradiction rather than a preference.
func (d ResourceDemand) Validate() error {
	if err := d.MinCPUClass.Validate(); err != nil {
		return fmt.Errorf("resource demand minimum %w", err)
	}
	if err := d.PreferredCPUClass.Validate(); err != nil {
		return fmt.Errorf("resource demand preferred %w", err)
	}
	if d.MinCPUClass != "" && d.PreferredCPUClass != "" &&
		d.PreferredCPUClass.Compare(d.MinCPUClass) < 0 {
		return fmt.Errorf("resource demand preferred cpu class %q is below its minimum %q",
			string(d.PreferredCPUClass), string(d.MinCPUClass))
	}
	if d.CPUUnits < 0 || d.MemoryMB < 0 || d.ScratchMB < 0 {
		return fmt.Errorf("resource demand sizes must not be negative")
	}
	return nil
}

// TargetCPUClass is the class placement scores against: the preferred class
// when one is declared, otherwise the floor. It is empty when the task asked
// for neither, and an empty target means the smallest sufficient worker wins.
func (d ResourceDemand) TargetCPUClass() CPUClass {
	if d.PreferredCPUClass != "" {
		return d.PreferredCPUClass
	}
	return d.MinCPUClass
}

// AllocatableCapacity is the operator-configured total the scheduler may
// commit on one worker. It changes only with a catalog revision, and
// reservations are subtracted from it. It is never inferred from live load.
type AllocatableCapacity struct {
	ExecutorSlots int     `json:"executorSlots,omitempty"`
	CPUUnits      float64 `json:"cpuUnits,omitempty"`
	MemoryMB      int     `json:"memoryMb,omitempty"`
	ScratchMB     int     `json:"scratchMb,omitempty"`
}

// IsZero reports whether a worker declares no allocatable capacity at all.
func (a AllocatableCapacity) IsZero() bool {
	return a.ExecutorSlots == 0 && a.CPUUnits == 0 && a.MemoryMB == 0 && a.ScratchMB == 0
}

// Validate rejects negative totals.
func (a AllocatableCapacity) Validate() error {
	if a.ExecutorSlots < 0 || a.CPUUnits < 0 || a.MemoryMB < 0 || a.ScratchMB < 0 {
		return fmt.Errorf("allocatable capacity must not be negative")
	}
	return nil
}

// ReservedCapacity is the part of AllocatableCapacity currently committed to
// steward reservations on one worker.
type ReservedCapacity struct {
	ExecutorSlots int     `json:"executorSlots,omitempty"`
	CPUUnits      float64 `json:"cpuUnits,omitempty"`
	MemoryMB      int     `json:"memoryMb,omitempty"`
	ScratchMB     int     `json:"scratchMb,omitempty"`
}

// CPUPressure is a worker's live observation of itself. It is an observation
// and nothing else: it feeds preference scoring and staleness checks, it never
// redefines the CPU class, and it never raises allocatable capacity. Raw load
// average alone is not the resource model, so Normalized carries a bounded
// pressure figure and Load is retained only as supporting observation.
type CPUPressure struct {
	Normalized float64 `json:"normalized"`
	Load       float64 `json:"load,omitempty"`
}

// Headroom is the scoring view of pressure, bounded to [0,1]. It describes how
// much of the worker is currently idle, not how much may be reserved.
func (p CPUPressure) Headroom() float64 {
	switch {
	case p.Normalized <= 0:
		return 1
	case p.Normalized >= 1:
		return 0
	default:
		return 1 - p.Normalized
	}
}

// Validate rejects a normalized pressure outside [0,1] and a negative load.
func (p CPUPressure) Validate() error {
	if p.Normalized < 0 || p.Normalized > 1 {
		return fmt.Errorf("normalized cpu pressure %v must be between 0 and 1", p.Normalized)
	}
	if p.Load < 0 {
		return fmt.Errorf("cpu load %v must not be negative", p.Load)
	}
	return nil
}

// WorkerCapacitySnapshot is one worker's observation of itself at an instant.
// It is owned by the worker, admitted by the coordinator's snapshot-accept
// transaction, and immutable once accepted. Its identity is the worker ID, the
// worker epoch and the sequence; a snapshot from a superseded epoch or past
// its freshness bound is not a placement input.
//
// The three separate facts appear here as three separate fields: CPUClass is
// the static capability floor, Allocatable is what may be reserved, and
// Pressure is the live observation.
type WorkerCapacitySnapshot struct {
	WorkerID    string              `json:"workerId"`
	WorkerEpoch string              `json:"workerEpoch,omitempty"`
	Sequence    int64               `json:"sequence,omitempty"`
	CPUClass    CPUClass            `json:"cpuClass,omitempty"`
	Allocatable AllocatableCapacity `json:"allocatable"`
	Reserved    ReservedCapacity    `json:"reserved"`
	Pressure    CPUPressure         `json:"pressure"`
	ObservedAt  time.Time           `json:"observedAt"`
}

// Validate checks the snapshot's identity and its three facts.
func (s WorkerCapacitySnapshot) Validate() error {
	if strings.TrimSpace(s.WorkerID) == "" {
		return fmt.Errorf("capacity snapshot requires a worker ID")
	}
	if err := s.CPUClass.Validate(); err != nil {
		return fmt.Errorf("capacity snapshot for worker %q: %w", s.WorkerID, err)
	}
	if err := s.Allocatable.Validate(); err != nil {
		return fmt.Errorf("capacity snapshot for worker %q: %w", s.WorkerID, err)
	}
	if err := s.Pressure.Validate(); err != nil {
		return fmt.Errorf("capacity snapshot for worker %q: %w", s.WorkerID, err)
	}
	if s.Reserved.ExecutorSlots < 0 || s.Reserved.CPUUnits < 0 ||
		s.Reserved.MemoryMB < 0 || s.Reserved.ScratchMB < 0 {
		return fmt.Errorf("capacity snapshot for worker %q has negative reservations", s.WorkerID)
	}
	if s.Sequence < 0 {
		return fmt.Errorf("capacity snapshot for worker %q has a negative sequence", s.WorkerID)
	}
	return nil
}

// Fresh reports whether the snapshot may still be used to admit work.
func (s WorkerCapacitySnapshot) Fresh(now time.Time, maxAge time.Duration) bool {
	if s.ObservedAt.IsZero() || maxAge <= 0 {
		return false
	}
	return now.Sub(s.ObservedAt) <= maxAge
}

// FreeSlots is allocatable executor slots minus reserved ones, never negative.
func (s WorkerCapacitySnapshot) FreeSlots() int {
	if free := s.Allocatable.ExecutorSlots - s.Reserved.ExecutorSlots; free > 0 {
		return free
	}
	return 0
}

// FreeCPUUnits is allocatable CPU units minus reserved ones, never negative.
// It is derived from configured capacity and committed reservations only, and
// never from observed pressure.
func (s WorkerCapacitySnapshot) FreeCPUUnits() float64 {
	if free := s.Allocatable.CPUUnits - s.Reserved.CPUUnits; free > 0 {
		return free
	}
	return 0
}

// FreeMemoryMB is allocatable memory minus reserved memory, never negative.
func (s WorkerCapacitySnapshot) FreeMemoryMB() int {
	if free := s.Allocatable.MemoryMB - s.Reserved.MemoryMB; free > 0 {
		return free
	}
	return 0
}

// FreeScratchMB is allocatable scratch minus reserved scratch, never negative.
func (s WorkerCapacitySnapshot) FreeScratchMB() int {
	if free := s.Allocatable.ScratchMB - s.Reserved.ScratchMB; free > 0 {
		return free
	}
	return 0
}

// ExecutorPool is one worker's configured concurrency envelope: how many
// executor slots may exist and the allocatable totals those slots draw from.
// It is coordinator-owned catalog state and changes only with a catalog
// revision. Its slot count is a configured floor to raise, not a fleet-wide
// constant, and it is independent of any provider-session concurrency limit.
type ExecutorPool struct {
	WorkerID        string              `json:"workerId"`
	Name            string              `json:"name"`
	CatalogRevision string              `json:"catalogRevision,omitempty"`
	CPUClass        CPUClass            `json:"cpuClass,omitempty"`
	Allocatable     AllocatableCapacity `json:"allocatable"`
}

// Validate requires an identified pool with at least one executor slot.
func (p ExecutorPool) Validate() error {
	if strings.TrimSpace(p.WorkerID) != p.WorkerID || p.WorkerID == "" {
		return fmt.Errorf("executor pool requires a trimmed worker ID")
	}
	if strings.TrimSpace(p.Name) != p.Name || p.Name == "" {
		return fmt.Errorf("executor pool on worker %q requires a trimmed name", p.WorkerID)
	}
	if err := p.CPUClass.Validate(); err != nil {
		return fmt.Errorf("executor pool %q on worker %q: %w", p.Name, p.WorkerID, err)
	}
	if err := p.Allocatable.Validate(); err != nil {
		return fmt.Errorf("executor pool %q on worker %q: %w", p.Name, p.WorkerID, err)
	}
	if p.Allocatable.ExecutorSlots < 1 {
		return fmt.Errorf("executor pool %q on worker %q requires at least one executor slot", p.Name, p.WorkerID)
	}
	return nil
}

// ExecutorSlotState is the occupancy of one independently fenced slot.
type ExecutorSlotState string

const (
	ExecutorSlotFree        ExecutorSlotState = "free"
	ExecutorSlotReserved    ExecutorSlotState = "reserved"
	ExecutorSlotPreparing   ExecutorSlotState = "preparing"
	ExecutorSlotRunning     ExecutorSlotState = "running"
	ExecutorSlotFinishing   ExecutorSlotState = "finishing"
	ExecutorSlotQuarantined ExecutorSlotState = "quarantined"
)

// ExecutorSlot is one execution position on a worker. Concurrent attempts hold
// different slots and share nothing implicitly: each slot owns its own
// execution identity, workspace, lease, logs and staging area. The fencing
// token is reissued on every acquisition, so a stale holder cannot act on a
// recycled ordinal.
type ExecutorSlot struct {
	WorkerID      string            `json:"workerId"`
	PoolName      string            `json:"poolName"`
	Ordinal       int               `json:"ordinal"`
	State         ExecutorSlotState `json:"state"`
	FencingToken  string            `json:"fencingToken,omitempty"`
	ReservationID string            `json:"reservationId,omitempty"`
	AssignmentID  string            `json:"assignmentId,omitempty"`
	UpdatedAt     time.Time         `json:"updatedAt"`
}

// ID is the stable slot identity without its fencing token.
func (s ExecutorSlot) ID() string {
	return fmt.Sprintf("%s/%s/%d", s.WorkerID, s.PoolName, s.Ordinal)
}

// Occupied reports whether the slot currently counts against capacity.
func (s ExecutorSlot) Occupied() bool {
	return s.State != ExecutorSlotFree
}

// ResourceReservationState is the coordinator's durable knowledge of one
// lease-bound capacity commitment.
type ResourceReservationState string

const (
	ResourceReservationHeld      ResourceReservationState = "held"
	ResourceReservationActive    ResourceReservationState = "active"
	ResourceReservationReleasing ResourceReservationState = "releasing"
	ResourceReservationReleased  ResourceReservationState = "released"
	ResourceReservationOrphaned  ResourceReservationState = "orphaned"
)

// ResourceReleaseReason names one terminal path that returns capacity. The set
// is closed: every reservation is released exactly once through one of these,
// and a release without one of them is not a release.
type ResourceReleaseReason string

const (
	ResourceReleaseSettled           ResourceReleaseReason = "settled"
	ResourceReleaseCancelled         ResourceReleaseReason = "cancelled"
	ResourceReleasePreparationFailed ResourceReleaseReason = "preparation-failed"
	ResourceReleaseLeaseRecovered    ResourceReleaseReason = "lease-recovered"
)

// ResourceReleaseReasons lists the terminal release paths in a stable order.
func ResourceReleaseReasons() []ResourceReleaseReason {
	return []ResourceReleaseReason{
		ResourceReleaseSettled,
		ResourceReleaseCancelled,
		ResourceReleasePreparationFailed,
		ResourceReleaseLeaseRecovered,
	}
}

// Valid reports whether the reason is one of the enumerated terminal paths.
func (r ResourceReleaseReason) Valid() bool {
	for _, candidate := range ResourceReleaseReasons() {
		if r == candidate {
			return true
		}
	}
	return false
}

// ResourceReservation is a lease-bound commitment of allocatable capacity on
// one worker. It is coordinator-owned, acquired in the same transaction that
// commits its assignment, and released exactly once.
type ResourceReservation struct {
	ID               string                   `json:"id"`
	WorkerID         string                   `json:"workerId"`
	PoolName         string                   `json:"poolName"`
	SlotOrdinal      int                      `json:"slotOrdinal"`
	SlotFencingToken string                   `json:"slotFencingToken"`
	AssignmentID     string                   `json:"assignmentId,omitempty"`
	AttemptID        string                   `json:"attemptId,omitempty"`
	LeaseToken       string                   `json:"leaseToken,omitempty"`
	CPUUnits         float64                  `json:"cpuUnits,omitempty"`
	MemoryMB         int                      `json:"memoryMb,omitempty"`
	ScratchMB        int                      `json:"scratchMb,omitempty"`
	State            ResourceReservationState `json:"state"`
	ReleaseReason    ResourceReleaseReason    `json:"releaseReason,omitempty"`
	LeaseExpiresAt   time.Time                `json:"leaseExpiresAt,omitempty"`
	CreatedAt        time.Time                `json:"createdAt"`
	UpdatedAt        time.Time                `json:"updatedAt"`
	ReleasedAt       *time.Time               `json:"releasedAt,omitempty"`
}

// Released reports whether this reservation has already returned its capacity.
func (r ResourceReservation) Released() bool {
	return r.State == ResourceReservationReleased
}

// Validate checks a reservation's identity and sizes.
func (r ResourceReservation) Validate() error {
	if strings.TrimSpace(r.ID) != r.ID || r.ID == "" {
		return fmt.Errorf("resource reservation requires a trimmed ID")
	}
	if strings.TrimSpace(r.WorkerID) == "" || strings.TrimSpace(r.PoolName) == "" {
		return fmt.Errorf("resource reservation %q requires a worker and pool", r.ID)
	}
	if r.SlotOrdinal < 0 {
		return fmt.Errorf("resource reservation %q has a negative slot ordinal", r.ID)
	}
	if r.SlotFencingToken == "" {
		return fmt.Errorf("resource reservation %q requires a slot fencing token", r.ID)
	}
	if r.CPUUnits < 0 || r.MemoryMB < 0 || r.ScratchMB < 0 {
		return fmt.Errorf("resource reservation %q has negative sizes", r.ID)
	}
	if r.State == ResourceReservationReleased && !r.ReleaseReason.Valid() {
		return fmt.Errorf("resource reservation %q is released without a terminal reason", r.ID)
	}
	return nil
}

// CapacitySnapshotRef identifies one snapshot a placement decision read, so
// that the decision can be re-examined after a restart without the snapshot
// itself being retained.
type CapacitySnapshotRef struct {
	WorkerID    string    `json:"workerId"`
	WorkerEpoch string    `json:"workerEpoch,omitempty"`
	Sequence    int64     `json:"sequence,omitempty"`
	ObservedAt  time.Time `json:"observedAt"`
}

// PlacementRejection is one hard constraint that removed one worker. Placement
// records every rejection, not the first, because a batch author needs the
// complete reason a task is unplaceable.
type PlacementRejection struct {
	WorkerID string `json:"workerId"`
	Code     string `json:"code"`
	Detail   string `json:"detail"`
}

// PlacementScoreComponent is one named contribution to a preference score. A
// component with zero weight is a declared seam: it is named and reported, and
// it does not influence the ranking until an operator policy weights it.
type PlacementScoreComponent struct {
	Name   string  `json:"name"`
	Weight float64 `json:"weight"`
	Value  float64 `json:"value"`
}

// PlacementScore is the preference score of one surviving candidate.
type PlacementScore struct {
	WorkerID   string                    `json:"workerId"`
	Total      float64                   `json:"total"`
	Components []PlacementScoreComponent `json:"components,omitempty"`
}

// PlacementDecision is the durable explanation of one placement: the
// candidates considered, every constraint that rejected a worker, the
// preference score of every survivor, the snapshots the decision read, the
// selected worker and the reservation acquired for it. It is written once by
// the coordinator planner, committed with the assignment it justifies, and is
// immutable afterwards. A submitter never authors or mutates it.
//
// A failed placement is recorded with no selected worker and no reservation,
// so "why is this still queued" and "why did this land here" are answered from
// the same durable evidence.
type PlacementDecision struct {
	TaskID           string                `json:"taskId,omitempty"`
	AttemptID        string                `json:"attemptId,omitempty"`
	AssignmentID     string                `json:"assignmentId,omitempty"`
	SelectedWorkerID string                `json:"selectedWorkerId,omitempty"`
	ReservationID    string                `json:"reservationId,omitempty"`
	Demand           ResourceDemand        `json:"demand"`
	CandidateIDs     []string              `json:"candidateIds,omitempty"`
	Rejections       []PlacementRejection  `json:"rejections,omitempty"`
	Scores           []PlacementScore      `json:"scores,omitempty"`
	Snapshots        []CapacitySnapshotRef `json:"snapshots,omitempty"`
	DecidedAt        time.Time             `json:"decidedAt"`
}

// Placed reports whether the decision selected a worker.
func (d PlacementDecision) Placed() bool { return d.SelectedWorkerID != "" }

// Validate checks that the explanation is self-consistent: a selected worker
// must have been a candidate, must have a score, and must not also have been
// rejected.
func (d PlacementDecision) Validate() error {
	if err := d.Demand.Validate(); err != nil {
		return fmt.Errorf("placement decision: %w", err)
	}
	if d.SelectedWorkerID == "" {
		if d.ReservationID != "" {
			return fmt.Errorf("placement decision holds reservation %q without a selected worker", d.ReservationID)
		}
		return nil
	}
	candidate := false
	for _, id := range d.CandidateIDs {
		if id == d.SelectedWorkerID {
			candidate = true
			break
		}
	}
	if !candidate {
		return fmt.Errorf("placement decision selected worker %q that was not a candidate", d.SelectedWorkerID)
	}
	for _, rejection := range d.Rejections {
		if rejection.WorkerID == d.SelectedWorkerID {
			return fmt.Errorf("placement decision selected worker %q that constraint %q rejected",
				d.SelectedWorkerID, rejection.Code)
		}
	}
	for _, score := range d.Scores {
		if score.WorkerID == d.SelectedWorkerID {
			return nil
		}
	}
	return fmt.Errorf("placement decision selected worker %q without a preference score", d.SelectedWorkerID)
}
