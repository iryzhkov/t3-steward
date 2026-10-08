package domain

// GoverningDemand is the part of a demand a worker's configured capacity can
// account for: each size the worker declares no allocatable capacity for is
// dropped, and the CPU classes are kept.
//
// A sized demand on a worker that declares neither cpu units nor memory is
// therefore counted against its executor slots only, exactly as an unsized
// demand is, and a worker that declares some dimensions is checked on those it
// declares. Without this rule every sized task, and every task of a sized
// preset, would be refused by a worker that configures slots alone.
//
// The demand committed with an assignment stays the full declaration; this
// projection is applied wherever a demand meets a worker's capacity.
func GoverningDemand(demand ResourceDemand, allocatable AllocatableCapacity) ResourceDemand {
	if allocatable.CPUUnits == 0 {
		demand.CPUUnits = 0
	}
	if allocatable.MemoryMB == 0 {
		demand.MemoryMB = 0
	}
	if allocatable.ScratchMB == 0 {
		demand.ScratchMB = 0
	}
	return demand
}

// CountsSizedDemandAsSlot reports whether a worker accounts a sized demand
// against its executor slots only, because it declares neither cpu units nor
// memory.
func CountsSizedDemandAsSlot(demand ResourceDemand, allocatable AllocatableCapacity) bool {
	return (demand.CPUUnits > 0 || demand.MemoryMB > 0) && allocatable.CPUUnits == 0 && allocatable.MemoryMB == 0
}
