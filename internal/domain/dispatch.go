package domain

// DispatchState is the coordinator's durable knowledge of one deterministic
// T3 thread creation.
type DispatchState string

const (
	DispatchPrepared  DispatchState = "prepared"
	DispatchCreating  DispatchState = "creating"
	DispatchConfirmed DispatchState = "confirmed"
	DispatchUnknown   DispatchState = "unknown"
	DispatchStopped   DispatchState = "stopped"
)

// AssignmentDispatchTransition atomically advances dispatch knowledge using an
// optimistic revision. Exact replay is a successful no-op.
type AssignmentDispatchTransition struct {
	ExpectedRevision int64      `json:"expectedRevision"`
	Assignment       Assignment `json:"assignment"`
}
