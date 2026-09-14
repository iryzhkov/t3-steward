package domain

import "time"

// WorkerHealth is a worker's self-reported ability to accept new assignments.
type WorkerHealth string

const (
	WorkerHealthReady    WorkerHealth = "ready"
	WorkerHealthDegraded WorkerHealth = "degraded"
	WorkerHealthOffline  WorkerHealth = "offline"
)

// WorkerProjectInventory reports whether one logical project can be prepared on a worker.
type WorkerProjectInventory struct {
	Name      string    `json:"name"`
	Available bool      `json:"available"`
	Revision  string    `json:"revision,omitempty"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
}

// WorkerProviderInventory reports one installed provider instance. Placement
// retains this inventory but does not use it; provider routing is a separate decision.
type WorkerProviderInventory struct {
	InstanceID  string   `json:"instanceId"`
	Models      []string `json:"models,omitempty"`
	QuotaPoolID string   `json:"quotaPoolId,omitempty"`
	Available   bool     `json:"available"`
}

// WorkerInventory is the coordinator's latest immutable view of a worker.
type WorkerRuntimeIdentity struct {
	Release         string    `json:"release"`
	Commit          string    `json:"commit"`
	BootstrapDigest string    `json:"bootstrapDigest"`
	LastReload      time.Time `json:"lastReload"`
}
type WorkerInventory struct {
	Runtime         *WorkerRuntimeIdentity    `json:"runtime,omitempty"`
	CatalogRevision string                    `json:"catalogRevision,omitempty"`
	ID              string                    `json:"id"`
	Epoch           string                    `json:"epoch,omitempty"`
	Sequence        int64                     `json:"sequence,omitempty"`
	AcceptBacklog   bool                      `json:"acceptBacklog"`
	Health          WorkerHealth              `json:"health"`
	Capabilities    []string                  `json:"capabilities,omitempty"`
	Projects        []WorkerProjectInventory  `json:"projects,omitempty"`
	Providers       []WorkerProviderInventory `json:"providers,omitempty"`
	WebBaseURL      string                    `json:"webBaseUrl,omitempty"`

	// CPUClass, Allocatable and Pressure are the three separate capacity
	// facts, deliberately not merged into one number. CPUClass is the static
	// operator-assigned capability floor, Allocatable is the configured total
	// the scheduler may reserve, Reserved is what is already committed, and
	// Pressure is the worker's live observation of itself. Pressure never
	// raises Allocatable and never redefines CPUClass.
	CPUClass    CPUClass            `json:"cpuClass,omitempty"`
	Allocatable AllocatableCapacity `json:"allocatable,omitempty"`
	Reserved    ReservedCapacity    `json:"reserved,omitempty"`
	Pressure    CPUPressure         `json:"pressure,omitempty"`

	ObservedAt time.Time `json:"observedAt"`
}

// CapacitySnapshot projects the three capacity facts this inventory carries
// into the snapshot identity placement reserves against. The projection copies
// facts; it never computes one fact from another.
func (w WorkerInventory) CapacitySnapshot() WorkerCapacitySnapshot {
	return WorkerCapacitySnapshot{
		WorkerID:    w.ID,
		WorkerEpoch: w.Epoch,
		Sequence:    w.Sequence,
		CPUClass:    w.CPUClass,
		Allocatable: w.Allocatable,
		Reserved:    w.Reserved,
		Pressure:    w.Pressure,
		ObservedAt:  w.ObservedAt,
	}
}
