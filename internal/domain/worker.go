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
type WorkerInventory struct {
	ID            string                    `json:"id"`
	AcceptBacklog bool                      `json:"acceptBacklog"`
	Health        WorkerHealth              `json:"health"`
	Capabilities  []string                  `json:"capabilities,omitempty"`
	Projects      []WorkerProjectInventory  `json:"projects,omitempty"`
	Providers     []WorkerProviderInventory `json:"providers,omitempty"`
	WebBaseURL    string                    `json:"webBaseUrl,omitempty"`
	ObservedAt    time.Time                 `json:"observedAt"`
}
