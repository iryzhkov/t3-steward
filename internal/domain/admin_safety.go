package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

// AdminSafetyState is the complete durable input used to recheck whether an
// attempt may start or resume. Its fingerprint fences planning against changes
// before the command transition is committed.
type AdminSafetyState struct {
	Tasks           []Task                 `json:"tasks"`
	Attempts        []Attempt              `json:"attempts"`
	Assignments     []Assignment           `json:"assignments"`
	QuotaPools      []QuotaPool            `json:"quotaPools"`
	Workers         []WorkerSnapshot       `json:"workers"`
	QuotaAdmissions []QuotaAdmissionRecord `json:"quotaAdmissions"`
}

// AdminSafetyFingerprint returns an order-independent fingerprint of the
// durable safety inputs used by admin start and resume policy.
func AdminSafetyFingerprint(state AdminSafetyState) (string, error) {
	state.Tasks = append([]Task(nil), state.Tasks...)
	state.Attempts = append([]Attempt(nil), state.Attempts...)
	state.Assignments = append([]Assignment(nil), state.Assignments...)
	state.QuotaPools = append([]QuotaPool(nil), state.QuotaPools...)
	state.Workers = append([]WorkerSnapshot(nil), state.Workers...)
	state.QuotaAdmissions = append([]QuotaAdmissionRecord(nil), state.QuotaAdmissions...)
	sort.Slice(state.Tasks, func(i, j int) bool { return state.Tasks[i].ID < state.Tasks[j].ID })
	sort.Slice(state.Attempts, func(i, j int) bool { return state.Attempts[i].ID < state.Attempts[j].ID })
	sort.Slice(state.Assignments, func(i, j int) bool { return state.Assignments[i].ID < state.Assignments[j].ID })
	sort.Slice(state.QuotaPools, func(i, j int) bool { return state.QuotaPools[i].ID < state.QuotaPools[j].ID })
	sort.Slice(state.Workers, func(i, j int) bool { return state.Workers[i].WorkerID < state.Workers[j].WorkerID })
	sort.Slice(state.QuotaAdmissions, func(i, j int) bool {
		return state.QuotaAdmissions[i].QuotaPoolID < state.QuotaAdmissions[j].QuotaPoolID
	})
	raw, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// AdminWorkerSafetyValidUntil returns a conservative validity deadline for the
// fresh, connected workers that can support admin start or resume policy.
func AdminWorkerSafetyValidUntil(workers []WorkerSnapshot, at time.Time) (time.Time, bool) {
	var validUntil time.Time
	for _, worker := range workers {
		if !worker.Connected || at.After(worker.ValidUntil) ||
			!worker.Inventory.AcceptBacklog || worker.Inventory.Health != WorkerHealthReady {
			continue
		}
		if validUntil.IsZero() || worker.ValidUntil.Before(validUntil) {
			validUntil = worker.ValidUntil
		}
	}
	return validUntil, !validUntil.IsZero()
}
