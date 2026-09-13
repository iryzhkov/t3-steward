package domain

import "time"

type WorkerEnrollmentRequest struct {
	ID               string `json:"id"`
	WorkerID         string `json:"workerId"`
	ExpectedRevision int64  `json:"expectedRevision"`
	CatalogRevision  string `json:"catalogRevision"`
	Reason           string `json:"reason"`
}
type WorkerEnrollment struct {
	Request          WorkerEnrollmentRequest `json:"request"`
	Revision         int64                   `json:"revision"`
	WorkerEpoch      string                  `json:"workerEpoch"`
	CoordinatorID    string                  `json:"coordinatorId"`
	CredentialRef    string                  `json:"credentialRef"`
	Principal        string                  `json:"principal"`
	Connection       string                  `json:"connection"`
	Actor            string                  `json:"actor"`
	EnrolledAt       time.Time               `json:"enrolledAt"`
	SnapshotSequence int64                   `json:"snapshotSequence"`
}
type WorkerRequirement struct {
	Draining        bool   `json:"draining,omitempty"`
	WorkerID        string `json:"workerId"`
	WorkerEpoch     string `json:"workerEpoch"`
	CatalogRevision string `json:"catalogRevision"`
	CredentialRef   string `json:"credentialRef"`
	Connection      string `json:"connection"`
}
