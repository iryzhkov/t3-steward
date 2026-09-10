package domain

import "time"

// ThrottleSeverity is the outward safety action requested for a quota pool.
type ThrottleSeverity string

const (
	ThrottleWarn  ThrottleSeverity = "warn"
	ThrottleDrain ThrottleSeverity = "drain"
	ThrottleStop  ThrottleSeverity = "stop"
)

// QuotaBucketEpoch binds an admission record and directive to one observed
// provider-window epoch.
type QuotaBucketEpoch struct {
	Bucket BucketKey `json:"bucket"`
	Epoch  string    `json:"epoch"`
}

// QuotaAdmissionRecord is the durable scheduler admission projection for one
// quota pool.
type QuotaAdmissionRecord struct {
	QuotaPoolID  string             `json:"quotaPoolId"`
	Revision     int64              `json:"revision"`
	Admission    AdmissionState     `json:"admission"`
	ObservedAt   time.Time          `json:"observedAt"`
	BucketEpochs []QuotaBucketEpoch `json:"bucketEpochs"`
	Reason       string             `json:"reason"`
	AppliedAt    time.Time          `json:"appliedAt"`
}

// ThrottleDirective becomes eligible for delivery only after the admission
// transition carrying it has committed.
type ThrottleDirective struct {
	ID                string             `json:"id"`
	QuotaPoolID       string             `json:"quotaPoolId"`
	AdmissionRevision int64              `json:"admissionRevision"`
	Severity          ThrottleSeverity   `json:"severity"`
	BucketEpochs      []QuotaBucketEpoch `json:"bucketEpochs"`
	Reason            string             `json:"reason"`
	Deadline          *time.Time         `json:"deadline,omitempty"`
	CreatedAt         time.Time          `json:"createdAt"`
}

// QuotaAdmissionTransition is one optimistic admission update and its optional
// outward directive. A store commits a batch atomically.
type QuotaAdmissionTransition struct {
	ExpectedRevision int64                `json:"expectedRevision"`
	Record           QuotaAdmissionRecord `json:"admission"`
	Directive        *ThrottleDirective   `json:"directive,omitempty"`
}
