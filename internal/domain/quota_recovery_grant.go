package domain

import "time"

// QuotaRecoveryGrant is current source-qualified coordinator authority,
// transported only to peers that negotiated collector provenance. It never
// grants model authorization or changes an execution's identity/lease fences.
type QuotaRecoveryGrant struct {
	EvidenceScope            QuotaRecoveryEvidenceScope `json:"evidenceScope"`
	EvidenceDigest           string                     `json:"evidenceDigest"`
	OriginProviderInstanceID string                     `json:"originProviderInstanceId"`
	CoverageDigest           string                     `json:"coverageDigest"`
	QuotaPoolID              string                     `json:"quotaPoolId"`
	AccountID                string                     `json:"accountId"`
	StopCommandID            string                     `json:"stopCommandId,omitempty"`
	StoppedAt                time.Time                  `json:"stoppedAt"`
	StopObservedSequence     int64                      `json:"stopObservedSequence,omitempty"`
	Provenance               QuotaProvenance            `json:"provenance"`
	ExpiresAt                time.Time                  `json:"expiresAt"`
}

// QuotaStopObservation preserves the worker's actual durable local stop time,
// rather than replacing it with each snapshot's collection time.
type QuotaStopObservation struct {
	Bucket           BucketKey `json:"bucket"`
	Epoch            string    `json:"epoch"`
	StoppedAt        time.Time `json:"stoppedAt"`
	ObservedSequence int64     `json:"observedSequence"`
}
