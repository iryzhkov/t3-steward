package domain

// FoldQuotaEvidence combines numeric restrictions, source provenance and origin
// expiry for already selected tied readings. Callers retain numeric validity,
// epoch/phase checks and their separate runway rules.
func FoldQuotaEvidence(current, candidate BucketState) BucketState {
	if candidate.UsedPercent > current.UsedPercent {
		current.UsedPercent = candidate.UsedPercent
	}
	if candidate.RatePerMinute > current.RatePerMinute {
		current.RatePerMinute = candidate.RatePerMinute
	}
	if !SameQuotaAcquisition(current.Provenance, candidate.Provenance) {
		current.Provenance = nil
	}
	if candidate.OriginValidUntil != nil && (current.OriginValidUntil == nil || candidate.OriginValidUntil.Before(*current.OriginValidUntil)) {
		current.OriginValidUntil = candidate.OriginValidUntil
	}
	return current
}
