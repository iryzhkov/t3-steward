package main

import (
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// failureClassText is the "class/code" of a failed or cancelled attempt, or
// empty for any other attempt. It reads the coordinator's recorded
// classification and classifies an older attempt from its reason.
func failureClassText(attempt *domain.Attempt) string {
	if attempt == nil {
		return ""
	}
	classification, ok := domain.ClassifyAttemptFailure(*attempt)
	if !ok {
		return ""
	}
	return classification.String()
}

// automaticRetryText describes an attempt the coordinator created to retry
// an infrastructure failure, or is empty for any other attempt.
func automaticRetryText(attempt *domain.Attempt) string {
	if attempt == nil || attempt.AutomaticRetry == nil {
		return ""
	}
	retry := attempt.AutomaticRetry
	return fmt.Sprintf("automatic retry %d of %d of attempt %s after %s/%s, not before %s",
		retry.Ordinal, retry.Budget, retry.SourceAttemptID, retry.Class, retry.Code,
		retry.NotBefore.UTC().Format(time.RFC3339))
}
