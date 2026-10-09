package main

import (
	"fmt"
	"strconv"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// validateIdempotencyKeyFlag applies the coordinator's rule for a submission
// key before anything is read or sent, so that a key the coordinator would
// refuse is refused offline, in --dry-run too, in the coordinator's words.
// The refusal shows the value escaped: a key with control bytes printed raw
// would rewrite the terminal reading it.
func validateIdempotencyKeyFlag(key string) error {
	if err := domain.ValidateIdempotencyKey(key); err != nil {
		return fmt.Errorf("--idempotency-key %q: %w", key, err)
	}
	return nil
}

// displayValue prints value as it is when that is safe to print and quoted
// with Go escapes when it holds anything that is not, such as a control byte.
func displayValue(value string) string {
	quoted := strconv.Quote(value)
	if quoted[1:len(quoted)-1] == value {
		return value
	}
	return quoted
}
