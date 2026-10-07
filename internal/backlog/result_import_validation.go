package backlog

// ResultValidationError marks a worker result whose content violates the import
// contract: an output the task did not declare, a media type its declaration
// does not allow, malformed evidence, or an invalid recovery proposal.
//
// The upload it came from is immutable, so the same result fails the same way
// on every retry. The importer therefore settles the attempt through
// rejectResult instead of returning it as an ordinary error, which the caller
// would retry forever while every later result from that worker waited behind
// it. Failures to read the upload or to write the store are not validation
// errors and stay retryable.
type ResultValidationError struct {
	Err error
}

func (e *ResultValidationError) Error() string { return e.Err.Error() }

func (e *ResultValidationError) Unwrap() error { return e.Err }
