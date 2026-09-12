package backlog

import "errors"

// ErrResultImportSuperseded reports a worker upload for an attempt that already
// reached a different terminal outcome (for example it was cancelled while the
// worker was still collecting). The upload carries no new information and can
// be acknowledged without import.
var ErrResultImportSuperseded = errors.New("result import superseded")
