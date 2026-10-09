package workerruntime

import (
	"errors"
	"sync"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// maxArchiveDecodePasses bounds the collection passes that read the same
// undecodable thread archive. An export is read again on a later pass in case
// T3 was still writing it; the same decode error that many times is
// deterministic, and the collection goes ahead without the archive's
// judgement instead of deferring forever.
const maxArchiveDecodePasses = 3

// archiveDecodeFailures counts, per execution, the consecutive collection
// passes that failed to decode the thread archive with the same error. It is
// process memory: a worker restart starts the count again, which keeps the
// bound and costs at most another few passes.
var archiveDecodeFailures = struct {
	sync.Mutex
	passes map[string]archiveDecodeCount
}{passes: map[string]archiveDecodeCount{}}

type archiveDecodeCount struct {
	err    string
	passes int
}

func archiveDecodeKey(pkg workerproto.ExecutionPackage) string {
	return pkg.Identity.AttemptID + "\x00" + pkg.Identity.DispatchToken
}

// archiveDecodeExhausted records one more pass that failed to decode the
// archive with err, and reports whether the bound is reached: the same error
// on maxArchiveDecodePasses consecutive passes. It returns the passes counted.
//
// The same error is the same decoding failure, whichever reader met it: a
// collection reads the archive for the current start request and then for
// the completion judgement, and the two readers name themselves differently.
func archiveDecodeExhausted(pkg workerproto.ExecutionPackage, err error) (bool, int) {
	key := archiveDecodeKey(pkg)
	cause := err.Error()
	var invalid *backlog.ThreadArchiveInvalidError
	if errors.As(err, &invalid) && invalid.Err != nil {
		cause = invalid.Err.Error()
	}
	archiveDecodeFailures.Lock()
	defer archiveDecodeFailures.Unlock()
	count := archiveDecodeFailures.passes[key]
	if count.err != cause {
		count = archiveDecodeCount{err: cause}
	}
	count.passes++
	archiveDecodeFailures.passes[key] = count
	return count.passes >= maxArchiveDecodePasses, count.passes
}

// forgetArchiveDecode drops the count of an execution whose archive decoded,
// or whose collection went ahead without it.
func forgetArchiveDecode(pkg workerproto.ExecutionPackage) {
	archiveDecodeFailures.Lock()
	delete(archiveDecodeFailures.passes, archiveDecodeKey(pkg))
	archiveDecodeFailures.Unlock()
}

// isThreadArchiveInvalid reports whether err is an archive Steward cannot
// decode, as opposed to one it could not read from T3.
func isThreadArchiveInvalid(err error) bool {
	var invalid *backlog.ThreadArchiveInvalidError
	return errors.As(err, &invalid)
}
