package backlogadmin

import (
	"context"
	"errors"
	"fmt"
)

// extendedQueryVersions are the versions that opt a v1 read of kind into the
// fields a strict older client would reject, newest first. A coordinator
// older than this release refuses the first, and one older than rc.116 every
// one of them.
func extendedQueryVersions(kind QueryKind) []string {
	if kind == QueryStatus {
		return []string{StatusIntakeVersion}
	}
	return []string{CurrentReadVersion, ExtendedReadVersion}
}

// queryExtended negotiates a v1 read. Each retry is bounded, read-only, and
// uses the original query with the next older version, and v1 last. No other
// error permits fallback.
func queryExtended(ctx context.Context, query Query, ask func(context.Context, Query) (Response, error)) (Response, error) {
	if query.Version != Version {
		return ask(ctx, query)
	}
	for _, version := range extendedQueryVersions(query.Kind) {
		extended := query
		extended.Version = version
		response, err := ask(ctx, extended)
		if !unsupportedVersionRefusal(err, version) {
			return response, err
		}
	}
	response, err := ask(ctx, query)
	if err == nil {
		err = legacyProjection(&response)
	}
	return response, err
}

// unsupportedVersionRefusal reports whether err is exactly a coordinator's
// refusal of version as unknown.
func unsupportedVersionRefusal(err error, version string) bool {
	var transportErr *TransportError
	unsupported := fmt.Sprintf("%s: got %q, want %q", ErrUnsupportedVersion, version, Version)
	return errors.As(err, &transportErr) && transportErr.Class == ClassRejected &&
		transportErr.Err != nil && transportErr.Err.Error() == unsupported
}

// legacyProjection clears the fields only an extended read carries, so a v1
// response has the frozen shape whichever side produced it.
func legacyProjection(response *Response) error {
	if response.Status != nil {
		response.Status.Runtime.LegacyFileIntake = ""
	}
	return projectV1Response(response)
}
