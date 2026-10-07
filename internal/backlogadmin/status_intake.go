package backlogadmin

import (
	"context"
	"errors"
	"fmt"
)

// extendedQueryVersion is the version that opts a v1 read of kind into the
// fields a strict older client would reject.
func extendedQueryVersion(kind QueryKind) string {
	if kind == QueryStatus {
		return StatusIntakeVersion
	}
	return ExtendedReadVersion
}

// queryExtended negotiates a v1 read. The retry is bounded, read-only, and
// uses the original query. No other error permits fallback.
func queryExtended(ctx context.Context, query Query, ask func(context.Context, Query) (Response, error)) (Response, error) {
	if query.Version != Version {
		return ask(ctx, query)
	}
	version := extendedQueryVersion(query.Kind)
	extended := query
	extended.Version = version
	response, err := ask(ctx, extended)
	var transportErr *TransportError
	unsupported := fmt.Sprintf("%s: got %q, want %q", ErrUnsupportedVersion, version, Version)
	if errors.As(err, &transportErr) && transportErr.Class == ClassRejected &&
		transportErr.Err != nil && transportErr.Err.Error() == unsupported {
		response, err = ask(ctx, query)
		if err == nil {
			err = legacyProjection(&response)
		}
	}
	return response, err
}

// legacyProjection clears the fields only an extended read carries, so a v1
// response has the frozen shape whichever side produced it.
func legacyProjection(response *Response) error {
	if response.Status != nil {
		response.Status.Runtime.LegacyFileIntake = ""
	}
	return projectV1Response(response)
}
