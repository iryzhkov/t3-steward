package backlogadmin

import (
	"context"
	"errors"
	"fmt"
)

// queryIntakeStatus negotiates only a v1 status read. The retry is bounded,
// read-only, and uses the original query. No other error permits fallback.
func queryIntakeStatus(ctx context.Context, query Query, ask func(context.Context, Query) (Response, error)) (Response, error) {
	if query.Kind != QueryStatus || query.Version != Version {
		return ask(ctx, query)
	}
	extended := query
	extended.Version = StatusIntakeVersion
	response, err := ask(ctx, extended)
	var transportErr *TransportError
	unsupported := fmt.Sprintf("%s: got %q, want %q", ErrUnsupportedVersion, StatusIntakeVersion, Version)
	if errors.As(err, &transportErr) && transportErr.Class == ClassRejected &&
		transportErr.Err != nil && transportErr.Err.Error() == unsupported {
		response, err = ask(ctx, query)
		if response.Status != nil {
			response.Status.Runtime.LegacyFileIntake = ""
		}
	}
	return response, err
}
