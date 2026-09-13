//go:build !linux

package providercontainment

import (
	"context"
	"errors"
)

func run(context.Context, Spec, Streams) error {
	return errors.New("provider containment requires Linux namespaces and bubblewrap")
}
