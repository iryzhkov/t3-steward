// Package source composes independent quota observation sources.
package source

import (
	"context"
	"errors"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type Source interface {
	Run(context.Context, chan<- domain.QuotaSnapshot) error
}

type group struct{ sources []Source }

// Compose keeps each source's timestamps and bucket identities unchanged.
// The caller owns the output channel. A terminal source error cancels and joins
// the other sources; acquisition failures handled inside a source stay local.
func Compose(sources ...Source) Source {
	if len(sources) == 1 {
		return sources[0]
	}
	return &group{sources: append([]Source(nil), sources...)}
}

func (g *group) Run(ctx context.Context, output chan<- domain.QuotaSnapshot) error {
	if len(g.sources) == 0 {
		return errors.New("quota sources: no sources configured")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(g.sources))
	for _, s := range g.sources {
		go func(s Source) { results <- s.Run(ctx, output) }(s)
	}
	var first error
	for range g.sources {
		err := <-results
		if err == nil && ctx.Err() == nil {
			err = errors.New("quota sources: source exited")
		}
		if first == nil && err != nil {
			first = err
			cancel()
		}
	}
	return first
}
