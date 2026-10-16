package source

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type sourceFunc func(context.Context, chan<- domain.QuotaSnapshot) error

func (f sourceFunc) Run(ctx context.Context, out chan<- domain.QuotaSnapshot) error {
	return f(ctx, out)
}

func TestComposePreservesEvidenceAndJoinsSources(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan domain.QuotaSnapshot, 2)
	joined := make(chan string, 2)
	makeSource := func(id string) Source {
		return sourceFunc(func(ctx context.Context, out chan<- domain.QuotaSnapshot) error {
			s := domain.QuotaSnapshot{Key: domain.BucketKey{AccountID: id, Window: "five_hour"}, SourceEventID: id, ObservedAt: time.Unix(123, 0)}
			select {
			case out <- s:
			case <-ctx.Done():
				return ctx.Err()
			}
			<-ctx.Done()
			joined <- id
			return ctx.Err()
		})
	}
	done := make(chan error, 1)
	go func() { done <- Compose(makeSource("a"), makeSource("b")).Run(ctx, out) }()
	got := map[string]bool{}
	for range 2 {
		s := <-out
		if s.ObservedAt != time.Unix(123, 0) || s.SourceEventID != s.Key.AccountID {
			t.Fatal("evidence changed")
		}
		got[s.Key.AccountID] = true
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(got) != 2 || len(joined) != 2 {
		t.Fatal("sources not joined")
	}
	// The source never closes its caller's channel.
	out <- domain.QuotaSnapshot{}
}
func TestComposeUnexpectedExitIsTerminal(t *testing.T) {
	t.Parallel()
	finished := sourceFunc(func(context.Context, chan<- domain.QuotaSnapshot) error { return nil })
	blocked := sourceFunc(func(ctx context.Context, _ chan<- domain.QuotaSnapshot) error { <-ctx.Done(); return ctx.Err() })
	if err := Compose(finished, blocked).Run(context.Background(), make(chan domain.QuotaSnapshot)); err == nil {
		t.Fatal("unexpected source exit was hidden")
	}
}

func TestComposeTerminalFailureCancelsOtherSource(t *testing.T) {
	t.Parallel()
	ready := make(chan struct{})
	joined := make(chan struct{})
	sentinel := errors.New("terminal")
	slow := sourceFunc(func(ctx context.Context, _ chan<- domain.QuotaSnapshot) error {
		close(ready)
		<-ctx.Done()
		close(joined)
		return ctx.Err()
	})
	failed := sourceFunc(func(context.Context, chan<- domain.QuotaSnapshot) error { <-ready; return sentinel })
	if err := Compose(slow, failed).Run(context.Background(), make(chan domain.QuotaSnapshot)); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	select {
	case <-joined:
	default:
		t.Fatal("other source still running")
	}
}
