package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Self-review of fix round 1: the store replays the first answer for a
// request id verbatim, so replaying an acquire whose lease has since expired
// (and may now belong to another thread) printed "held by" and exited 0.
func TestLeaseCLIRefusesReplayedGrantThatHasExpired(t *testing.T) {
	now := time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)
	for _, action := range []string{"acquire", "renew"} {
		for _, asJSON := range []bool{false, true} {
			var out bytes.Buffer
			cli := leaseCLI{stdout: &out, now: func() time.Time { return now }, resolve: func(string) (string, error) { return "thread-a", nil },
				exchange: func(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error) {
					return domain.LeaseResponse{Replay: true, Lease: &domain.Lease{Name: "repo:s/main", OwnerThread: "thread-a", Token: 1, ExpiresAt: now.Add(-time.Hour)}}, nil
				}}
			args := []string{action, "repo:s/main", "--reason", "r", "--request-id", "lost-a"}
			if action == "renew" {
				args = append(args, "--token", "1")
			}
			if asJSON {
				args = append(args, "--json")
			}
			err := cli.run(context.Background(), args)
			if exitCodeFor(err) != 10 || strings.Contains(out.String(), "held by thread") || !strings.Contains(out.String(), "new --request-id") {
				t.Fatalf("%s json=%v: exit %d, stdout %q, err %v", action, asJSON, exitCodeFor(err), out.String(), err)
			}
		}
	}
	// A replayed grant that is still live is the ordinary idempotent answer.
	var out bytes.Buffer
	cli := leaseCLI{stdout: &out, now: func() time.Time { return now }, resolve: func(string) (string, error) { return "thread-a", nil },
		exchange: func(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error) {
			return domain.LeaseResponse{Replay: true, Lease: &domain.Lease{Name: "repo:s/main", OwnerThread: "thread-a", Token: 1, ExpiresAt: now.Add(time.Hour)}}, nil
		}}
	if err := cli.run(context.Background(), []string{"acquire", "repo:s/main", "--reason", "r", "--request-id", "lost-a"}); err != nil || !strings.Contains(out.String(), "held by thread thread-a token 1") {
		t.Fatalf("live replay: %q %v", out.String(), err)
	}
}
