package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestLeaseCLIReleasedRecordReportsRelease(t *testing.T) {
	var out bytes.Buffer
	cli := leaseCLI{stdout: &out, resolve: func(string) (string, error) { return "thread", nil }, exchange: func(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error) {
		return domain.LeaseResponse{Lease: &domain.Lease{Name: "repo:s/main", OwnerThread: "thread", Token: 7, Released: true}}, nil
	}}
	if err := cli.run(context.Background(), []string{"release", "repo:s/main", "--token", "7"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "released") || strings.Contains(out.String(), "held") {
		t.Fatalf("released record reported as held: %s", &out)
	}
}
