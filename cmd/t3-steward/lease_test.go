package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestLeaseCLIIdentityAndVerdicts(t *testing.T) {
	for _, action := range []string{"acquire", "renew", "release", "check", "show", "list"} {
		t.Run(action, func(t *testing.T) {
			var out bytes.Buffer
			var got domain.LeaseRequest
			cli := leaseCLI{stdout: &out, resolve: func(explicit string) (string, error) {
				if explicit != "" {
					t.Fatalf("current passed as explicit: %q", explicit)
				}
				return "canonical-thread", nil
			}, exchange: func(_ context.Context, r domain.LeaseRequest) (domain.LeaseResponse, error) {
				got = r
				return domain.LeaseResponse{Code: 10, Message: "held by other"}, nil
			}}
			args := []string{action}
			if action != "list" {
				args = append(args, "repo:Project/main")
			}
			args = append(args, "--json")
			if action == "acquire" {
				args = append(args, "--reason", "integrate")
			}
			if action == "renew" || action == "release" {
				args = append(args, "--token", "7")
			}
			err := cli.run(context.Background(), args)
			if exitCodeFor(err) != 10 || !strings.Contains(out.String(), "\"code\":10") {
				t.Fatalf("verdict %v output %s", err, &out)
			}
			if got.Action != action || (action != "show" && action != "list" && got.OwnerThread != "canonical-thread") {
				t.Fatalf("request %#v", got)
			}
			if action == "acquire" && got.TTL != 2*time.Hour {
				t.Fatalf("ttl %v", got.TTL)
			}
			if got.Mutating() && got.RequestID == "" {
				t.Fatal("missing idempotency identity")
			}
		})
	}
}
func TestLeaseCLIExplicitIdentityAndUnresolvedCurrent(t *testing.T) {
	var out bytes.Buffer
	called := false
	cli := leaseCLI{stdout: &out, resolve: func(v string) (string, error) {
		if v == "explicit" {
			return v, nil
		}
		return "", errors.New("unresolved")
	}, exchange: func(_ context.Context, r domain.LeaseRequest) (domain.LeaseResponse, error) {
		called = true
		if r.OwnerThread != "explicit" {
			t.Fatal(r)
		}
		return domain.LeaseResponse{Code: 11, Message: "free"}, nil
	}}
	if err := cli.run(context.Background(), []string{"check", "repo:p/main", "--owner-thread", "explicit", "--json"}); exitCodeFor(err) != 11 {
		t.Fatal(err)
	}
	called = false
	if err := cli.run(context.Background(), []string{"check", "repo:p/main"}); err == nil || !strings.Contains(err.Error(), "--owner-thread") {
		t.Fatalf("identity error %v", err)
	}
	if called {
		t.Fatal("unresolved identity sent")
	}
}
func TestLeaseCLISuccessAndTransportFailure(t *testing.T) {
	var out bytes.Buffer
	cli := leaseCLI{stdout: &out, resolve: func(string) (string, error) { return "thread", nil }, exchange: func(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error) {
		return domain.LeaseResponse{Code: 0}, nil
	}}
	if err := cli.run(context.Background(), []string{"check", "repo:p/main", "--json"}); err != nil || !strings.Contains(out.String(), "\"code\":0") {
		t.Fatalf("%v %s", err, &out)
	}
	out.Reset()
	cli.exchange = func(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error) {
		return domain.LeaseResponse{}, &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable, Err: errors.New("offline")}
	}
	if err := cli.run(context.Background(), []string{"check", "repo:p/main", "--json"}); exitCodeFor(err) != 5 {
		t.Fatalf("failed open: %v", err)
	}
	if out.Len() != 0 {
		t.Fatal("transport error printed success response")
	}
}

func TestLeaseCLIRejectsMalformedBeforeExchange(t *testing.T) {
	for _, args := range [][]string{
		{"acquire", "repo:p/main", "--reason", "why", "--ttl", "4m"},
		{"acquire", "repo:p/main", "--reason", "why", "--ttl", "0"},
		{"acquire", "deploy:p", "--reason", "why"},
		{"renew", "repo:p/main"},
		{"release", "repo:p/main", "--force"},
		{"check", "repo:p/main", "--bogus"},
		{"check", "repo:p/main", "extra"},
	} {
		cli := leaseCLI{stdout: &bytes.Buffer{}, resolve: func(string) (string, error) { return "thread", nil }, exchange: func(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error) {
			t.Fatal("invalid command sent")
			return domain.LeaseResponse{}, nil
		}}
		if err := cli.run(context.Background(), args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
