package workerruntime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestEnvironmentCredentialCheckerResolvesReferencesWithoutReturningValues(t *testing.T) {
	lookups := make([]string, 0)
	checker := EnvironmentCredentialChecker{
		Lookup: func(name string) (string, bool) {
			lookups = append(lookups, name)
			if name == "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN" {
				return "super-secret", true
			}
			return "", false
		},
	}
	if err := checker.Require(context.Background(), []string{"github-token", "github-token"}); err != nil {
		t.Fatalf("resolve credential: %v", err)
	}
	if len(lookups) != 1 || lookups[0] != "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN" {
		t.Fatalf("lookups = %#v", lookups)
	}
	err := checker.Require(context.Background(), []string{"missing/token"})
	if err == nil || !strings.Contains(err.Error(), "missing/token") || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("missing credential error = %v", err)
	}
}

func TestEnvironmentProtocolCredentialResolverUsesConfiguredReferenceWithoutLeakingValue(t *testing.T) {
	raw, err := json.Marshal(testProtocolCredentials())
	if err != nil {
		t.Fatal(err)
	}
	resolver := EnvironmentProtocolCredentialResolver{Lookup: func(name string) (string, bool) {
		if name == "T3_STEWARD_CREDENTIAL_WORKER_AUTH" {
			return string(raw), true
		}
		return "", false
	}}
	credentials, err := resolver.ResolveProtocol(context.Background(), "worker-auth")
	if err != nil {
		t.Fatal(err)
	}
	if credentials.CoordinatorPrincipal != "ssh:coordinator" ||
		string(credentials.WorkerSecret) != "worker-response-secret" {
		t.Fatalf("credentials = %#v", credentials)
	}
	secret := "do-not-log-this-secret"
	resolver.Lookup = func(string) (string, bool) { return secret, true }
	if _, err := resolver.ResolveProtocol(context.Background(), "worker-auth"); err == nil ||
		strings.Contains(err.Error(), secret) {
		t.Fatalf("malformed credential error = %v", err)
	}
}

func TestEnvironmentCredentialCheckerHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := (EnvironmentCredentialChecker{Lookup: func(string) (string, bool) {
		called = true
		return "secret", true
	}}).Require(ctx, []string{"credential"})
	if err == nil || called {
		t.Fatalf("cancelled resolution = %v, lookup called = %t", err, called)
	}
}
