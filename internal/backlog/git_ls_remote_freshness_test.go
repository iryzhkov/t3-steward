package backlog

import (
	"context"
	"testing"
	"time"
)

func TestMissingRefIsReprobedInsideTTL(t *testing.T) {
	for _, ref := range []string{"refs/heads/main", "a90c0978567908625629368af0749936c9ff6b92"} {
		t.Run(ref, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
			cache := &RepositoryProbeCache{Now: func() time.Time { return now }}
			key := RepositoryProbeKey{WorkerID: "homelab", Repository: "https://github.com/owner/project", Ref: ref}
			runner := &stubRunner{}
			workspace := t.TempDir()
			for range 2 {
				// Simulate another fetch cycle with an unchanged key.
				runner.defaults = stubResponse{exit: 2, err: &ProcessExitError{ExitCode: 2}}
				cache.Store(RepositoryProbeObservation{Key: key, Class: RepositoryRefNotFound, ObservedAt: now})
				before := len(runner.calls)
				missing, err := ObserveRepository(context.Background(), cache, runner, workspace, key)
				if err != nil || missing.Class != RepositoryRefNotFound || !missing.Class.Permanent() {
					t.Fatalf("fresh missing ref = %+v, %v", missing, err)
				}
				if len(runner.calls)-before != 1 {
					t.Fatal("missing ref was reused")
				}
				now = now.Add(time.Second)
				runner.defaults = stubResponse{output: "a90c0978567908625629368af0749936c9ff6b92\trefs/heads/main\n"}
				before = len(runner.calls)
				present, err := ObserveRepository(context.Background(), cache, runner, workspace, key)
				if err != nil || present.Class != RepositoryAuthenticatedOK || !present.ObservedAt.After(missing.ObservedAt) {
					t.Fatalf("after fetch = %+v, %v", present, err)
				}
				if len(runner.calls)-before != 1 {
					t.Fatal("fetch did not cause a fresh probe")
				}
				cached, err := ObserveRepository(context.Background(), cache, runner, workspace, key)
				if err != nil || cached.ObservedAt != present.ObservedAt || len(runner.calls)-before != 1 {
					t.Fatal("successful observation was not reused")
				}
			}
		})
	}
}

func TestProbeCachePreservesOtherClasses(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	cache := &RepositoryProbeCache{Now: func() time.Time { return now }}
	key := RepositoryProbeKey{Repository: "https://github.com/owner/project", Ref: "main"}
	for _, class := range []RepositoryReachability{RepositoryAuthenticatedOK, RepositoryAuthenticationFailed, RepositoryNotFound, RepositoryProbeTimeout, RepositoryDNSFailure, RepositoryNetworkUnavailable} {
		cache.Store(RepositoryProbeObservation{Key: key, Class: class})
		got, found := cache.Lookup(key)
		if !found || got.Class != class {
			t.Fatalf("%s evidence not reused: %+v", class, got)
		}
	}
	cache.Store(RepositoryProbeObservation{Key: key, Class: RepositoryRefNotFound})
	if got, found := cache.Lookup(key); found || got.Class != "" {
		t.Fatal("missing ref returned as current evidence")
	}
	if cache.Len() != 1 {
		t.Fatal("historical negative was not retained")
	}
	now = now.Add(RepositoryProbeEvidenceTTL)
	cache.Lookup(key)
	if cache.Len() != 0 {
		t.Fatal("historical negative outlived retention")
	}
}
