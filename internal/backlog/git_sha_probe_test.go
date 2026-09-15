package backlog

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

type localAdvertisementRunner struct {
	bare   string
	direct directRunner
}

func (r *localAdvertisementRunner) Run(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	request.Args = append([]string(nil), request.Args...)
	if len(request.Args) < 4 || request.Args[0] != "ls-remote" || request.Args[2] != "--" {
		panic("unexpected probe command")
	}
	request.Args[3] = r.bare
	return r.direct.Run(ctx, request)
}

func TestPinnedCommitProbeUsesObjectAdvertisement(t *testing.T) {
	bare := gitFixtureRepository(t)
	raw, err := exec.Command("git", "--git-dir", bare, "rev-parse", "refs/heads/main").Output()
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.TrimSpace(string(raw))
	runner := &localAdvertisementRunner{bare: bare}
	key := RepositoryProbeKey{Repository: "https://example.invalid/repo.git", Ref: sha}
	observation, err := ObserveRepository(context.Background(), nil, runner, t.TempDir(), key)
	if err != nil || observation.Class != RepositoryAuthenticatedOK {
		t.Fatalf("advertised commit: %+v %v", observation, err)
	}
	if got := runner.direct.calls[0].Args; len(got) != 4 {
		t.Fatalf("SHA used as ref-name filter: %v", got)
	}
	// The same object becomes an unadvertised ancestor, without being deleted.
	raw, err = exec.Command("git", "--git-dir", bare, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit-tree", sha+"^{tree}", "-p", sha, "-m", "next").Output()
	if err != nil {
		t.Fatal(err)
	}
	next := strings.TrimSpace(string(raw))
	if out, err := exec.Command("git", "--git-dir", bare, "update-ref", "refs/heads/main", next).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	observation, err = ObserveRepository(context.Background(), nil, runner, t.TempDir(), key)
	if err == nil || observation.Class.Permanent() || observation.Class == RepositoryAuthenticatedOK {
		t.Fatalf("unadvertised ancestor incorrectly classified: %+v %v", observation, err)
	}
	// Named-ref probes still use their original filter and permanent absence.
	key.Ref = "refs/heads/missing"
	observation, err = ObserveRepository(context.Background(), nil, runner, t.TempDir(), key)
	if err != nil || observation.Class != RepositoryRefNotFound {
		t.Fatalf("named missing ref: %+v %v", observation, err)
	}
	key.Ref = "refs/heads/main"
	observation, err = ObserveRepository(context.Background(), nil, runner, t.TempDir(), key)
	if err != nil || observation.Class != RepositoryAuthenticatedOK {
		t.Fatalf("named existing ref: %+v %v", observation, err)
	}
}

func TestPinnedCommitAdvertisementBoundsAndFailures(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, test := range []struct {
		name, ref, output string
		exit              int
		wantOK            bool
	}{
		{"sha1", sha, sha + "\trefs/heads/main\n", 0, true},
		{"sha256", strings.Repeat("b", 64), strings.Repeat("b", 64) + "\trefs/heads/main\n", 0, true},
		{"peeled-tag", sha, sha + "\trefs/tags/v1^{}\n", 0, true},
		{"different", sha, strings.Repeat("b", 40) + "\trefs/heads/main\n", 0, false},
		{"empty-advertisement", sha, "", 2, false},
		{"truncated-record", sha, sha + "\trefs/heads/mai", 0, false},
		{"substring", sha, "f" + sha + "\trefs/heads/main\n", 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := &stubRunner{defaults: stubResponse{output: test.output, exit: test.exit}}
			observation, err := ObserveRepositoryWith(context.Background(), nil, runner, t.TempDir(), RepositoryProbeKey{Repository: "https://example.invalid/repo.git", Ref: test.ref}, RepositoryProbeOptions{MaxOutputBytes: 256})
			if test.wantOK {
				if err != nil || observation.Class != RepositoryAuthenticatedOK {
					t.Fatalf("%+v %v", observation, err)
				}
			} else if err == nil || observation.Class.Permanent() || observation.Class == RepositoryAuthenticatedOK {
				t.Fatalf("absence is not proven: %+v %v", observation, err)
			}
			if len(runner.calls) != 1 || runner.calls[0].MaxOutputBytes != 256 || len(runner.calls[0].Args) != 4 {
				t.Fatalf("unsafe probe shape: %+v", runner.calls)
			}
		})
	}
}
