package backlog

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type localAdvertisementRunner struct {
	bare   string
	direct directRunner
}

func (r *localAdvertisementRunner) Run(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	request.Args = append([]string(nil), request.Args...)
	switch request.Args[0] {
	case "ls-remote":
		request.Args[3] = r.bare
	case "fetch":
		request.Args[5] = r.bare
	case "init", "rev-parse", "for-each-ref":
	default:
		panic("unexpected probe command")
	}
	return r.direct.Run(ctx, request)
}

func TestPinnedCommitProbeUsesObjectAdvertisement(t *testing.T) {
	bare := gitFixtureRepository(t)
	sha := gitFixtureHead(t, bare)
	runner := &localAdvertisementRunner{bare: bare}
	key := RepositoryProbeKey{Repository: "https://example.invalid/repo.git", Ref: sha}
	observation, err := ObserveRepository(context.Background(), nil, runner, t.TempDir(), key)
	if err != nil || observation.Class != RepositoryAuthenticatedOK {
		t.Fatalf("advertised commit: %+v %v", observation, err)
	}
	if len(runner.direct.calls) != 1 || len(runner.direct.calls[0].Args) != 4 {
		t.Fatalf("tip did not use the advertisement: %+v", runner.direct.calls)
	}
	// The same object becomes an unadvertised ancestor, without being deleted.
	raw, err := exec.Command("git", "--git-dir", bare, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit-tree", sha+"^{tree}", "-p", sha, "-m", "next").Output()
	if err != nil {
		t.Fatal(err)
	}
	next := strings.TrimSpace(string(raw))
	if out, err := exec.Command("git", "--git-dir", bare, "update-ref", "refs/heads/main", next).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	for _, test := range []struct {
		name, ref string
		want      RepositoryReachability
	}{
		{"ancestor", sha, RepositoryAuthenticatedOK},
		{"uppercase ancestor", strings.ToUpper(sha), RepositoryAuthenticatedOK},
		{"missing", strings.Repeat("a", 40), RepositoryRefNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner.direct.calls = nil
			key.Ref = test.ref
			observation, err := ObserveRepositoryWith(context.Background(), nil, runner, t.TempDir(), key, RepositoryProbeOptions{MaxOutputBytes: 256})
			if err != nil || observation.Class != test.want {
				t.Fatalf("pin: %+v %v, want %s", observation, err, test.want)
			}
			if observation.Class.Permanent() != (test.want == RepositoryRefNotFound) {
				t.Fatalf("wrong permanence: %+v", observation)
			}
			if len(runner.direct.calls) < 4 {
				t.Fatalf("did not fetch history: %+v", runner.direct.calls)
			}
			dir := runner.direct.calls[1].Dir
			if info, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("private repository retained: %v %v", info, err)
			}
			for _, call := range runner.direct.calls {
				if call.Program != "git" || call.MaxOutputBytes != 256 {
					t.Fatalf("unsafe probe shape: %+v", call)
				}
			}
		})
	}
	// Named-ref probes retain their original behaviour.
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
		want              RepositoryReachability
		wantFetch         bool
	}{
		{"sha1", sha, sha + "\trefs/heads/main\n", 0, RepositoryAuthenticatedOK, false},
		{"sha256", strings.Repeat("b", 64), strings.Repeat("b", 64) + "\trefs/heads/main\n", 0, RepositoryAuthenticatedOK, false},
		{"peeled-tag", sha, sha + "\trefs/tags/v1^{}\n", 0, RepositoryAuthenticatedOK, false},
		{"different", sha, strings.Repeat("b", 40) + "\trefs/heads/main\n", 0, RepositoryNetworkUnavailable, true},
		{"empty-advertisement", sha, "", 2, RepositoryRefNotFound, false},
		{"truncated-record", sha, sha + "\trefs/heads/mai", 0, RepositoryNetworkUnavailable, true},
		{"substring", sha, "f" + sha + "\trefs/heads/main\n", 0, RepositoryNetworkUnavailable, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := &stubRunner{responses: map[string]stubResponse{
				"git ls-remote --exit-code -- https://example.invalid/repo.git":                                                {output: test.output, exit: test.exit},
				"git fetch --quiet --no-tags --no-recurse-submodules -- https://example.invalid/repo.git +refs/*:refs/probe/*": {exit: 128, output: "fatal: connection refused"},
			}}
			observation, err := ObserveRepositoryWith(context.Background(), nil, runner, t.TempDir(), RepositoryProbeKey{Repository: "https://example.invalid/repo.git", Ref: test.ref}, RepositoryProbeOptions{MaxOutputBytes: 256})
			if err != nil || observation.Class != test.want {
				t.Fatalf("%+v %v, want %s", observation, err, test.want)
			}
			wantCalls := 1
			if test.wantFetch {
				wantCalls = 3
			}
			if len(runner.calls) != wantCalls || runner.calls[0].MaxOutputBytes != 256 || len(runner.calls[0].Args) != 4 {
				t.Fatalf("unsafe probe shape: %+v", runner.calls)
			}
		})
	}
}

func TestPinnedCommitUnreachableLocalRepositoryStaysTemporary(t *testing.T) {
	// A closed local listening port exercises Git's real transport failure.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	runner := &localAdvertisementRunner{bare: "git://" + address + "/repo.git"}
	observation, err := ObserveRepository(context.Background(), nil, runner, t.TempDir(), RepositoryProbeKey{
		Repository: "https://example.invalid/repo.git", Ref: strings.Repeat("a", 40),
	})
	if err != nil || observation.Class != RepositoryNetworkUnavailable || observation.Class.Permanent() {
		t.Fatalf("unreachable repository: %+v %v", observation, err)
	}
}

func TestPinnedCommitFetchFailuresKeepClasses(t *testing.T) {
	for _, test := range []struct {
		name, output string
		err          error
		want         RepositoryReachability
	}{
		{"network", "fatal: connection refused", &ProcessExitError{ExitCode: 128}, RepositoryNetworkUnavailable},
		{"auth", "fatal: Authentication failed", &ProcessExitError{ExitCode: 128}, RepositoryAuthenticationFailed},
		{"timeout", "", context.DeadlineExceeded, RepositoryProbeTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := &pinFetchFailureRunner{t: t, output: test.output, err: test.err}
			observation, err := ObserveRepositoryWith(context.Background(), nil, runner, t.TempDir(), RepositoryProbeKey{
				Repository: "https://example.invalid/repo.git", Ref: strings.Repeat("a", 40),
			}, RepositoryProbeOptions{MaxOutputBytes: 256, Timeout: time.Second})
			if err != nil || observation.Class != test.want {
				t.Fatalf("fetch failure: %+v %v", observation, err)
			}
			if len(runner.calls) != 3 {
				t.Fatalf("calls = %+v", runner.calls)
			}
			if _, err := os.Stat(runner.calls[1].Dir); !os.IsNotExist(err) {
				t.Fatalf("private repository retained: %v", err)
			}
		})
	}
}

type pinFetchFailureRunner struct {
	t        *testing.T
	output   string
	err      error
	calls    []ProcessRequest
	deadline time.Time
}

func (r *pinFetchFailureRunner) Run(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	r.calls = append(r.calls, request)
	deadline, ok := ctx.Deadline()
	if !ok || (!r.deadline.IsZero() && deadline != r.deadline) || request.MaxOutputBytes != 256 {
		r.t.Fatalf("bounds changed: %+v, deadline %v", request, deadline)
	}
	r.deadline = deadline
	switch request.Args[0] {
	case "ls-remote":
		return ProcessResult{Output: strings.Repeat("b", 40) + "\trefs/heads/main\n"}, nil
	case "init":
		return ProcessResult{}, nil
	case "fetch":
		result := ProcessResult{Output: r.output}
		if _, ok := r.err.(*ProcessExitError); ok {
			result.ExitCode = 128
		}
		return result, r.err
	default:
		r.t.Fatalf("unexpected command: %+v", request)
		return ProcessResult{}, nil
	}
}
