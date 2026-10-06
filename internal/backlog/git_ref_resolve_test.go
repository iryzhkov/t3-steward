package backlog

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const exactRefFixtureRef = "refs/heads/steward/run-x/task-y/cp-1"

// exactRefRunnerFunc answers a resolution with scripted behaviour.
type exactRefRunnerFunc func(context.Context, ProcessRequest) (ProcessResult, error)

func (f exactRefRunnerFunc) Run(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	return f(ctx, request)
}

// gitFixtureCommit adds one commit on top of parent in the bare fixture and
// points ref at it, returning the new object ID.
func gitFixtureCommit(t *testing.T, bare, ref, parent, message string) string {
	t.Helper()
	raw, err := exec.Command("git", "--git-dir", bare, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
		"commit-tree", parent+"^{tree}", "-p", parent, "-m", message).Output()
	if err != nil {
		t.Fatal(err)
	}
	object := strings.TrimSpace(string(raw))
	if output, err := exec.Command("git", "--git-dir", bare, "update-ref", ref, object).CombinedOutput(); err != nil {
		t.Fatalf("update-ref %s: %v %s", ref, err, output)
	}
	return object
}

func gitFixtureHead(t *testing.T, bare string) string {
	t.Helper()
	raw, err := exec.Command("git", "--git-dir", bare, "rev-parse", "refs/heads/main").Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(raw))
}

func exactRefRequest(ref string) ExactRefRequest {
	return ExactRefRequest{Repository: "https://example.invalid/repo.git", Ref: ref, Timeout: 30 * time.Second}
}

// TestResolveExactRefMatchesOneRefOnly states the exact-match property against
// a real remote. git ls-remote matches its pattern against the tail of every
// ref name, so refs/heads/other/refs/heads/steward/... is offered for the
// pattern refs/heads/steward/...; only the identical name may answer.
func TestResolveExactRefMatchesOneRefOnly(t *testing.T) {
	bare := gitFixtureRepository(t)
	main := gitFixtureHead(t, bare)
	similar := gitFixtureCommit(t, bare, "refs/heads/other/"+exactRefFixtureRef, main, "similar")
	runner := &localAdvertisementRunner{bare: bare}

	// Only the similar ref exists: the advertisement is not empty, but the
	// ref asked about is absent.
	resolution, err := ResolveExactRef(context.Background(), runner, t.TempDir(), exactRefRequest(exactRefFixtureRef))
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Found || resolution.ObjectID != "" || resolution.Class != RepositoryRefNotFound {
		t.Fatalf("a similar ref answered for the exact one: %+v", resolution)
	}
	if got := runner.direct.calls[0].Args; strings.Join(got[:3], " ") != "ls-remote --exit-code --" || got[4] != exactRefFixtureRef || len(got) != 5 {
		t.Fatalf("argv = %v", got)
	}

	exact := gitFixtureCommit(t, bare, exactRefFixtureRef, main, "exact")
	if exact == similar {
		t.Fatal("fixture commits are not distinct")
	}
	resolution, err = ResolveExactRef(context.Background(), runner, t.TempDir(), exactRefRequest(exactRefFixtureRef))
	if err != nil {
		t.Fatal(err)
	}
	if !resolution.Found || resolution.ObjectID != exact || resolution.Class != RepositoryAuthenticatedOK || resolution.Ref != exactRefFixtureRef {
		t.Fatalf("resolution = %+v, want %s", resolution, exact)
	}
}

// TestResolveExactRefNotFound states that a remote that answered without the
// ref is a permanent not-found rather than an unreachable remote.
func TestResolveExactRefNotFound(t *testing.T) {
	bare := gitFixtureRepository(t)
	resolution, err := ResolveExactRef(context.Background(), &localAdvertisementRunner{bare: bare}, t.TempDir(),
		exactRefRequest("refs/heads/missing"))
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Found || resolution.Class != RepositoryRefNotFound || resolution.ExitCode != 2 {
		t.Fatalf("resolution = %+v", resolution)
	}
}

// TestResolveExactRefUnreachable states that a remote that does not answer is
// classified by the reachability table, never reported as a missing ref.
func TestResolveExactRefUnreachable(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	absent := t.TempDir() + "/absent.git"
	resolution, err := ResolveExactRef(context.Background(), &localAdvertisementRunner{bare: absent}, t.TempDir(),
		exactRefRequest(exactRefFixtureRef))
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Found || resolution.Class == RepositoryRefNotFound || resolution.Class == RepositoryAuthenticatedOK {
		t.Fatalf("an absent remote was classified as %+v", resolution)
	}
	if resolution.Detail == "" {
		t.Fatal("an unreachable remote carried no detail")
	}
}

// TestResolveExactRefTimesOut states that the request's own timeout ends a
// resolution the remote never answers, and that it is classified, not raised.
func TestResolveExactRefTimesOut(t *testing.T) {
	runner := exactRefRunnerFunc(func(ctx context.Context, _ ProcessRequest) (ProcessResult, error) {
		<-ctx.Done()
		return ProcessResult{}, ctx.Err()
	})
	request := exactRefRequest(exactRefFixtureRef)
	request.Timeout = 50 * time.Millisecond
	started := time.Now()
	resolution, err := ResolveExactRef(context.Background(), runner, t.TempDir(), request)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 10*time.Second {
		t.Fatal("the resolution was not bounded by its timeout")
	}
	if resolution.Found || resolution.Class != RepositoryProbeTimeout {
		t.Fatalf("resolution = %+v", resolution)
	}
}

// TestResolveExactRefRefusesInvalidRefNames states that ref names follow the
// git check-ref-format rules plus the exact-ref rules (a full refs/ name, no
// pattern), and that a refused name starts no process.
func TestResolveExactRefRefusesInvalidRefNames(t *testing.T) {
	for _, ref := range []string{
		"main",
		"HEAD",
		"heads/main",
		"refs/heads",
		"refs/heads/*",
		"refs/heads/cp-?",
		"refs/heads/[a]",
		"refs/heads/a..b",
		"refs/heads/a.lock",
		"refs/heads/a.lock/b",
		"refs/heads/a@{1}",
		"refs/heads/@",
		"refs/heads/.hidden",
		"refs/heads/a/",
		"refs//heads/a",
		"refs/heads/a.",
		"refs/heads/a b",
		"refs/heads/a\\b",
		"refs/heads/a~1",
		"refs/heads/a^",
		"refs/heads/a:b",
		"refs/heads/a\x7f",
		"--upload-pack=touch /tmp/pwned",
		"a90c0978567908625629368af0749936c9ff6b92",
	} {
		t.Run(ref, func(t *testing.T) {
			if err := ValidateExactRef(ref); !errors.Is(err, ErrInvalidExactRef) {
				t.Fatalf("ValidateExactRef(%q) = %v", ref, err)
			}
			calls := 0
			runner := exactRefRunnerFunc(func(context.Context, ProcessRequest) (ProcessResult, error) {
				calls++
				return ProcessResult{}, nil
			})
			_, err := ResolveExactRef(context.Background(), runner, os.TempDir(), exactRefRequest(ref))
			if !errors.Is(err, ErrInvalidExactRef) {
				t.Fatalf("error = %v, want an invalid-ref refusal", err)
			}
			if calls != 0 {
				t.Fatal("a refused ref started a process")
			}
		})
	}
	for _, ref := range []string{exactRefFixtureRef, "refs/heads/main", "refs/tags/v0.11.0-rc.111", "refs/pull/12/head"} {
		if err := ValidateExactRef(ref); err != nil {
			t.Fatalf("ValidateExactRef(%q) = %v", ref, err)
		}
	}
}

// TestResolveExactRefRefusesUntrustworthyAdvertisements states that output the
// resolver cannot read whole produces no object ID: a truncated listing, an
// abbreviated or malformed object ID, and two different answers for one name.
func TestResolveExactRefRefusesUntrustworthyAdvertisements(t *testing.T) {
	object := "a90c0978567908625629368af0749936c9ff6b92"
	for name, result := range map[string]ProcessResult{
		"truncated":   {Output: "b90c0978567908625629368af0749936c9ff6b92\trefs/heads/x/" + exactRefFixtureRef + "\n" + object[:10], Truncated: true},
		"short id":    {Output: object[:12] + "\t" + exactRefFixtureRef + "\n"},
		"non-hex id":  {Output: strings.Repeat("z", 40) + "\t" + exactRefFixtureRef + "\n"},
		"ambiguous":   {Output: object + "\t" + exactRefFixtureRef + "\n" + strings.Repeat("c", 40) + "\t" + exactRefFixtureRef + "\n"},
		"uppercase":   {Output: strings.ToUpper(object) + "\t" + exactRefFixtureRef + "\n"},
		"no newline":  {Output: object + "\t" + exactRefFixtureRef},
		"extra field": {Output: object + "\t" + exactRefFixtureRef + "\textra\n"},
	} {
		t.Run(name, func(t *testing.T) {
			runner := exactRefRunnerFunc(func(context.Context, ProcessRequest) (ProcessResult, error) { return result, nil })
			resolution, err := ResolveExactRef(context.Background(), runner, t.TempDir(), exactRefRequest(exactRefFixtureRef))
			if err == nil {
				t.Fatalf("an untrustworthy advertisement resolved: %+v", resolution)
			}
			if resolution.ObjectID != "" || resolution.Found {
				t.Fatalf("resolution carried an object ID: %+v", resolution)
			}
		})
	}
}

// TestResolveExactRefDoesNotCache states that two resolutions of the same ref
// observe the remote twice, so a head that moved in between is reported moved.
func TestResolveExactRefDoesNotCache(t *testing.T) {
	bare := gitFixtureRepository(t)
	main := gitFixtureHead(t, bare)
	first := gitFixtureCommit(t, bare, exactRefFixtureRef, main, "first")
	runner := &localAdvertisementRunner{bare: bare}
	resolution, err := ResolveExactRef(context.Background(), runner, t.TempDir(), exactRefRequest(exactRefFixtureRef))
	if err != nil || resolution.ObjectID != first {
		t.Fatalf("first = %+v %v, want %s", resolution, err, first)
	}
	second := gitFixtureCommit(t, bare, exactRefFixtureRef, first, "second")
	resolution, err = ResolveExactRef(context.Background(), runner, t.TempDir(), exactRefRequest(exactRefFixtureRef))
	if err != nil || resolution.ObjectID != second {
		t.Fatalf("second = %+v %v, want %s", resolution, err, second)
	}
	if len(runner.direct.calls) != 2 {
		t.Fatalf("remote observed %d times, want 2", len(runner.direct.calls))
	}
}
