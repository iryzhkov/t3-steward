package workerruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// repositoryProbeRunner records the exact argument vector and bound one probe
// asked for, and answers with scripted output.
type repositoryProbeRunner struct {
	calls  []backlog.ProcessRequest
	output string
	exit   int
	err    error
	// block makes the run wait for the context, which is how a timeout is
	// observed without depending on a real network.
	block bool
}

func (r *repositoryProbeRunner) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	r.calls = append(r.calls, request)
	if r.block {
		<-ctx.Done()
		return backlog.ProcessResult{}, ctx.Err()
	}
	return backlog.ProcessResult{Output: r.output, ExitCode: r.exit}, r.err
}

// repositoryProbeCredentials answers availability for a fixed set of references
// and never returns a value for any of them.
type repositoryProbeCredentials struct {
	available map[string]bool
	asked     [][]string
}

func (c *repositoryProbeCredentials) Require(_ context.Context, references []string) error {
	c.asked = append(c.asked, append([]string(nil), references...))
	for _, reference := range references {
		if !c.available[reference] {
			return errors.New("resolve credentials: reference " + reference + " is unavailable")
		}
	}
	return nil
}

func repositoryProbeDriver(t *testing.T, runner backlog.PreflightRunner, credentials CredentialChecker) *LocalDriver {
	t.Helper()
	return &LocalDriver{
		Config:      LocalDriverConfig{RunsRoot: t.TempDir()},
		Preflight:   runner,
		Credentials: credentials,
		Now:         func() time.Time { return runtimeTestNow },
	}
}

func repositoryProbeRequest() workerproto.RepositoryProbeRequest {
	return workerproto.RepositoryProbeRequest{
		Repository: "https://github.com/owner/project",
		Ref:        "refs/heads/main",
	}
}

// TestWorkerObservesRepositoryReachability is the happy path on the worker: the
// fixed argument vector runs once, bounded, and the classified answer comes back
// with no credential value anywhere in it.
func TestWorkerObservesRepositoryReachability(t *testing.T) {
	runner := &repositoryProbeRunner{output: "deadbeef\trefs/heads/main\n"}
	credentials := &repositoryProbeCredentials{available: map[string]bool{"secretref:f03-admin/homelab": true}}
	driver := repositoryProbeDriver(t, runner, credentials)
	request := repositoryProbeRequest()
	request.CredentialRefs = []string{"secretref:f03-admin/homelab"}

	observation, err := driver.ObserveRepository(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Class != string(backlog.RepositoryAuthenticatedOK) {
		t.Fatalf("class = %q", observation.Class)
	}
	if !observation.CredentialsResolved {
		t.Fatal("a probe that resolved its references reported that it did not")
	}
	if len(credentials.asked) != 1 || credentials.asked[0][0] != "secretref:f03-admin/homelab" {
		t.Fatalf("credential references asked for = %v", credentials.asked)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runs = %d, want 1", len(runner.calls))
	}
	call := runner.calls[0]
	want := []string{"ls-remote", "--exit-code", "--", request.Repository, request.Ref}
	if call.Program != "git" || strings.Join(call.Args, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv = %s %v, want git %v", call.Program, call.Args, want)
	}
	if call.Dir != driver.Config.RunsRoot {
		t.Fatalf("probe ran in %q, want the worker runs root", call.Dir)
	}
	if strings.Contains(observation.Detail, "secretref:") {
		t.Fatalf("detail named a credential reference: %q", observation.Detail)
	}
}

// TestWorkerProbeRefusesArgumentInjectionOnTheDispatchedPath states that the
// refusal survives dispatch: a message carrying an option-shaped value starts no
// process on the worker, whatever the coordinator believed it had validated.
func TestWorkerProbeRefusesArgumentInjectionOnTheDispatchedPath(t *testing.T) {
	tests := []struct {
		name       string
		repository string
		ref        string
	}{
		{name: "option shaped repository", repository: "--upload-pack=touch /tmp/pwned", ref: "refs/heads/main"},
		{name: "option shaped ref", repository: "https://example.invalid/x.git", ref: "--upload-pack=touch /tmp/pwned"},
		{name: "scheme not allowed", repository: "ext::sh -c touch% /tmp/pwned", ref: "refs/heads/main"},
		{name: "file scheme not allowed", repository: "file:///etc", ref: "refs/heads/main"},
		{name: "embedded credentials", repository: "https://user:secret@example.invalid/x.git", ref: "refs/heads/main"},
		{name: "newline in ref", repository: "https://example.invalid/x.git", ref: "main\nrefs/heads/other"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &repositoryProbeRunner{}
			driver := repositoryProbeDriver(t, runner, nil)
			_, err := driver.ObserveRepository(context.Background(), workerproto.RepositoryProbeRequest{
				Repository: test.repository, Ref: test.ref,
			})
			if err == nil {
				t.Fatal("the worker accepted an argument it must refuse")
			}
			if len(runner.calls) != 0 {
				t.Fatalf("a refused argument still started %d process(es)", len(runner.calls))
			}
		})
	}
}

// TestWorkerProbeAppliesItsOwnBounds states that the worker clamps what the
// coordinator asked for rather than trusting it. An absent bound becomes the
// maximum, never an unbounded run.
func TestWorkerProbeAppliesItsOwnBounds(t *testing.T) {
	tests := []struct {
		name      string
		requested int
		want      int
	}{
		{name: "an absent bound becomes the maximum", requested: 0, want: workerproto.MaxRepositoryProbeOutputBytes},
		{name: "a smaller bound is honoured", requested: 4096, want: 4096},
		{name: "a larger bound is clamped", requested: workerproto.MaxRepositoryProbeOutputBytes * 4, want: workerproto.MaxRepositoryProbeOutputBytes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &repositoryProbeRunner{output: "deadbeef\trefs/heads/main\n"}
			driver := repositoryProbeDriver(t, runner, nil)
			request := repositoryProbeRequest()
			// A request above the transport limit never reaches a well-behaved
			// coordinator's wire, so the clamp is asserted on the worker's own
			// helper as well as through a valid request.
			if got := repositoryProbeOutputBound(test.requested); got != test.want {
				t.Fatalf("bound = %d, want %d", got, test.want)
			}
			if test.requested <= workerproto.MaxRepositoryProbeOutputBytes {
				request.MaxOutputBytes = test.requested
				if _, err := driver.ObserveRepository(context.Background(), request); err != nil {
					t.Fatal(err)
				}
				if runner.calls[0].MaxOutputBytes != test.want {
					t.Fatalf("the probe ran with bound %d, want %d", runner.calls[0].MaxOutputBytes, test.want)
				}
			}
		})
	}

	for _, test := range []struct {
		seconds int
		want    time.Duration
	}{
		{seconds: 0, want: workerproto.MaxRepositoryProbeTimeout},
		{seconds: 5, want: 5 * time.Second},
		{seconds: 3600, want: workerproto.MaxRepositoryProbeTimeout},
	} {
		if got := repositoryProbeTimeout(test.seconds); got != test.want {
			t.Fatalf("timeout for %ds = %s, want %s", test.seconds, got, test.want)
		}
	}
}

// TestWorkerProbeTimesOutRatherThanHanging states that the worker's own bound
// ends a probe the repository never answers, and that the result is temporary.
func TestWorkerProbeTimesOutRatherThanHanging(t *testing.T) {
	runner := &repositoryProbeRunner{block: true}
	driver := repositoryProbeDriver(t, runner, nil)
	request := repositoryProbeRequest()
	request.TimeoutSeconds = 1

	started := time.Now()
	observation, err := driver.ObserveRepository(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 30*time.Second {
		t.Fatalf("the probe ran for %s, which is not bounded by its request", elapsed)
	}
	if observation.Class != string(backlog.RepositoryProbeTimeout) {
		t.Fatalf("class = %q, want %q", observation.Class, backlog.RepositoryProbeTimeout)
	}
	class, known := backlog.ParseRepositoryReachability(observation.Class)
	if !known || class.Permanent() {
		t.Fatal("a timeout must never refuse a submission permanently")
	}
}

// TestWorkerProbeWillNotAnswerWithoutItsCredentialReferences is the property
// that makes the answer mean what it claims: a worker that cannot present the
// project's credential references reports nothing rather than a reachability it
// observed under some other identity.
func TestWorkerProbeWillNotAnswerWithoutItsCredentialReferences(t *testing.T) {
	runner := &repositoryProbeRunner{output: "deadbeef\trefs/heads/main\n"}
	credentials := &repositoryProbeCredentials{available: map[string]bool{}}
	driver := repositoryProbeDriver(t, runner, credentials)
	request := repositoryProbeRequest()
	request.CredentialRefs = []string{"secretref:f03-admin/homelab"}

	if _, err := driver.ObserveRepository(context.Background(), request); err == nil {
		t.Fatal("a worker without the credential reference still reported a reachability")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("the probe ran %d time(s) without its credential references", len(runner.calls))
	}

	// A worker with no resolver at all is the same refusal, not a silent pass.
	bare := repositoryProbeDriver(t, runner, nil)
	if _, err := bare.ObserveRepository(context.Background(), request); err == nil {
		t.Fatal("a worker with no credential resolver still reported a reachability")
	}
}

// TestWorkerProbeDetailIsRedactedAndBounded states that neither a secret-shaped
// span nor an unbounded message can travel back to the coordinator.
func TestWorkerProbeDetailIsRedactedAndBounded(t *testing.T) {
	runner := &repositoryProbeRunner{
		exit: 128,
		err:  &backlog.ProcessExitError{ExitCode: 128},
		output: "fatal: token=ghp_" + strings.Repeat("a", 32) + " " +
			strings.Repeat("x", 4*workerproto.MaxRepositoryProbeDetailBytes) + "\n",
	}
	driver := repositoryProbeDriver(t, runner, nil)
	observation, err := driver.ObserveRepository(context.Background(), repositoryProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(observation.Detail, "ghp_") || strings.Contains(observation.Detail, "token=") {
		t.Fatalf("detail carried a credential-shaped span: %q", observation.Detail)
	}
	if err := workerproto.ValidateRepositoryObservation(observation); err != nil {
		t.Fatal(err)
	}
}
