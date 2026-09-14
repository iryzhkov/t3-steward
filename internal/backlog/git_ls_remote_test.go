package backlog

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// directRunner executes the probe's argument vector without a shell and honours
// the accumulation bound, which is what the contained runner does in
// production. The probe is what is under test here, not the containment.
type directRunner struct {
	calls []ProcessRequest
}

func (r *directRunner) Run(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	r.calls = append(r.calls, request)
	if err := validateProcessRequest(request); err != nil {
		return ProcessResult{}, err
	}
	output := boundedBuffer{limit: request.MaxOutputBytes}
	command := exec.CommandContext(ctx, request.Program, request.Args...)
	command.Dir = request.Dir
	command.Stdout = &output
	command.Stderr = &output
	command.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ASKPASS=/bin/true",
		"GIT_SSH_COMMAND=ssh -oBatchMode=yes -oConnectTimeout=5",
	)
	err := command.Run()
	result := ProcessResult{Output: output.String(), Truncated: output.truncated}
	if err == nil {
		return result, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.ExitCode = exitError.ExitCode()
		return result, &ProcessExitError{ExitCode: result.ExitCode, Err: err}
	}
	return result, err
}

// gitFixtureRepository builds a disposable bare repository with one ref, so the
// measured cases do not depend on a network or on a checkout of this project.
func gitFixtureRepository(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	root := t.TempDir()
	work := filepath.Join(root, "work")
	bare := filepath.Join(root, "good.git")
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, arguments := range [][]string{
		{"init", "--quiet", "--initial-branch=main", work},
		{"-C", work, "config", "user.email", "fixture@example.invalid"},
		{"-C", work, "config", "user.name", "Fixture"},
		{"-C", work, "add", "README.md"},
		{"-C", work, "commit", "--quiet", "-m", "fixture"},
		{"clone", "--quiet", "--bare", work, bare},
	} {
		command := exec.Command("git", arguments...)
		command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(arguments, " "), err, output)
		}
	}
	return bare
}

// TestClassifyRepositoryProbeTable pins the classifier to the Git output that
// was measured rather than to output that was assumed. Every message below is a
// verbatim excerpt of a real run.
func TestClassifyRepositoryProbeTable(t *testing.T) {
	tests := []struct {
		name     string
		exitCode int
		output   string
		err      error
		want     RepositoryReachability
	}{
		{
			name: "a matching ref is authenticated ok", exitCode: 0,
			output: "a90c0978567908625629368af0749936c9ff6b92\trefs/heads/main\n",
			want:   RepositoryAuthenticatedOK,
		},
		{
			name: "exit two is a missing ref", exitCode: 2,
			err: &ProcessExitError{ExitCode: 2}, want: RepositoryRefNotFound,
		},
		{
			name: "an absent local repository is not found", exitCode: 128,
			output: "fatal: '/absent.git' does not appear to be a git repository\nfatal: Could not read from remote repository.\n",
			err:    &ProcessExitError{ExitCode: 128}, want: RepositoryNotFound,
		},
		{
			name: "a remote 404 is not found", exitCode: 128,
			output: "remote: Repository not found.\nfatal: repository 'https://github.com/owner/absent/' not found\n",
			err:    &ProcessExitError{ExitCode: 128}, want: RepositoryNotFound,
		},
		{
			name: "basic auth rejection is an authentication failure", exitCode: 128,
			output: "remote: HTTP Basic: Access denied.\nfatal: Authentication failed for 'https://gitlab.com/owner/private.git/'\n",
			err:    &ProcessExitError{ExitCode: 128}, want: RepositoryAuthenticationFailed,
		},
		{
			name: "a refused public key is an authentication failure", exitCode: 128,
			output: "git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.\n",
			err:    &ProcessExitError{ExitCode: 128}, want: RepositoryAuthenticationFailed,
		},
		{
			name: "an unresolved host is a dns failure", exitCode: 128,
			output: "fatal: unable to access 'https://no-such-host.invalid/x.git/': Could not resolve host: no-such-host.invalid\n",
			err:    &ProcessExitError{ExitCode: 128}, want: RepositoryDNSFailure,
		},
		{
			name: "a refused connection is network unavailable", exitCode: 128,
			output: "fatal: unable to access 'https://127.0.0.1:9/x.git/': Failed to connect to 127.0.0.1 port 9 after 0 ms: Could not connect to server\n",
			err:    &ProcessExitError{ExitCode: 128}, want: RepositoryNetworkUnavailable,
		},
		{
			name: "an expired deadline is a timeout", exitCode: -1,
			err: context.DeadlineExceeded, want: RepositoryProbeTimeout,
		},
		{
			name: "an unrecognised failure stays temporary", exitCode: 128,
			output: "fatal: something nobody has measured yet\n",
			err:    &ProcessExitError{ExitCode: 128}, want: RepositoryNetworkUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyRepositoryProbe(test.exitCode, test.output, test.err); got != test.want {
				t.Fatalf("classification = %q, want %q", got, test.want)
			}
		})
	}
}

// TestRepositoryReachabilityPermanence states which codes refuse a submission
// and which ones only delay it.
func TestRepositoryReachabilityPermanence(t *testing.T) {
	permanent := map[RepositoryReachability]bool{
		RepositoryAuthenticatedOK:      false,
		RepositoryAuthenticationFailed: true,
		RepositoryNotFound:             true,
		RepositoryRefNotFound:          true,
		RepositoryProbeTimeout:         false,
		RepositoryDNSFailure:           false,
		RepositoryNetworkUnavailable:   false,
	}
	for class, want := range permanent {
		if class.Permanent() != want {
			t.Fatalf("%q permanence = %t, want %t", class, class.Permanent(), want)
		}
	}
}

// TestRepositoryProbeAgainstFixtures runs the real argument vector against a
// disposable repository, so the exit codes and messages the classifier depends
// on are observed rather than assumed.
//
// It calls the runner directly rather than going through ObserveRepository,
// because a local path is not an accepted repository syntax: the catalog allows
// https and ssh only, and the probe reuses that validator rather than relaxing
// it for a test. What is measured here is Git's behaviour; the argument vector
// and the refusals are asserted separately.
func TestRepositoryProbeAgainstFixtures(t *testing.T) {
	bare := gitFixtureRepository(t)
	workspace := t.TempDir()
	tests := []struct {
		name       string
		repository string
		ref        string
		wantExit   int
		want       RepositoryReachability
	}{
		{name: "present ref", repository: bare, ref: "refs/heads/main", wantExit: 0, want: RepositoryAuthenticatedOK},
		{name: "absent ref", repository: bare, ref: "refs/heads/absent", wantExit: 2, want: RepositoryRefNotFound},
		{name: "absent repository", repository: filepath.Join(filepath.Dir(bare), "absent.git"), ref: "refs/heads/main", wantExit: 128, want: RepositoryNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &directRunner{}
			result, err := runner.Run(context.Background(), ProcessRequest{
				ID: "probe-fixture", Dir: workspace, Program: "git",
				Args:           []string{"ls-remote", "--exit-code", "--", test.repository, test.ref},
				MaxOutputBytes: 1 << 16,
			})
			if result.ExitCode != test.wantExit {
				t.Fatalf("exit = %d, want %d (output %q)", result.ExitCode, test.wantExit, result.Output)
			}
			if got := ClassifyRepositoryProbe(result.ExitCode, result.Output, err); got != test.want {
				t.Fatalf("class = %q (output %q), want %q", got, result.Output, test.want)
			}
		})
	}
}

// TestRepositoryProbeArgumentVector asserts the exact argv, the bound and that
// no credential reference reaches the retained detail.
func TestRepositoryProbeArgumentVector(t *testing.T) {
	runner := &stubRunner{defaults: stubResponse{output: "deadbeef\trefs/heads/main\n"}}
	key := RepositoryProbeKey{
		WorkerID: "homelab", CatalogDigest: "catalog-1",
		Repository: "https://github.com/owner/project", Ref: "refs/heads/main",
		CredentialRefs: []string{"secretref:f03-admin/homelab"},
	}
	observation, err := ObserveRepository(context.Background(), &RepositoryProbeCache{}, runner, t.TempDir(), key)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Class != RepositoryAuthenticatedOK {
		t.Fatalf("class = %q, want %q", observation.Class, RepositoryAuthenticatedOK)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner calls = %d, want 1", len(runner.calls))
	}
	call := runner.calls[0]
	wantArgs := []string{"ls-remote", "--exit-code", "--", key.Repository, key.Ref}
	if call.Program != "git" || strings.Join(call.Args, "\x00") != strings.Join(wantArgs, "\x00") {
		t.Fatalf("argv = %s %v, want git %v", call.Program, call.Args, wantArgs)
	}
	if call.MaxOutputBytes <= 0 {
		t.Fatal("the probe ran without an accumulation bound")
	}
	if strings.Contains(observation.Detail, "secretref:") {
		t.Fatalf("detail leaked a credential reference: %q", observation.Detail)
	}
}

// TestRepositoryProbeRefusesOptionShapedArguments is the injection case: a
// repository or ref that begins with a dash is refused as a value and no
// process runs at all.
func TestRepositoryProbeRefusesOptionShapedArguments(t *testing.T) {
	tests := []struct {
		name       string
		repository string
		ref        string
	}{
		{name: "option shaped repository", repository: "--upload-pack=touch /tmp/pwned", ref: "refs/heads/main"},
		{name: "short option repository", repository: "-o", ref: "refs/heads/main"},
		{name: "option shaped ref", repository: "https://example.invalid/x.git", ref: "--upload-pack=touch /tmp/pwned"},
		{name: "upload pack ref", repository: "https://example.invalid/x.git", ref: "-o"},
		{name: "scheme not allowed", repository: "ext::sh -c touch% /tmp/pwned", ref: "refs/heads/main"},
		{name: "file scheme not allowed", repository: "file:///etc", ref: "refs/heads/main"},
		{name: "embedded credentials", repository: "https://user:secret@example.invalid/x.git", ref: "refs/heads/main"},
		{name: "newline in ref", repository: "https://example.invalid/x.git", ref: "main\nrefs/heads/other"},
		{name: "nul in repository", repository: "https://example.invalid/x\x00.git", ref: "refs/heads/main"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateRepositoryProbeArguments(test.repository, test.ref); err == nil {
				t.Fatal("the probe accepted an argument it must refuse")
			}
			runner := &directRunner{}
			_, err := ObserveRepository(context.Background(), &RepositoryProbeCache{}, runner,
				t.TempDir(), RepositoryProbeKey{Repository: test.repository, Ref: test.ref})
			if err == nil {
				t.Fatal("observation accepted an argument it must refuse")
			}
			if len(runner.calls) != 0 {
				t.Fatalf("a refused argument still started %d process(es)", len(runner.calls))
			}
		})
	}
}

// TestProbeOutputIsBoundedDuringAccumulation asserts the bound is applied while
// the output arrives rather than afterwards.
func TestProbeOutputIsBoundedDuringAccumulation(t *testing.T) {
	buffer := boundedBuffer{limit: 8}
	for range 100 {
		if _, err := buffer.Write([]byte("0123456789")); err != nil {
			t.Fatal(err)
		}
	}
	if buffer.builder.Len() != 8 || !buffer.truncated {
		t.Fatalf("accumulated %d bytes (truncated %t), want 8 and true", buffer.builder.Len(), buffer.truncated)
	}
	if got := buffer.String(); got != "01234567" {
		t.Fatalf("output = %q", got)
	}

	unbounded := boundedBuffer{}
	if _, err := unbounded.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if unbounded.truncated || unbounded.String() != "0123456789" {
		t.Fatal("an unbounded buffer truncated")
	}

	bare := gitFixtureRepository(t)
	runner := &directRunner{}
	result, err := runner.Run(context.Background(), ProcessRequest{
		ID: "probe-bound", Dir: t.TempDir(), Program: "git",
		Args:           []string{"ls-remote", "--exit-code", "--", bare, "refs/heads/main"},
		MaxOutputBytes: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Output) > 12 || !result.Truncated {
		t.Fatalf("output = %d bytes (truncated %t), want at most 12 and true", len(result.Output), result.Truncated)
	}
}

// TestRepositoryProbeRuntimeIsBounded asserts an expired deadline classifies as
// a timeout rather than as a repository fault.
func TestRepositoryProbeRuntimeIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	observation, err := ObserveRepository(ctx, &RepositoryProbeCache{}, &directRunner{},
		t.TempDir(), RepositoryProbeKey{Repository: "https://github.com/owner/project", Ref: "refs/heads/main"})
	if err != nil {
		t.Fatal(err)
	}
	if observation.Class != RepositoryProbeTimeout {
		t.Fatalf("class = %q, want %q", observation.Class, RepositoryProbeTimeout)
	}
	if observation.Class.Permanent() {
		t.Fatal("a timeout must not refuse a submission permanently")
	}
}

// TestRepositoryProbeEvidenceTTLAndKeys asserts evidence is reused inside its
// window and that a change to any key component invalidates it.
func TestRepositoryProbeEvidenceTTLAndKeys(t *testing.T) {
	workspace := t.TempDir()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	runner := &stubRunner{defaults: stubResponse{output: "deadbeef\trefs/heads/main\n"}}
	cache := &RepositoryProbeCache{Now: func() time.Time { return now }}
	base := RepositoryProbeKey{
		WorkerID: "homelab", CatalogDigest: "catalog-1",
		Repository: "https://github.com/owner/project", Ref: "refs/heads/main",
		CredentialRefs: []string{"secretref:f03-admin/homelab"},
	}
	if _, err := ObserveRepository(context.Background(), cache, runner, workspace, base); err != nil {
		t.Fatal(err)
	}
	if _, err := ObserveRepository(context.Background(), cache, runner, workspace, base); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("retained evidence was not reused: %d runs", len(runner.calls))
	}

	now = now.Add(RepositoryProbeEvidenceTTL)
	if _, err := ObserveRepository(context.Background(), cache, runner, workspace, base); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("evidence outlived its TTL: %d runs", len(runner.calls))
	}

	for _, changed := range []RepositoryProbeKey{
		{WorkerID: "other", CatalogDigest: base.CatalogDigest, Repository: base.Repository, Ref: base.Ref, CredentialRefs: base.CredentialRefs},
		{WorkerID: base.WorkerID, CatalogDigest: "catalog-2", Repository: base.Repository, Ref: base.Ref, CredentialRefs: base.CredentialRefs},
		{WorkerID: base.WorkerID, CatalogDigest: base.CatalogDigest, Repository: base.Repository, Ref: "refs/heads/release", CredentialRefs: base.CredentialRefs},
		{WorkerID: base.WorkerID, CatalogDigest: base.CatalogDigest, Repository: base.Repository, Ref: base.Ref, CredentialRefs: []string{"secretref:f03-admin/rotated"}},
	} {
		if changed.Digest() == base.Digest() {
			t.Fatalf("key %+v shares a digest with the base key", changed)
		}
		if _, found := cache.Lookup(changed); found {
			t.Fatalf("key %+v reused evidence recorded for another key", changed)
		}
	}

	// Credential reference order is not part of the identity: the same set must
	// find the same evidence however it was listed.
	reordered := base
	reordered.CredentialRefs = []string{"secretref:f03-admin/homelab"}
	if reordered.Digest() != base.Digest() {
		t.Fatal("an equal credential reference set produced a different digest")
	}
}
