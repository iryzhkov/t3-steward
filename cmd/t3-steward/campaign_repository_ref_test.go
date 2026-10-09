package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

const (
	refWorkerID   = "omarchy-pc"
	refRepository = "https://github.com/iryzhkov/t3-steward.git"
	refCheckpoint = "refs/heads/steward/run-x/task-y/cp-1"
)

// refFixtureRemote is a disposable bare repository standing in for the
// project remote. The worker's runner below points git at it, so the worker
// resolves through a real git ls-remote rather than a scripted answer.
type refFixtureRemote struct {
	t    *testing.T
	bare string
}

func newRefFixtureRemote(t *testing.T) *refFixtureRemote {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	root := t.TempDir()
	remote := &refFixtureRemote{t: t, bare: filepath.Join(root, "remote.git")}
	remote.git("init", "--quiet", "--bare", "--initial-branch=main", remote.bare)
	tree := strings.TrimSpace(remote.git("--git-dir", remote.bare, "mktree"))
	root0 := strings.TrimSpace(remote.git("--git-dir", remote.bare, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
		"commit-tree", tree, "-m", "root"))
	remote.git("--git-dir", remote.bare, "update-ref", "refs/heads/main", root0)
	return remote
}

func (r *refFixtureRemote) git(arguments ...string) string {
	r.t.Helper()
	command := exec.Command("git", arguments...)
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	command.Stdin = strings.NewReader("")
	output, err := command.Output()
	if err != nil {
		r.t.Fatalf("git %s: %v", strings.Join(arguments, " "), err)
	}
	return string(output)
}

// commit points ref at a new commit and returns its object ID.
func (r *refFixtureRemote) commit(ref, message string) string {
	r.t.Helper()
	parent := strings.TrimSpace(r.git("--git-dir", r.bare, "rev-parse", "refs/heads/main"))
	object := strings.TrimSpace(r.git("--git-dir", r.bare, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
		"commit-tree", parent+"^{tree}", "-p", parent, "-m", message))
	r.git("--git-dir", r.bare, "update-ref", ref, object)
	return object
}

// refFixtureRunner runs the worker's fixed argument vector with the catalog
// repository replaced by the fixture's path, and counts what it ran.
type refFixtureRunner struct {
	bare  string
	calls []backlog.ProcessRequest
}

func (r *refFixtureRunner) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	r.calls = append(r.calls, request)
	args := append([]string(nil), request.Args...)
	if len(args) != 5 || args[0] != "ls-remote" || args[2] != "--" || args[3] != refRepository {
		return backlog.ProcessResult{}, errors.New("unexpected worker command")
	}
	args[3] = r.bare
	command := exec.CommandContext(ctx, request.Program, args...)
	command.Dir = request.Dir
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_TERMINAL_PROMPT=0")
	output, err := command.CombinedOutput()
	result := backlog.ProcessResult{Output: string(output)}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.ExitCode = exitError.ExitCode()
		return result, &backlog.ProcessExitError{ExitCode: result.ExitCode, Err: err}
	}
	return result, err
}

// refLoopback carries one signed envelope into a real worker protocol server
// whose allowlist is the one this build's worker service uses for the message.
type refLoopback struct {
	server *workerproto.Server
	driver *workerruntime.LocalDriver
}

func (l refLoopback) RoundTripWithRetry(ctx context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy) (workerproto.Envelope, error) {
	return l.server.Handle(ctx, request, func(ctx context.Context, envelope workerproto.Envelope) (workerproto.MessageType, any, error) {
		var resolve workerproto.RepositoryRefRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageRepositoryRefResolve, &resolve); err != nil {
			return "", nil, err
		}
		resolution, err := l.driver.ResolveRepositoryRef(ctx, resolve)
		return workerproto.MessageRepositoryRefResolution, resolution, err
	})
}

func refWorkerClient(t *testing.T, runner backlog.PreflightRunner, allowed workerproto.MessageType) *workerproto.Client {
	t.Helper()
	driver := &workerruntime.LocalDriver{
		Config:      workerruntime.LocalDriverConfig{RunsRoot: t.TempDir()},
		Preflight:   runner,
		Credentials: probeCredentialChecker{probeCredentialRef: true},
		Now:         time.Now,
	}
	server, err := workerproto.NewServer(workerproto.ServerConfig{
		CoordinatorID: "coordinator", WorkerID: refWorkerID, CoordinatorEpoch: 7, WorkerEpoch: "worker-1",
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key",
		PeerSecret:      []byte("coordinator-secret-value"),
		SignerPrincipal: "ssh:" + refWorkerID, SignerKeyID: "worker-key",
		SignerSecret: []byte("worker-secret-value"),
		Allowed:      map[workerproto.MessageType]bool{allowed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := workerproto.NewClient(workerproto.ClientConfig{
		CoordinatorID: "coordinator", WorkerID: refWorkerID,
		CoordinatorEpoch: 7, WorkerEpoch: "worker-1", SessionID: "ref-" + refWorkerID,
		RequestTimeout:  time.Minute,
		SignerPrincipal: "ssh:coordinator", SignerKeyID: "coordinator-key",
		SignerSecret: []byte("coordinator-secret-value"),
		RetryPolicy:  workerproto.RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		Transport:    refLoopback{server: server, driver: driver},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// refSnapshots is the coordinator's stored worker observations.
type refSnapshots struct {
	snapshots []domain.WorkerSnapshot
	err       error
}

func (s refSnapshots) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return s.snapshots, s.err
}

func refWorkerSnapshot(epoch string, capabilities ...string) domain.WorkerSnapshot {
	snapshot := probeWorkerSnapshot(refWorkerID)
	snapshot.WorkerEpoch = epoch
	snapshot.Inventory.Epoch = epoch
	snapshot.Inventory.Capabilities = capabilities
	return snapshot
}

func refSettings() config.BacklogV2 {
	return config.BacklogV2{Workers: map[string]config.V2Worker{refWorkerID: {Epoch: "worker-1"}}}
}

// refResolver builds the coordinator's resolver over the stored snapshots and
// one dial seam that counts sessions.
func refResolver(snapshots refSnapshots, dial func() (repositoryRefClient, error)) (*coordinatorRepositoryRefResolver, *int) {
	dialled := new(int)
	resolver := newCoordinatorRepositoryRefResolver(refSettings(), nil, 7, nil, snapshots)
	resolver.dial = func(_ context.Context, workerID string) (repositoryRefClient, func() error, error) {
		*dialled++
		if workerID != refWorkerID {
			return nil, nil, errors.New("dialled the wrong worker " + workerID)
		}
		client, err := dial()
		return client, nil, err
	}
	return resolver, dialled
}

func refQuery(ref string) repositoryRefQuery {
	return repositoryRefQuery{
		WorkerID: refWorkerID, Repository: refRepository, Ref: ref,
		CredentialRefs: []string{probeCredentialRef},
	}
}

func capableSnapshots() refSnapshots {
	return refSnapshots{snapshots: []domain.WorkerSnapshot{
		refWorkerSnapshot("worker-1", "git", workerproto.CapabilityRepositoryRefResolve),
	}}
}

// TestRepositoryRefResolutionIsWorkerSourcedAndNotCached is the outcome: the
// coordinator asks the named worker, receives the object ID it observed on the
// remote, and a second call after the head moved observes the remote again
// rather than answering from anything retained.
func TestRepositoryRefResolutionIsWorkerSourcedAndNotCached(t *testing.T) {
	remote := newRefFixtureRemote(t)
	first := remote.commit(refCheckpoint, "first")
	runner := &refFixtureRunner{bare: remote.bare}
	client := refWorkerClient(t, runner, workerproto.MessageRepositoryRefResolve)
	resolver, dialled := refResolver(capableSnapshots(), func() (repositoryRefClient, error) { return client, nil })

	answer, err := resolver.ResolveRef(context.Background(), refQuery(refCheckpoint))
	if err != nil {
		t.Fatal(err)
	}
	if answer.Status != workerproto.RefResolutionResolved || answer.ObjectID != first ||
		answer.Ref != refCheckpoint || answer.WorkerID != refWorkerID || answer.ObservedAt.IsZero() {
		t.Fatalf("answer = %+v, want %s", answer, first)
	}

	second := remote.commit(refCheckpoint, "second")
	answer, err = resolver.ResolveRef(context.Background(), refQuery(refCheckpoint))
	if err != nil {
		t.Fatal(err)
	}
	if answer.ObjectID != second {
		t.Fatalf("the second call answered %s after the head moved to %s", answer.ObjectID, second)
	}
	if *dialled != 2 || len(runner.calls) != 2 {
		t.Fatalf("sessions = %d, worker runs = %d; want 2 and 2", *dialled, len(runner.calls))
	}
	if runner.calls[0].Args[3] != refRepository || runner.calls[0].Args[4] != refCheckpoint {
		t.Fatalf("the worker ran %v", runner.calls[0].Args)
	}
}

// TestRepositoryRefResolutionExactMatchAndNotFound states that a ref whose
// name merely ends with the requested one never answers for it.
func TestRepositoryRefResolutionExactMatchAndNotFound(t *testing.T) {
	remote := newRefFixtureRemote(t)
	similar := remote.commit("refs/heads/archive/"+refCheckpoint, "similar")
	client := refWorkerClient(t, &refFixtureRunner{bare: remote.bare}, workerproto.MessageRepositoryRefResolve)
	resolver, _ := refResolver(capableSnapshots(), func() (repositoryRefClient, error) { return client, nil })

	answer, err := resolver.ResolveRef(context.Background(), refQuery(refCheckpoint))
	if err != nil {
		t.Fatal(err)
	}
	if answer.Status != workerproto.RefResolutionNotFound || answer.ObjectID != "" {
		t.Fatalf("a similar ref (%s) answered: %+v", similar, answer)
	}
	exact := remote.commit(refCheckpoint, "exact")
	answer, err = resolver.ResolveRef(context.Background(), refQuery(refCheckpoint))
	if err != nil {
		t.Fatal(err)
	}
	if answer.Status != workerproto.RefResolutionResolved || answer.ObjectID != exact {
		t.Fatalf("answer = %+v, want %s", answer, exact)
	}
}

// TestRepositoryRefResolutionUnreachable states both unreachable shapes: a
// worker that answers that the remote did not, and a worker that cannot be
// reached at all, which is an error and never an answer.
func TestRepositoryRefResolutionUnreachable(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	client := refWorkerClient(t, &refFixtureRunner{bare: filepath.Join(t.TempDir(), "absent.git")}, workerproto.MessageRepositoryRefResolve)
	resolver, _ := refResolver(capableSnapshots(), func() (repositoryRefClient, error) { return client, nil })
	answer, err := resolver.ResolveRef(context.Background(), refQuery(refCheckpoint))
	if err != nil {
		t.Fatal(err)
	}
	if answer.Status != workerproto.RefResolutionUnreachable || answer.ObjectID != "" || answer.Class == "" {
		t.Fatalf("answer = %+v", answer)
	}

	offline, _ := refResolver(capableSnapshots(), func() (repositoryRefClient, error) {
		return nil, errors.New("worker omarchy-pc did not answer")
	})
	if _, err := offline.ResolveRef(context.Background(), refQuery(refCheckpoint)); err == nil {
		t.Fatal("an unreachable worker produced an answer")
	}
}

// TestRepositoryRefResolutionTimeout states that the coordinator bounds the
// worker's run by the transport timeout and that a remote that never answers
// comes back as a structured unreachable timeout.
func TestRepositoryRefResolutionTimeout(t *testing.T) {
	blocking := &probeBlockingRunner{}
	client := refWorkerClient(t, blocking, workerproto.MessageRepositoryRefResolve)
	resolver, _ := refResolver(capableSnapshots(), func() (repositoryRefClient, error) { return client, nil })
	resolver.settings.Transport.RequestTimeout = config.Duration(time.Second)
	started := time.Now()
	answer, err := resolver.ResolveRef(context.Background(), refQuery(refCheckpoint))
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > testtiming.Bound(30*time.Second) {
		t.Fatal("the resolution was not bounded by the transport timeout")
	}
	if answer.Status != workerproto.RefResolutionUnreachable || answer.Class != backlog.RepositoryProbeTimeout {
		t.Fatalf("answer = %+v", answer)
	}
}

// probeBlockingRunner never finishes until its context ends.
type probeBlockingRunner struct{}

func (probeBlockingRunner) Run(ctx context.Context, _ backlog.ProcessRequest) (backlog.ProcessResult, error) {
	<-ctx.Done()
	return backlog.ProcessResult{}, ctx.Err()
}

// TestRepositoryRefResolutionInvalidRef states that an invalid name is a
// structured answer the coordinator gives without opening a session.
func TestRepositoryRefResolutionInvalidRef(t *testing.T) {
	resolver, dialled := refResolver(capableSnapshots(), func() (repositoryRefClient, error) {
		return nil, errors.New("an invalid ref reached the transport")
	})
	for _, ref := range []string{"main", "refs/heads/*", "refs/heads/a..b", "--upload-pack=x", "refs/heads/a.lock"} {
		answer, err := resolver.ResolveRef(context.Background(), refQuery(ref))
		if err != nil {
			t.Fatalf("%q: %v", ref, err)
		}
		if answer.Status != workerproto.RefResolutionInvalidRef || answer.ObjectID != "" {
			t.Fatalf("%q: answer = %+v", ref, answer)
		}
	}
	if *dialled != 0 {
		t.Fatalf("invalid refs opened %d session(s)", *dialled)
	}

	query := refQuery(refCheckpoint)
	query.Repository = "ext::sh -c touch% /tmp/pwned"
	if _, err := resolver.ResolveRef(context.Background(), query); err == nil {
		t.Fatal("a disallowed repository was accepted")
	}
	if *dialled != 0 {
		t.Fatal("a disallowed repository opened a session")
	}
}

// TestRepositoryRefResolutionCapabilityNegotiation states the compatibility
// contract: a worker build that does not advertise the capability is never
// sent the message and the caller gets a clear unsupported error, and a worker
// whose capabilities were not observed for its current enrolment is not
// assumed to have them.
func TestRepositoryRefResolutionCapabilityNegotiation(t *testing.T) {
	for name, snapshots := range map[string]refSnapshots{
		"old worker build": {snapshots: []domain.WorkerSnapshot{refWorkerSnapshot("worker-1", "git",
			workerproto.CapabilityQuotaObservations)}},
	} {
		t.Run(name, func(t *testing.T) {
			resolver, dialled := refResolver(snapshots, func() (repositoryRefClient, error) {
				return nil, errors.New("an old worker was sent the message")
			})
			_, err := resolver.ResolveRef(context.Background(), refQuery(refCheckpoint))
			if !errors.Is(err, errRepositoryRefUnsupportedByWorker) {
				t.Fatalf("error = %v, want unsupported by worker", err)
			}
			if !strings.Contains(err.Error(), refWorkerID) {
				t.Fatalf("error does not name the worker: %v", err)
			}
			if *dialled != 0 {
				t.Fatalf("the old worker was dialled %d time(s)", *dialled)
			}
		})
	}
	for name, snapshots := range map[string]refSnapshots{
		"no snapshot":            {},
		"another worker only":    {snapshots: []domain.WorkerSnapshot{probeWorkerSnapshot("homelab")}},
		"previous enrolment":     {snapshots: []domain.WorkerSnapshot{refWorkerSnapshot("worker-0", workerproto.CapabilityRepositoryRefResolve)}},
		"snapshot store failure": {err: errors.New("database is locked")},
	} {
		t.Run(name, func(t *testing.T) {
			resolver, dialled := refResolver(snapshots, func() (repositoryRefClient, error) {
				return nil, errors.New("an unobserved worker was sent the message")
			})
			_, err := resolver.ResolveRef(context.Background(), refQuery(refCheckpoint))
			if err == nil || errors.Is(err, errRepositoryRefUnsupportedByWorker) {
				t.Fatalf("error = %v, want an unobserved-capability error", err)
			}
			if *dialled != 0 {
				t.Fatalf("dialled %d time(s)", *dialled)
			}
		})
	}

	// A worker that advertises the capability but whose build refuses the
	// message anyway is reported as the worker's refusal, never as an answer.
	client := refWorkerClient(t, &refFixtureRunner{}, workerproto.MessageRepositoryProbe)
	resolver, _ := refResolver(capableSnapshots(), func() (repositoryRefClient, error) { return client, nil })
	if _, err := resolver.ResolveRef(context.Background(), refQuery(refCheckpoint)); err == nil {
		t.Fatal("a refusing worker produced an answer")
	}
}

// TestRepositoryRefResolutionLeavesTheProbeCacheAlone states that the new call
// is separate from the cached reachability observer: resolving a ref neither
// reads nor fills the observer's retained evidence.
func TestRepositoryRefResolutionLeavesTheProbeCacheAlone(t *testing.T) {
	remote := newRefFixtureRemote(t)
	remote.commit(refCheckpoint, "first")
	client := refWorkerClient(t, &refFixtureRunner{bare: remote.bare}, workerproto.MessageRepositoryRefResolve)
	resolver, _ := refResolver(capableSnapshots(), func() (repositoryRefClient, error) { return client, nil })
	observer := probeObserver(nil)
	if _, err := resolver.ResolveRef(context.Background(), refQuery(refCheckpoint)); err != nil {
		t.Fatal(err)
	}
	if observer.cache.Len() != 0 {
		t.Fatal("a ref resolution filled the reachability cache")
	}
}
