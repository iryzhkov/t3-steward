package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// fakeHostProbe is a probe whose every observation is scripted.
func fakeHostProbe(reach, worker error, configHome string, push map[string]bool) (*hostCapabilityProbe, *int) {
	calls := 0
	return &hostCapabilityProbe{
		coordinatorReach: func(config.Config) error { return reach },
		taskWorker:       func(config.Config) (string, error) { return "worker", worker },
		configHome:       func() (string, error) { return configHome, nil },
		pushCredentials: func(_ context.Context, repository string) bool {
			calls++
			return push[repository]
		},
		now: func() time.Time { return time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC) },
	}, &calls
}

func hostProbeSettings(workspaces string) config.BacklogV2 {
	settings := config.Default().BacklogV2
	settings.Storage.Workspaces = workspaces
	settings.Projects = map[string]config.V2Project{
		"pushable":   {Repository: "git@github.com:owner/pushable.git"},
		"read-only":  {Repository: "https://github.com/owner/read-only"},
		"no-repo":    {},
		"Bad.Name":   {Repository: "git@github.com:owner/bad.git"},
		"unassigned": {Repository: "git@github.com:owner/unassigned.git"},
	}
	return settings
}

func hostProbeProjects() []domain.WorkerProjectInventory {
	return []domain.WorkerProjectInventory{{Name: "pushable"}, {Name: "read-only"}, {Name: "no-repo"}, {Name: "Bad.Name"}}
}

func writeHuyangConfig(t *testing.T, home, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, "huyang"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "huyang", "config.toml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A host with a coordinator client, a worker identity, a Huyang trust root over
// its workspaces and push credentials for one project advertises exactly the
// host capabilities those give it.
func TestHostCapabilityProbeAdvertisesWhatTheHostProvides(t *testing.T) {
	workspaces := t.TempDir()
	home := t.TempDir()
	writeHuyangConfig(t, home, "[trust]\nroots = [\""+filepath.Dir(workspaces)+"\"]\n")
	probe, _ := fakeHostProbe(nil, nil, home, map[string]bool{"git@github.com:owner/pushable.git": true})
	got := probe.Observe(context.Background(), hostProbeSettings(workspaces), hostProbeProjects())
	want := []string{
		workerproto.CapabilityAskRelay, workerproto.CapabilityCoordinatorClient,
		"git-push-pushable", workerproto.CapabilityHuyangTrusted,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("observed = %v, want %v", got, want)
	}
}

// The client-less worker of feedback 135 advertises neither the coordinator
// client nor the ask relay, and a client without a worker identity is not an
// ask relay, because nobody on the host would deliver the answer.
func TestHostCapabilityProbeWithholdsWhatTheHostLacks(t *testing.T) {
	home := t.TempDir()
	for name, test := range map[string]struct {
		reach, worker error
		want          []string
	}{
		"no coordinator client": {reach: errors.New("no client"), want: nil},
		"no worker identity":    {worker: errNoTaskWorkerIdentity, want: []string{workerproto.CapabilityCoordinatorClient}},
	} {
		t.Run(name, func(t *testing.T) {
			probe, _ := fakeHostProbe(test.reach, test.worker, home, nil)
			got := probe.Observe(context.Background(), hostProbeSettings(t.TempDir()), hostProbeProjects())
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("observed = %v, want %v", got, test.want)
			}
		})
	}
}

// A push-credential answer is kept for the TTL, so a snapshot does not start a
// credential helper per project every few seconds, and asked again after it.
func TestHostCapabilityProbeCachesPushCredentials(t *testing.T) {
	probe, calls := fakeHostProbe(nil, nil, t.TempDir(), map[string]bool{"git@github.com:owner/pushable.git": true})
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	probe.now = func() time.Time { return now }
	settings := hostProbeSettings(t.TempDir())
	projects := []domain.WorkerProjectInventory{{Name: "pushable"}, {Name: "read-only"}}
	probe.Observe(context.Background(), settings, projects)
	probe.Observe(context.Background(), settings, projects)
	if *calls != 2 {
		t.Fatalf("credential checks = %d within the TTL, want 2", *calls)
	}
	now = now.Add(hostCapabilityTTL)
	probe.Observe(context.Background(), settings, projects)
	if *calls != 4 {
		t.Fatalf("credential checks = %d after the TTL, want 4", *calls)
	}
}

// A snapshot out of time reports the capability as not observed and does not
// cache that, so the next snapshot asks again.
func TestHostCapabilityProbeDoesNotCacheAnUnaskedQuestion(t *testing.T) {
	probe, calls := fakeHostProbe(nil, nil, t.TempDir(), map[string]bool{"git@github.com:owner/pushable.git": true})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	projects := []domain.WorkerProjectInventory{{Name: "pushable"}}
	if got := probe.Observe(ctx, hostProbeSettings(t.TempDir()), projects); len(got) != 2 {
		t.Fatalf("observed = %v, want only the client capabilities", got)
	}
	if *calls != 0 {
		t.Fatalf("credential checks = %d on a cancelled snapshot", *calls)
	}
	if got := probe.Observe(context.Background(), hostProbeSettings(t.TempDir()), projects); len(got) != 3 {
		t.Fatalf("observed = %v, want the push capability once asked", got)
	}
}

func TestHuyangTrustsWorkspaces(t *testing.T) {
	base := t.TempDir()
	workspaces := filepath.Join(base, "steward-workspaces")
	if err := os.MkdirAll(workspaces, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(workspaces, link); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		config    string
		workspace string
		want      bool
	}{
		"exact root":            {config: "[trust]\nroots = [\"" + workspaces + "\"]\n", workspace: workspaces, want: true},
		"ancestor root":         {config: "[trust]\nroots = ['" + base + "']\n", workspace: workspaces, want: true},
		"through a symlink":     {config: "[trust]\nroots = [\"" + workspaces + "\"]\n", workspace: link, want: true},
		"multi-line array":      {config: "# trust\n[trust]\nroots = [\n  \"/elsewhere\", # other\n  \"" + workspaces + "\",\n]\n", workspace: workspaces, want: true},
		"dotted key":            {config: "trust.roots = [\"" + workspaces + "\"]\n", workspace: workspaces, want: true},
		"sibling prefix":        {config: "[trust]\nroots = [\"" + workspaces + "-other\"]\n", workspace: workspaces, want: false},
		"descendant only":       {config: "[trust]\nroots = [\"" + filepath.Join(workspaces, "one") + "\"]\n", workspace: workspaces, want: false},
		"other table":           {config: "[other]\nroots = [\"" + workspaces + "\"]\n", workspace: workspaces, want: false},
		"commented out":         {config: "[trust]\n# roots = [\"" + workspaces + "\"]\n", workspace: workspaces, want: false},
		"relative root":         {config: "[trust]\nroots = [\"steward-workspaces\"]\n", workspace: workspaces, want: false},
		"no workspace root":     {config: "[trust]\nroots = [\"" + workspaces + "\"]\n", workspace: "", want: false},
		"relative workspace":    {config: "[trust]\nroots = [\"" + workspaces + "\"]\n", workspace: "steward-workspaces", want: false},
		"unsupported escape":    {config: "[trust]\nroots = [\"" + workspaces + "\\t\"]\n", workspace: workspaces, want: false},
		"no huyang config file": {workspace: workspaces, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			if test.config != "" {
				writeHuyangConfig(t, home, test.config)
			}
			if got := huyangTrustsWorkspaces(home, test.workspace); got != test.want {
				t.Fatalf("trusted = %v, want %v", got, test.want)
			}
		})
	}
}

func TestRepositoryLocation(t *testing.T) {
	for repository, want := range map[string][3]string{
		"git@github.com:owner/repo.git":        {"ssh", "github.com", ""},
		"github.com:owner/repo.git":            {"ssh", "github.com", ""},
		"ssh://git@github.com:22/owner/repo":   {"ssh", "github.com", ""},
		"https://github.com/owner/repo":        {"https", "github.com", ""},
		"https://user@git.example:8443/r.git":  {"https", "git.example:8443", ""},
		"file:///srv/git/repo.git":             {"file", "", "/srv/git/repo.git"},
		"/srv/git/repo.git":                    {"file", "", "/srv/git/repo.git"},
		"relative/path":                        {"", "", ""},
		"ftp://example/repo":                   {"", "", ""},
		"":                                     {"", "", ""},
		"./owner:repo":                         {"", "", ""},
		"https://github.com/owner/repo%zz":     {"", "", ""},
		"git+ssh://git@example.org/owner/repo": {"ssh", "example.org", ""},
		"http://insecure.example/owner/repo":   {"http", "insecure.example", ""},
		"  git@github.com:owner/repo.git  ":    {"ssh", "github.com", ""},
		"user@host.example:repositories/r.git": {"ssh", "host.example", ""},
	} {
		scheme, host, path := repositoryLocation(repository)
		if got := [3]string{scheme, host, path}; got != want {
			t.Errorf("repositoryLocation(%q) = %v, want %v", repository, got, want)
		}
	}
}

// A local repository is pushable when its directory is writable; an address
// the probe cannot classify is never reported as pushable.
func TestGitPushCredentialsPresentForLocalAndUnknownRepositories(t *testing.T) {
	dir := t.TempDir()
	if !gitPushCredentialsPresent(context.Background(), dir) {
		t.Fatal("a writable local repository directory was not pushable")
	}
	if gitPushCredentialsPresent(context.Background(), filepath.Join(dir, "missing")) {
		t.Fatal("a missing local repository was pushable")
	}
	if gitPushCredentialsPresent(context.Background(), "ftp://example/repo") {
		t.Fatal("an unclassified repository was pushable")
	}
}

// The coordinator host reaches its own socket; a host that is neither a
// coordinator nor a configured client has no route; a configured client whose
// credential does not resolve has none either.
func TestHostCoordinatorReach(t *testing.T) {
	cfg := config.Default()
	cfg.StatePath = filepath.Join(t.TempDir(), "state.db")
	if err := hostCoordinatorReach(cfg); err == nil {
		t.Fatal("a host with no socket and no client reached the coordinator")
	}
	coordinator := cfg
	coordinator.BacklogV2.Mode = "coordinator"
	if err := hostCoordinatorReach(coordinator); err != nil {
		t.Fatalf("the coordinator host: %v", err)
	}
	client := cfg
	client.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{
		CoordinatorID: "normandy", Address: "normandy", Credential: "secretref:f03-admin/agent-a",
	}
	withAdminCredentials(t, fixedAdminCredentials{err: errors.New("unresolved")})
	if err := hostCoordinatorReach(client); err == nil {
		t.Fatal("a client whose credential does not resolve reached the coordinator")
	}
	withAdminCredentials(t, fixedAdminCredentials{credentials: completeAdminCredentials()})
	if err := hostCoordinatorReach(client); err != nil {
		t.Fatalf("a client with a resolving credential: %v", err)
	}
}

// The persistent worker's inventory carries the host capabilities beside the
// configured ones without the operator listing them. A host capability an
// operator does list follows the observation: present, the worker is ready;
// absent, the worker is degraded exactly as for any configured capability.
func TestObserveHostInventoryAdvertisesHostCapabilities(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{})
	}))
	defer server.Close()
	control := t3control.New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	previous := taskWaitWorkerHome
	taskWaitWorkerHome = func() (string, error) { return t.TempDir(), nil }
	t.Cleanup(func() { taskWaitWorkerHome = previous })
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	coordinatorHost := config.Default()
	coordinatorHost.StatePath = filepath.Join(t.TempDir(), "state.db")
	coordinatorHost.BacklogV2.Mode = "coordinator"
	coordinatorHost.BacklogV2.LocalWorker.ID = "worker"
	clientless := config.Default()
	clientless.StatePath = filepath.Join(t.TempDir(), "state.db")

	for name, test := range map[string]struct {
		cfg        config.Config
		configured []string
		want       []string
		health     domain.WorkerHealth
	}{
		"coordinator host, nothing configured": {cfg: coordinatorHost,
			want:   []string{workerproto.CapabilityAskRelay, workerproto.CapabilityCoordinatorClient},
			health: domain.WorkerHealthReady},
		"coordinator host, ask relay configured": {cfg: coordinatorHost, configured: []string{workerproto.CapabilityAskRelay},
			want:   []string{workerproto.CapabilityAskRelay, workerproto.CapabilityCoordinatorClient},
			health: domain.WorkerHealthReady},
		"client-less worker, nothing configured": {cfg: clientless, health: domain.WorkerHealthReady},
		"client-less worker, ask relay configured": {cfg: clientless, configured: []string{workerproto.CapabilityAskRelay},
			health: domain.WorkerHealthDegraded},
	} {
		t.Run(name, func(t *testing.T) {
			wanted := domain.WorkerInventory{AcceptBacklog: true, Capabilities: test.configured}
			inventory, err := observeHostInventory(test.cfg, control, t.TempDir())(context.Background(), config.Default().BacklogV2, wanted)
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(inventory.Capabilities)
			if !slices.Equal(inventory.Capabilities, test.want) || inventory.Health != test.health {
				t.Fatalf("capabilities %v health %q, want %v %q", inventory.Capabilities, inventory.Health, test.want, test.health)
			}
		})
	}
}
