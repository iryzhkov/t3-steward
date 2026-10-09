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

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestReviewCoordinatorCapabilityRequiresAuthentication(t *testing.T) {
	cfg := config.Default()
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{
		CoordinatorID: "review-coordinator", Address: "unreachable.invalid",
		Credential: "secretref:f03-admin/review-worker",
	}
	withAdminCredentials(t, fixedAdminCredentials{credentials: completeAdminCredentials()})
	if err := hostCoordinatorReach(cfg); err == nil {
		t.Fatal("accepted local credentials without authenticating the coordinator")
	}
}

// This seam models responses after the transport's signature verification.
// Actual local peer authentication is exercised by TestHostCoordinatorReach;
// signed remote authentication and rejection are covered by backlogadmin tests.
type hostStatusTransport struct {
	backlogadmin.CoordinatorAdminTransport
	query func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
}

func (c hostStatusTransport) Query(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
	return c.query(ctx, q)
}

func TestHostCoordinatorAuthenticationFailures(t *testing.T) {
	previous := hostCoordinatorTransport
	t.Cleanup(func() { hostCoordinatorTransport = previous })
	for _, name := range []string{"revoked", "wrong coordinator", "no status", "deadline", "authenticated"} {
		t.Run(name, func(t *testing.T) {
			hostCoordinatorTransport = func(config.Config) (coordinatorTransport, error) {
				return coordinatorTransport{principal: backlogadmin.Principal{ID: "host"}, client: hostStatusTransport{query: func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
					if q.Kind != backlogadmin.QueryStatus || q.Principal.ID != "host" {
						t.Fatal("not a read-only authenticated status query")
					}
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("unbounded authentication query")
					}
					switch name {
					case "revoked", "wrong coordinator":
						return backlogadmin.Response{}, errors.New(name)
					case "deadline":
						<-ctx.Done()
						return backlogadmin.Response{}, ctx.Err()
					case "no status":
						return backlogadmin.Response{}, nil
					default:
						return backlogadmin.Response{Status: &backlogadmin.Status{}}, nil
					}
				}}}, nil
			}
			probe, _ := fakeHostProbe(nil, nil, t.TempDir(), nil)
			probe.coordinatorReach = authenticatedHostCoordinatorReach
			ctx, cancel := context.WithCancel(context.Background())
			if name == "deadline" {
				cancel()
			} else {
				defer cancel()
			}
			caps := probe.Observe(ctx, hostProbeSettings(t.TempDir()), nil)
			want := name == "authenticated"
			for _, cap := range []string{workerproto.CapabilityCoordinatorClient, workerproto.CapabilityAskRelay} {
				if strings.Contains(strings.Join(caps, ","), cap) != want {
					t.Fatalf("%s advertised: %v", cap, caps)
				}
			}
		})
	}
}

func TestHostCoordinatorCacheExpiresAndHonorsDeadline(t *testing.T) {
	probe, _ := fakeHostProbe(nil, nil, t.TempDir(), nil)
	now := time.Now()
	probe.now = func() time.Time { return now }
	calls := 0
	present := true
	probe.coordinatorReach = func(ctx context.Context, _ config.Config) error {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > hostPushProbeTimeout {
			t.Fatal("authentication not bounded")
		}
		if present {
			return nil
		}
		return errors.New("revoked")
	}
	if !probe.coordinatorPresent(context.Background()) {
		t.Fatal("success withheld")
	}
	present = false
	if !probe.coordinatorPresent(context.Background()) || calls != 1 {
		t.Fatal("success not cached")
	}
	now = now.Add(hostCapabilityTTL)
	if probe.coordinatorPresent(context.Background()) || calls != 2 {
		t.Fatal("revocation not rechecked")
	}
	if probe.coordinatorPresent(context.Background()) || calls != 2 {
		t.Fatal("negative not cached")
	}
	present = true
	now = now.Add(hostCapabilityTTL)
	if !probe.coordinatorPresent(context.Background()) || calls != 3 {
		t.Fatal("negative did not expire")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if probe.coordinatorPresent(ctx) {
		t.Fatal("cancelled snapshot advertised cached access")
	}
}

func TestReviewHTTPSPathScopedPushCredentials(t *testing.T) {
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	t.Setenv("GIT_CONFIG_COUNT", "3")
	t.Setenv("GIT_CONFIG_KEY_0", "credential.helper")
	t.Setenv("GIT_CONFIG_VALUE_0", "")
	t.Setenv("GIT_CONFIG_KEY_1", "credential.helper")
	t.Setenv("GIT_CONFIG_VALUE_1", "!f() { path=; user=; while IFS= read -r line; do case \"$line\" in path=*) path=$line ;; username=*) user=$line ;; esac; done; if test \"$path\" = path=owner/repo.git && test \"$user\" = username=review-user; then printf 'username=review-user\\npassword=review-dummy\\n'; fi; }; f")
	t.Setenv("GIT_CONFIG_KEY_2", "credential.useHttpPath")
	t.Setenv("GIT_CONFIG_VALUE_2", "true")
	repository := "https://review-user@example.invalid/owner/repo.git"
	control := exec.Command("git", "credential", "fill")
	control.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	control.Stdin = strings.NewReader("url=" + repository + "\n\n")
	if output, err := control.CombinedOutput(); err != nil || !strings.Contains(string(output), "password=review-dummy") {
		t.Fatalf("fixture: %v %s", err, output)
	}
	if !gitPushCredentialsPresent(context.Background(), repository) {
		t.Fatal("path/username-scoped credentials missing")
	}
	for _, wrong := range []string{"https://review-user@example.invalid/other/repo.git", "https://other@example.invalid/owner/repo.git"} {
		if gitPushCredentialsPresent(context.Background(), wrong) {
			t.Fatal("credentials applied to wrong project/user")
		}
	}
}

func TestReviewSSHConfiguredProjectIdentity(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SSH_AUTH_SOCK", "")
	dir := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	identity := filepath.Join(dir, "project-key")
	if output, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", identity).CombinedOutput(); err != nil {
		t.Fatalf("key fixture: %v %s", err, output)
	}
	configPath := filepath.Join(dir, "config")
	content := "Host project-alias\n HostName example.invalid\n User git\n Port 2222\n IdentitiesOnly yes\n IdentityFile " + identity + "\nHost missing-alias\n HostName example.invalid\n IdentitiesOnly yes\n IdentityFile " + filepath.Join(dir, "missing") + "\n"
	if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	control := exec.Command("ssh", "-G", "-F", configPath, "project-alias")
	if output, err := control.CombinedOutput(); err != nil || !strings.Contains(string(output), "identityfile "+identity) {
		t.Fatalf("fixture: %v %s", err, output)
	}
	for _, repository := range []string{"git@project-alias:owner/repo.git", "ssh://git@project-alias:2222/owner/repo.git"} {
		if !gitPushCredentialsPresent(context.Background(), repository) {
			t.Fatalf("selected identity missing for %s", repository)
		}
	}
	// A valid default key must not satisfy IdentitiesOnly for another project.
	if output, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", filepath.Join(dir, "id_ed25519")).CombinedOutput(); err != nil {
		t.Fatalf("default key: %v %s", err, output)
	}
	if gitPushCredentialsPresent(context.Background(), "git@missing-alias:owner/repo.git") {
		t.Fatal("unrelated default identity satisfied project")
	}
}
