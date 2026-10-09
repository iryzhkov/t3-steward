package main

import (
	"bytes"
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
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type reviewAskTransport struct {
	hostStatusTransport
	register func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error)
}

func (c reviewAskTransport) NodeWait(ctx context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	return c.register(ctx, op)
}

// The review's 3.5-second status response must not impose a three-second
// gate on registration. The timer exists only in the forbidden Query path;
// successful registration does not sleep.
func TestReviewSlowCoordinatorAsk(t *testing.T) {
	for _, class := range []backlogadmin.TransportClass{"", backlogadmin.ClassUnavailable, backlogadmin.ClassTimeout, backlogadmin.ClassAuthentication} {
		t.Run(string(class), func(t *testing.T) {
			previous := hostCoordinatorTransport
			t.Cleanup(func() { hostCoordinatorTransport = previous })
			queries, registrations := 0, 0
			wantErr := &backlogadmin.TransportError{Class: class, Err: errors.New("registration transport failure")}
			hostCoordinatorTransport = func(config.Config) (coordinatorTransport, error) {
				return coordinatorTransport{client: reviewAskTransport{
					hostStatusTransport: hostStatusTransport{query: func(ctx context.Context, _ backlogadmin.Query) (backlogadmin.Response, error) {
						queries++
						timer := time.NewTimer(3500 * time.Millisecond)
						defer timer.Stop()
						select {
						case <-ctx.Done():
							return backlogadmin.Response{}, ctx.Err()
						case <-timer.C:
							return backlogadmin.Response{Status: &backlogadmin.Status{}}, nil
						}
					}},
					register: func(ctx context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
						registrations++
						if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) <= hostPushProbeTimeout {
							t.Fatal("registration inherited snapshot timeout")
						}
						if op.Action != "register-task" || op.Task.Kind != domain.WaitKindAsk {
							t.Fatal("not an ask registration")
						}
						if class != "" {
							return backlogadmin.NodeWaitResponse{}, wantErr
						}
						return backlogadmin.NodeWaitResponse{TaskWaits: []domain.TaskWait{{ID: "tw-review", AttemptID: "attempt-review"}}}, nil
					},
				}}, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var out bytes.Buffer
			err := runAsk(ctx, config.Default(), askSpec{}, taskIdentity{}, &out)
			if queries != 0 || registrations != 1 {
				t.Fatalf("status queries=%d registrations=%d", queries, registrations)
			}
			if class == "" {
				if err != nil || !strings.Contains(out.String(), "End this turn now") {
					t.Fatalf("ask did not park: %v %s", err, &out)
				}
			} else if err != wantErr || backlogadmin.ClassOf(err) != class || strings.Contains(err.Error(), askRelayUnavailableCode) || out.Len() != 0 {
				t.Fatalf("transport error changed or task reported parked: %v %s", err, &out)
			}
		})
	}
}

func TestAskConfiguredRouteNeedsNoStatusObservation(t *testing.T) {
	cfg := config.Default()
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{
		CoordinatorID: "review-coordinator", Address: "unreachable.invalid",
		Credential: "secretref:f03-admin/review-worker", RemoteCommand: "t3-steward",
		RequestTimeout: config.Duration(30 * time.Second),
		MessageLimits:  config.V2MessageLimits{MaxBytes: 1 << 20, MaxArtifactBytes: 1 << 20},
	}
	withAdminCredentials(t, fixedAdminCredentials{credentials: completeAdminCredentials()})
	if err := askCoordinatorRoute(cfg); err != nil {
		t.Fatalf("configured route refused: %v", err)
	}
	withAdminCredentials(t, fixedAdminCredentials{err: errors.New("unresolved")})
	if err := runAsk(context.Background(), cfg, askSpec{}, taskIdentity{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), askRelayUnavailableCode) {
		t.Fatalf("unresolved client did not block: %v", err)
	}
}

func TestReviewSSHSystemConfigurationWithUserConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SSH_AUTH_SOCK", "")
	dir := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("Host unrelated\n"), 0600); err != nil {
		t.Fatal(err)
	}
	identity := filepath.Join(dir, "system-project-key")
	if output, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", identity).CombinedOutput(); err != nil {
		t.Fatalf("key fixture: %v %s", err, output)
	}
	// Model OpenSSH's documented config selection: -F suppresses system
	// includes; its normal resolution selects the system project identity.
	bin := t.TempDir()
	script := "#!/bin/sh\nfor arg do test \"$arg\" != -F || exit 91; done\nprintf 'identitiesonly yes\\nidentityagent none\\nidentityfile %s\\n' \"$TEST_SYSTEM_IDENTITY\"\n"
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_SYSTEM_IDENTITY", identity)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if !sshIdentityPresent(context.Background(), "git@project-alias:owner/repo.git") {
		t.Fatal("system identity lost when user config exists")
	}
}
