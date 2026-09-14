package providercontainment

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCommandResultNeedsExitAndConfirmedStop(t *testing.T) {
	s, l, f := supervisorFixture(t)
	state, code, status := "running", "0", "0"
	raw := f.command
	s.command = func(ctx context.Context, cmd string, args ...string) ([]byte, error) {
		out, err := raw(ctx, cmd, args...)
		if err == nil && cmd == "systemctl" && args[1] == "show" {
			out = []byte(strings.ReplaceAll(string(out), "SubState=running", "SubState="+state) + "ExecMainCode=" + code + "\nExecMainStatus=" + status + "\n")
		}
		return out, err
	}
	ctx := context.Background()
	if _, err := s.Start(ctx, l); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Result(ctx, l); !errors.Is(err, ErrCommandRunning) {
		t.Fatalf("running command accepted: %v", err)
	}
	if f.stops != 0 {
		t.Fatal("observation stopped live command")
	}
	state, code, status = "exited", "1", "0"
	f.loseStop = true
	if _, err := s.Result(ctx, l); err == nil {
		t.Fatal("lost stop reply accepted")
	}
	f.loseStop = false
	// Recovery uses the saved exit observation, even if systemd exit properties
	// have changed while the stop response was lost.
	code, status = "0", "0"
	got, err := s.Result(ctx, l)
	if err != nil || got.ExitCode != 0 || got.InvocationID != "same-execution" {
		t.Fatalf("recovered result: %+v %v", got, err)
	}
	if _, err := s.Result(ctx, l); err != nil {
		t.Fatal(err)
	}
	if f.starts != 1 {
		t.Fatal("result recovery relaunched command")
	}
}

func TestCommandResultHonestFailureAndInvocationFence(t *testing.T) {
	for _, mode := range []string{"nonzero", "signal", "failed-zero", "changed"} {
		t.Run(mode, func(t *testing.T) {
			s, l, f := supervisorFixture(t)
			raw := f.command
			s.command = func(ctx context.Context, cmd string, args ...string) ([]byte, error) {
				out, err := raw(ctx, cmd, args...)
				if err == nil && cmd == "systemctl" && args[1] == "show" {
					text := strings.ReplaceAll(string(out), "SubState=running", "SubState=exited")
					code, status := "1", "7"
					if mode == "signal" {
						code, status = "2", "15"
					}
					if mode == "failed-zero" {
						text = strings.ReplaceAll(text, "ActiveState=active", "ActiveState=failed")
						text = strings.ReplaceAll(text, "SubState=exited", "SubState=failed")
						status = "0"
					}
					if mode == "changed" && strings.Contains(strings.Join(args, " "), "ExecMainCode") {
						text = strings.ReplaceAll(text, "same-execution", "changed")
					}
					out = []byte(text + "ExecMainCode=" + code + "\nExecMainStatus=" + status + "\n")
				}
				return out, err
			}
			if _, err := s.Start(context.Background(), l); err != nil {
				t.Fatal(err)
			}
			got, err := s.Result(context.Background(), l)
			if mode == "changed" {
				if err == nil || f.stops != 0 {
					t.Fatal("changed invocation accepted")
				}
				return
			}
			if err != nil || got.ExitCode == 0 {
				t.Fatalf("failure became success: %+v %v", got, err)
			}
		})
	}
}
