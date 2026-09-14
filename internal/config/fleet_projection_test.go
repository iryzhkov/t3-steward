package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The bytes under testdata/fleet are the exact bytes UpKeeper's fleet component
// renders onto a host. They are carried by both repositories on purpose.
//
// This test exists because carrying them was not enough. The contract said both
// repositories held identical fixtures and that UpKeeper rendered them while the
// steward parsed them, but nothing on this side ever fed one to a loader, and a
// cross-repository check later found that the producer wrote request_timeout as a
// number while this loader required a duration string. Both test suites were
// green. The failure would have appeared on the first host to converge, and not
// only for coordinator commands: this document is decoded during ordinary
// configuration loading, so the worker daemon would have failed to start too.
//
// A fixture nobody executes is a comment. This executes them.
func fleetFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "fleet", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

func TestTheRenderedClientProjectionIsAcceptedByItsLoader(t *testing.T) {
	for _, name := range []string{
		"projection-client-homelab.json",
		"projection-client-omarchy-pc.json",
	} {
		t.Run(name, func(t *testing.T) {
			bootstrap, err := DecodeCoordinatorClientBootstrap(fleetFixture(t, name))
			if err != nil {
				t.Fatalf("the fleet component renders a document this loader refuses: %v", err)
			}
			if bootstrap.CoordinatorID == "" || bootstrap.Address == "" {
				t.Fatalf("decoded an empty coordinator identity: %+v", bootstrap)
			}
			if bootstrap.RemoteCommand != "t3-steward" {
				// The remote command is appended to an argv, so a value carrying
				// arguments becomes one nonsensical token rather than a command.
				t.Errorf("remote command is %q", bootstrap.RemoteCommand)
			}
			if _, err := time.ParseDuration(bootstrap.RequestTimeout); err != nil {
				t.Errorf("request timeout %q is not a duration: %v", bootstrap.RequestTimeout, err)
			}
		})
	}
}

// The exact mismatch that was found, pinned so it cannot return silently. If a
// future producer goes back to seconds-as-a-number, this fails here rather than
// on the first host to converge.
func TestASecondsNumberIsRefusedWhereADurationIsRequired(t *testing.T) {
	document := []byte(`{"schema_version":1,"coordinator_id":"normandy-coordinator",` +
		`"address":"normandy","connection":"ssh","remote_command":"t3-steward",` +
		`"credential_ref":"secretref:f03-admin/omarchy-pc","request_timeout":30}`)
	if _, err := DecodeCoordinatorClientBootstrap(document); err == nil {
		t.Fatal("a seconds number was accepted where a duration string is required")
	}
}

// The whole-host consequence, stated as a test: the document is read during
// ordinary configuration loading, so a projection this loader refuses takes the
// worker runtime down with it, not only the coordinator client.
func TestARenderedProjectionDoesNotBreakOrdinaryConfigurationLoading(t *testing.T) {
	home := t.TempDir()
	directory := filepath.Join(home, ".config/t3-steward")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "coordinator-client.json")
	raw := fleetFixture(t, "projection-client-omarchy-pc.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	bootstrap, present, err := LoadCoordinatorClientBootstrap(home)
	if err != nil {
		t.Fatalf("loading the rendered projection failed: %v", err)
	}
	if !present {
		t.Fatal("the rendered projection was not found")
	}
	if bootstrap.CredentialRef != "secretref:f03-admin/omarchy-pc" {
		t.Errorf("credential reference is %q", bootstrap.CredentialRef)
	}
}
