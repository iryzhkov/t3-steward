package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validBootstrapDocument = `{
  "schema_version": 1,
  "coordinator_id": "normandy-coordinator",
  "address": "normandy",
  "connection": "ssh",
  "remote_command": "t3-steward",
  "credential_ref": "secretref:f03-admin/omarchy-pc",
  "request_timeout": "45s",
  "message_limits": {"max_bytes": 4194304, "max_artifact_bytes": 1073741824}
}`

// bootstrapHome writes a coordinator client bootstrap into a temporary home and
// returns that home.
func bootstrapHome(t *testing.T, document string, mode os.FileMode) string {
	t.Helper()
	home := t.TempDir()
	path := filepath.Join(home, CoordinatorClientBootstrapPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(document), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return home
}

func TestCoordinatorClientBootstrapFillsAnAbsentBlock(t *testing.T) {
	home := bootstrapHome(t, validBootstrapDocument, 0o600)
	cfg := Default()
	if err := cfg.applyCoordinatorClientBootstrap(home); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	client := cfg.BacklogV2.CoordinatorClient
	if client.CoordinatorID != "normandy-coordinator" || client.Address != "normandy" ||
		client.Connection != "ssh" || client.RemoteCommand != "t3-steward" ||
		client.Credential != "secretref:f03-admin/omarchy-pc" {
		t.Fatalf("client = %+v", client)
	}
	if client.RequestTimeout.D() != 45*time.Second {
		t.Fatalf("request timeout = %s", client.RequestTimeout.D())
	}
	if client.MessageLimits.MaxBytes != 4<<20 || client.MessageLimits.MaxArtifactBytes != 1<<30 {
		t.Fatalf("message limits = %+v", client.MessageLimits)
	}
}

func TestCoordinatorClientConfigBlockWinsOverTheFile(t *testing.T) {
	home := bootstrapHome(t, validBootstrapDocument, 0o600)
	cfg := clientHostConfig()
	cfg.BacklogV2.CoordinatorClient.CoordinatorID = "operator-choice"
	if err := cfg.applyCoordinatorClientBootstrap(home); err != nil {
		t.Fatal(err)
	}
	if got := cfg.BacklogV2.CoordinatorClient.CoordinatorID; got != "operator-choice" {
		t.Fatalf("coordinator id = %q, want the explicit block to win", got)
	}
}

func TestCoordinatorClientBootstrapAbsentIsNotAnError(t *testing.T) {
	cfg := Default()
	if err := cfg.applyCoordinatorClientBootstrap(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if cfg.BacklogV2.CoordinatorClient.Configured() {
		t.Fatal("an absent file configured a client")
	}
	bootstrap, found, err := LoadCoordinatorClientBootstrap(t.TempDir())
	if err != nil || found || bootstrap.SchemaVersion != 0 {
		t.Fatalf("absent file = %+v, %t, %v", bootstrap, found, err)
	}
}

func TestCoordinatorClientBootstrapRefusals(t *testing.T) {
	tests := map[string]struct {
		document string
		want     string
	}{
		"unknown field": {
			document: `{"schema_version":1,"coordinator_id":"normandy","address":"normandy",` +
				`"connection":"ssh","remote_command":"t3-steward",` +
				`"credential_ref":"secretref:f03-admin/omarchy-pc","request_timeout":"30s",` +
				`"quota_pools":{"codex":1}}`,
			want: "unknown field",
		},
		"inline secret": {
			document: `{"schema_version":1,"coordinator_id":"normandy","address":"normandy",` +
				`"connection":"ssh","remote_command":"t3-steward",` +
				`"credential_ref":"secretref:f03-admin/omarchy-pc","request_timeout":"30s",` +
				`"credential":"hunter2"}`,
			want: "unknown field",
		},
		"wrong schema version": {
			document: strings.Replace(validBootstrapDocument, `"schema_version": 1`, `"schema_version": 2`, 1),
			want:     "schema_version must be 1",
		},
		"worker credential": {
			document: strings.Replace(validBootstrapDocument, "f03-admin", "f02-protocol", 1),
			want:     "worker protocol reference",
		},
		"unsupported connection": {
			document: strings.Replace(validBootstrapDocument, `"connection": "ssh"`, `"connection": "http"`, 1),
			want:     "connection must be ssh",
		},
		"unsafe address": {
			document: strings.Replace(validBootstrapDocument, `"address": "normandy"`, `"address": "-oProxyCommand=id"`, 1),
			want:     "address is invalid",
		},
		"unparsable timeout": {
			document: strings.Replace(validBootstrapDocument, `"request_timeout": "45s"`, `"request_timeout": "soon"`, 1),
			want:     "request_timeout must be a duration",
		},
		"trailing content": {
			document: validBootstrapDocument + "\n{}",
			want:     "trailing content",
		},
		"non-positive limit": {
			document: strings.Replace(validBootstrapDocument, `"max_bytes": 4194304`, `"max_bytes": 0`, 1),
			want:     "message limits must be positive",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			home := bootstrapHome(t, test.document, 0o600)
			_, _, err := LoadCoordinatorClientBootstrap(home)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want one naming %q", err, test.want)
			}
		})
	}
}

func TestCoordinatorClientBootstrapRefusesUnprotectedFile(t *testing.T) {
	home := bootstrapHome(t, validBootstrapDocument, 0o644)
	_, _, err := LoadCoordinatorClientBootstrap(home)
	if err == nil || !strings.Contains(err.Error(), "owner-only regular 0600 file") {
		t.Fatalf("error = %v", err)
	}
	// A symlink is not a regular file and must be refused too, so that the
	// file cannot be pointed at something the operator did not write.
	linkHome := t.TempDir()
	path := filepath.Join(linkHome, CoordinatorClientBootstrapPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(linkHome, "elsewhere.json")
	if err := os.WriteFile(target, []byte(validBootstrapDocument), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadCoordinatorClientBootstrap(linkHome); err == nil {
		t.Fatal("a symlinked bootstrap was accepted")
	}
}
