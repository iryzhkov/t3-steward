package workerruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkerBootstrapStrictU0Contract(t *testing.T) {
	valid := []byte(`{"schema_version":1,"worker_id":"homelab","coordinator_id":"normandy","transport":"ssh","capabilities":["git","huyang"],"provider_routes":[],"credential_ref":"secretref:f02-protocol/homelab"}`)
	b, digest, err := DecodeWorkerBootstrap(valid)
	if err != nil || b.WorkerID != "homelab" || len(digest) != 64 {
		t.Fatal(b, digest, err)
	}
	for _, raw := range []string{
		strings.Replace(string(valid), `"schema_version":1`, `"schema_version":2`, 1),
		strings.Replace(string(valid), `["git","huyang"]`, `["huyang","git"]`, 1),
		strings.Replace(string(valid), `"provider_routes":[]`, `"provider_routes":null`, 1),
		strings.Replace(string(valid), `"provider_routes":[]`, `"provider_routes":[],"secret":"value"`, 1),
		strings.Replace(string(valid), "secretref:f02-protocol/homelab", "secretref:f02-protocol/other", 1),
		string(valid) + "{}",
	} {
		if _, _, err = DecodeWorkerBootstrap([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid bootstrap: %s", raw)
		}
	}
}
func TestProtocolFileReferenceCustody(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, ".config/upkeeper/secrets/f02-protocol")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "homelab")
	credentials := ProtocolCredentials{CoordinatorPrincipal: "ssh:normandy", CoordinatorKeyID: "coordinator", CoordinatorSecret: []byte(strings.Repeat("c", 32)), WorkerPrincipal: "ssh:homelab", WorkerKeyID: "worker", WorkerSecret: []byte(strings.Repeat("w", 32))}
	raw, _ := json.Marshal(credentials)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	resolver := FileProtocolCredentialResolver{Home: home}
	got, err := resolver.ResolveProtocol(context.Background(), "secretref:f02-protocol/homelab")
	if err != nil || got.WorkerPrincipal != credentials.WorkerPrincipal {
		t.Fatal(got.WorkerPrincipal, err)
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = resolver.ResolveProtocol(context.Background(), "secretref:f02-protocol/homelab"); err == nil {
		t.Fatal("world-readable credential accepted")
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(home, "outside")
	if err = os.WriteFile(outside, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if _, err = resolver.ResolveProtocol(context.Background(), "secretref:f02-protocol/homelab"); err == nil {
		t.Fatal("symlink credential accepted")
	}
	if _, err = resolver.ResolveProtocol(context.Background(), "secretref:f02-protocol/../outside"); err == nil {
		t.Fatal("escaped reference accepted")
	}
}
