package workerruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestSecretScanPatternsAndChunkBoundaries(t *testing.T) {
	tokens := []string{"ghp_" + strings.Repeat("A", 36), "gho_" + strings.Repeat("B", 36), "ghs_" + strings.Repeat("C", 36), "github_pat_" + strings.Repeat("D", 82), "sk-ant-" + strings.Repeat("x", 80), "sk-" + strings.Repeat("y", 48), "AKIA" + strings.Repeat("A", 16), "CLOUDFLARE_API_TOKEN=" + strings.Repeat("a", 40), "-----BEGIN PRIVATE KEY-----", "AGE-SECRET-KEY-1" + strings.Repeat("A", 58)}
	for _, token := range tokens {
		scan := newResultScanner(SecretScanConfig{}, nil, nil)
		input := strings.Repeat("x", (64<<10)-11) + " " + token + "\n"
		var finding *SecretScanError
		if err := scan.scan("fixture", "commit", strings.NewReader(input)); !errors.As(err, &finding) {
			t.Fatalf("missing token detector for %s", token[:4])
		}
	}
	secret := "synthetic-cross-chunk-secret"
	for _, value := range []string{secret, base64.StdEncoding.EncodeToString([]byte(secret))} {
		var logs bytes.Buffer
		scan := newResultScanner(SecretScanConfig{Log: slog.New(slog.NewTextHandler(&logs, nil))}, []string{secret}, nil)
		err := scan.scan(secret+"-filename", "archive", strings.NewReader(strings.Repeat("x", (64<<10)-3)+value))
		if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
			t.Fatal("boundary or path redaction failed")
		}
	}
}
func TestSecretScanWarnOverrideAndReadError(t *testing.T) {
	token := "ghp_" + strings.Repeat("A", 36)
	var logs bytes.Buffer
	scan := newResultScanner(SecretScanConfig{PatternPolicy: "warn", Log: slog.New(slog.NewTextHandler(&logs, nil))}, nil, nil)
	if err := scan.scan("commit", "commit", strings.NewReader(token)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), token) || !strings.Contains(logs.String(), "github") {
		t.Fatal("unsafe/missing warning")
	}
	if err := scan.scan("output", "output", io.MultiReader(strings.NewReader("safe"), secretFailReader{})); err == nil || strings.Contains(err.Error(), "sensitive") {
		t.Fatal("unsafe read failure")
	}
}

type secretFailReader struct{}

func (secretFailReader) Read([]byte) (int, error) { return 0, errors.New("sensitive I/O details") }

func TestSecretScanServiceCanaries(t *testing.T) {
	checker := EnvironmentCredentialChecker{Lookup: func(name string) (string, bool) { return "synthetic-project-secret", true }}
	resolve := serviceScanCanaries(WorkerServiceOptions{ProjectCredentials: checker, ModelLoginFiles: []string{}}, ProtocolCredentials{CoordinatorSecret: []byte("synthetic-coordinator-secret"), WorkerSecret: []byte("synthetic-worker-secret")}, "unused")
	pkg := testPackage()
	pkg.Environment.RequiredCredentials = []string{"fixture"}
	values, err := resolve(context.Background(), pkg)
	if err != nil || len(values) < 3 {
		t.Fatal("execution credentials missing", err)
	}
	for _, secret := range []string{"synthetic-project-secret", "synthetic-coordinator-secret", "synthetic-worker-secret"} {
		scan := newResultScanner(SecretScanConfig{}, values, nil)
		if err := scan.scan("output", "output", strings.NewReader(secret)); err == nil {
			t.Fatal("known credential admitted")
		}
	}
}
