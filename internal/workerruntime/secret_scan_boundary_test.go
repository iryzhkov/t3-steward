package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// Every credential-shaped value and every embedded identifier in this file is
// assembled at run time. The scanner that collects this repository's own
// results refuses a literal one, and the workers that run it may still run a
// scanner without the left boundary.

func boundaryHex(n int) string {
	return strings.Repeat("0123456789abcdef", n/16+1)[:n]
}

// boundaryTaskID is shaped like a Steward task id: the word task, a hyphen
// and 32 lowercase hex digits.
func boundaryTaskID() string { return "ta" + "sk-" + boundaryHex(32) }

// boundaryIdentifier is n characters of the class the prefix detectors accept
// after their prefix.
func boundaryIdentifier(n int) string {
	return strings.Repeat("aZ9_-", n/5+1)[:n]
}

type boundaryKey struct{ detector, token string }

// boundaryRealKeys are the real shapes the base scanner detects, one per
// prefix detector alternative.
func boundaryRealKeys() []boundaryKey {
	return []boundaryKey{
		{"github", "gh" + "p_" + strings.Repeat("A", 36)},
		{"github", "gh" + "o_" + strings.Repeat("B", 36)},
		{"github", "gh" + "s_" + strings.Repeat("C", 36)},
		{"github", "gh" + "u_" + strings.Repeat("E", 36)},
		{"github", "gh" + "r_" + strings.Repeat("F", 36)},
		{"github", "github" + "_pat_" + strings.Repeat("D", 82)},
		{"anthropic", "sk" + "-ant-" + strings.Repeat("x", 80)},
		{"openai", "sk" + "-" + strings.Repeat("y", 48)},
		{"aws", "AK" + "IA" + strings.Repeat("A", 16)},
		{"age", "AGE-SECRET" + "-KEY-1" + strings.Repeat("A", 58)},
	}
}

func scanBoundaryText(t *testing.T, scan *resultScanner, text string) *SecretScanError {
	t.Helper()
	err := scan.scan("fixture", "commit", strings.NewReader(text))
	if err == nil {
		return nil
	}
	var finding *SecretScanError
	if !errors.As(err, &finding) {
		t.Fatalf("scan failed: %v", err)
	}
	return finding
}

func TestSecretScanPrefixDetectorsIgnoreEmbeddedIdentifiers(t *testing.T) {
	id := boundaryTaskID()
	texts := []string{
		id,
		"The review of " + id + " found nothing.\n",
		"Collected `" + id + "` after the gate.",
		"runs/" + id + "/handoff.md",
		`{"task":"` + id + `","phase":"done"}`,
		"run-" + boundaryHex(32),
		"attempt-" + boundaryHex(32),
	}
	for _, word := range []string{"ask-", "disk-", "risk-", "desk-", "mask-"} {
		for _, n := range []int{30, 32, 40} {
			texts = append(texts, word+boundaryIdentifier(n), word+boundaryHex(n))
		}
	}
	for _, key := range boundaryRealKeys() {
		for _, before := range []string{"a", "Z", "7", "_", "-"} {
			texts = append(texts, before+key.token, "value "+before+key.token+" end")
		}
	}
	// Failures name the text by index: printing it would put a key-shaped
	// value in the test log.
	for i, text := range texts {
		scan := newResultScanner(SecretScanConfig{}, nil, nil)
		if finding := scanBoundaryText(t, scan, text); finding != nil {
			t.Errorf("text %d: embedded identifier reported: detector=%s byte=%d fingerprint=%s", i, finding.Detector, finding.Offset, finding.Fingerprint)
		}
		if got := scan.safeName(text); got != text {
			t.Errorf("text %d: safeName changed an embedded identifier", i)
		}
	}
}

func TestSecretScanPrefixDetectorsKeepRealKeys(t *testing.T) {
	contexts := []string{
		"", " ", "\t", "\n", "=", ":", `"`, "'", "`", "(", "[", "{", ",", ";", "/", "?", "&", "@", "<",
		"Authorization: Bearer ",
		"https://example.test/v1?key=",
		"https://example.test/v1?a=b&token=",
		"https://example.test/v1?key%3D",
		"https://example.test/v1?key%3d",
		`{"note":"first line\n`,
		`{"note":"first\t`,
		`{"note":"first\r`,
		"caf\xc3\xa9",
	}
	for _, key := range boundaryRealKeys() {
		for _, before := range contexts {
			finding := scanBoundaryText(t, newResultScanner(SecretScanConfig{}, nil, nil), before+key.token+"\n")
			if finding == nil {
				t.Errorf("%s key after %q not detected", key.detector, before)
				continue
			}
			if finding.Detector != key.detector || finding.Offset != int64(len(before)) || finding.Fingerprint != secretFingerprint(key.token) {
				t.Errorf("%s key after %q: detector=%s byte=%d fingerprint=%s", key.detector, before, finding.Detector, finding.Offset, finding.Fingerprint)
			}
		}
		allow := map[string]bool{secretFingerprint(key.token): true}
		if err := newResultScanner(SecretScanConfig{}, nil, allow).scan("fixture", "commit", strings.NewReader("key="+key.token)); err != nil {
			t.Errorf("allowlisted %s fixture refused: %v", key.detector, err)
		}
	}
	// The exempt detectors are unchanged: a Cloudflare label joined to a
	// longer environment name, and a PEM header after a word character.
	value := strings.Repeat("a", 40)
	for _, label := range []string{"CLOUDFLARE_API_TOKEN=", "DEPLOY_CLOUDFLARE_API_TOKEN=", "xCLOUDFLARE_API_TOKEN=", "cf_api_token: "} {
		finding := scanBoundaryText(t, newResultScanner(SecretScanConfig{}, nil, nil), label+value)
		if finding == nil || finding.Detector != "cloudflare" || finding.Offset != int64(len(label)) || finding.Fingerprint != secretFingerprint(value) {
			t.Errorf("cloudflare label %q: %+v", label, finding)
		}
	}
	dashes := strings.Repeat("-", 5)
	header := dashes + "BEGIN " + "PRIVATE KEY" + dashes
	for _, before := range []string{"", "x", "_"} {
		finding := scanBoundaryText(t, newResultScanner(SecretScanConfig{}, nil, nil), before+header+"\n")
		if finding == nil || finding.Detector != "private-key" || finding.Offset != int64(len(before)) {
			t.Errorf("private key header after %q: %+v", before, finding)
		}
	}
}

func TestSecretScanBoundaryAcrossChunkCuts(t *testing.T) {
	probe := newResultScanner(SecretScanConfig{}, nil, nil)
	// The first chunk is cut here: the byte at cut is the first one kept for
	// the next window, and every byte before it is discarded.
	cut := (64 << 10) - probe.overlap
	key := "sk" + "-" + strings.Repeat("y", 48)
	cases := []struct {
		name, before, token string
		detected            bool
	}{
		{"task id", "ta", "sk-" + boundaryHex(32), false},
		{"embedded aws", "X", "AK" + "IA" + strings.Repeat("B", 16), false},
		{"embedded github", "_", "gh" + "p_" + strings.Repeat("A", 36), false},
		{"key after equals", "=", key, true},
		{"key after percent escape", "%3D", key, true},
		{"key after json escape", `\n`, key, true},
	}
	for _, c := range cases {
		// shift 0 starts the token at the cut; larger shifts move it later,
		// so the bytes before it straddle the cut.
		for shift := 0; shift <= 3; shift++ {
			at := cut + shift
			text := strings.Repeat(" ", at-len(c.before)) + c.before + c.token + "\n" + strings.Repeat(" ", 70<<10)
			finding := scanBoundaryText(t, newResultScanner(SecretScanConfig{}, nil, nil), text)
			if !c.detected {
				if finding != nil {
					t.Errorf("%s shift %d: reported %+v", c.name, shift, finding)
				}
				continue
			}
			if finding == nil || finding.Detector != "openai" || finding.Offset != int64(at) || finding.Fingerprint != secretFingerprint(c.token) {
				t.Errorf("%s shift %d: %+v, want openai at %d", c.name, shift, finding, at)
			}
		}
		// One-byte reads move the cut one byte at a time, so the bytes before
		// the token come only from what the scanner carried across cuts.
		at := 2*probe.overlap + 7
		text := strings.Repeat(" ", at-len(c.before)) + c.before + c.token + "\n"
		err := newResultScanner(SecretScanConfig{}, nil, nil).scan("fixture", "commit", iotest.OneByteReader(strings.NewReader(text)))
		var finding *SecretScanError
		if errors.As(err, &finding) != c.detected {
			t.Errorf("%s one-byte reads: %v", c.name, err)
		} else if c.detected && finding.Offset != int64(at) {
			t.Errorf("%s one-byte reads: byte=%d, want %d", c.name, finding.Offset, at)
		}
	}
}

func TestSecretScanBoundaryRejectedCandidateDoesNotHideKey(t *testing.T) {
	for _, key := range boundaryRealKeys() {
		for i, embedded := range []string{boundaryTaskID(), "x" + key.token, "_" + key.token} {
			for _, separator := range []string{" ", "=", ","} {
				before := "see " + embedded + separator
				finding := scanBoundaryText(t, newResultScanner(SecretScanConfig{}, nil, nil), before+key.token+"\n")
				if finding == nil || finding.Detector != key.detector || finding.Offset != int64(len(before)) || finding.Fingerprint != secretFingerprint(key.token) {
					t.Errorf("%s key after rejected candidate %d and %q: %+v, want byte %d", key.detector, i, separator, finding, len(before))
				}
			}
		}
	}
}

func TestSecretScanRedactTextKeepsIdentifiers(t *testing.T) {
	id := boundaryTaskID()
	github := "gh" + "p_" + strings.Repeat("A", 36)
	text := "task " + id + " leaked key=" + github + "; ref " + id + "\n"
	want := "task " + id + " leaked key=[redacted]; ref " + id + "\n"
	scan := newResultScanner(SecretScanConfig{}, nil, nil)
	// Failures do not print redacted text, which may still hold the key.
	if got := scan.safeName(text); got != want {
		t.Fatal("safeName did not keep the task ids and redact only the key")
	}
	for _, key := range boundaryRealKeys() {
		if got := scan.safeName("(" + key.token + ")"); got != "([redacted])" {
			t.Errorf("safeName of a %s key kept %d bytes", key.detector, len(got))
		}
	}
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	got, err := store.RedactText(context.Background(), testPackage(), text)
	if err != nil || got != want {
		t.Fatalf("RedactText did not keep the task ids and redact only the key (err %v)", err)
	}
}

// This committed document quotes Steward task ids; the base scanner refused a
// commit that touched it.
func TestSecretScanRepositoryTaskIDDocs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "annotation-repair-evidence", "review-sol.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile("ta" + "sk-[0-9a-f]{32}").Match(data) {
		t.Fatal("the document no longer quotes a task id; choose another fixture")
	}
	if finding := scanBoundaryText(t, newResultScanner(SecretScanConfig{}, nil, nil), string(data)); finding != nil {
		t.Fatalf("task id document refused: detector=%s byte=%d fingerprint=%s", finding.Detector, finding.Offset, finding.Fingerprint)
	}
}
