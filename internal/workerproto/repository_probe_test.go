package workerproto

import (
	"strings"
	"testing"
	"time"
)

// TestRepositoryProbeRequestRefusesOptionShapedValues is the injection case at
// the transport boundary: a value that Git could read as an option never becomes
// a message, so it cannot reach a worker even if a caller skipped the catalog
// validators.
func TestRepositoryProbeRequestRefusesOptionShapedValues(t *testing.T) {
	tests := []struct {
		name    string
		request RepositoryProbeRequest
	}{
		{
			name:    "option shaped repository",
			request: RepositoryProbeRequest{Repository: "--upload-pack=touch /tmp/pwned", Ref: "main"},
		},
		{
			name:    "option shaped ref",
			request: RepositoryProbeRequest{Repository: "https://example.invalid/x.git", Ref: "-o"},
		},
		{
			name:    "option shaped credential reference",
			request: RepositoryProbeRequest{Repository: "https://example.invalid/x.git", Ref: "main", CredentialRefs: []string{"-o"}},
		},
		{
			name:    "newline in ref",
			request: RepositoryProbeRequest{Repository: "https://example.invalid/x.git", Ref: "main\nrefs/heads/other"},
		},
		{
			name:    "nul in repository",
			request: RepositoryProbeRequest{Repository: "https://example.invalid/x\x00.git", Ref: "main"},
		},
		{
			name:    "absent repository",
			request: RepositoryProbeRequest{Ref: "main"},
		},
		{
			name:    "absent ref",
			request: RepositoryProbeRequest{Repository: "https://example.invalid/x.git"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateRepositoryProbeRequest(test.request); err == nil {
				t.Fatal("the transport accepted a value it must refuse")
			}
		})
	}
}

// TestRepositoryProbeRequestBounds states that every quantity in the message is
// bounded before it is sent, not after a worker has already spent it.
func TestRepositoryProbeRequestBounds(t *testing.T) {
	base := RepositoryProbeRequest{Repository: "https://example.invalid/x.git", Ref: "main"}
	if err := ValidateRepositoryProbeRequest(base); err != nil {
		t.Fatalf("a minimal request was refused: %v", err)
	}

	oversized := base
	oversized.Repository = "https://example.invalid/" + strings.Repeat("x", MaxRepositoryProbeValueBytes)
	if err := ValidateRepositoryProbeRequest(oversized); err == nil {
		t.Fatal("an oversized repository was accepted")
	}

	tooManyReferences := base
	for range MaxRepositoryProbeCredentialRefs + 1 {
		tooManyReferences.CredentialRefs = append(tooManyReferences.CredentialRefs, "secretref:f03-admin/homelab")
	}
	if err := ValidateRepositoryProbeRequest(tooManyReferences); err == nil {
		t.Fatal("an unbounded credential reference list was accepted")
	}

	unboundedOutput := base
	unboundedOutput.MaxOutputBytes = MaxRepositoryProbeOutputBytes + 1
	if err := ValidateRepositoryProbeRequest(unboundedOutput); err == nil {
		t.Fatal("an output bound above the limit was accepted")
	}

	longTimeout := base
	longTimeout.TimeoutSeconds = int(MaxRepositoryProbeTimeout/time.Second) + 1
	if err := ValidateRepositoryProbeRequest(longTimeout); err == nil {
		t.Fatal("a timeout above the limit was accepted")
	}

	negative := base
	negative.TimeoutSeconds = -1
	if err := ValidateRepositoryProbeRequest(negative); err == nil {
		t.Fatal("a negative timeout was accepted")
	}
}

// TestRepositoryObservationIsBoundedAndClassified states what a coordinator
// refuses to act on: an answer with no classification, and an answer whose
// detail ignored the bound.
func TestRepositoryObservationIsBoundedAndClassified(t *testing.T) {
	if err := ValidateRepositoryObservation(RepositoryObservation{}); err == nil {
		t.Fatal("an unclassified observation was accepted")
	}
	oversized := RepositoryObservation{
		Class:  "authenticated-ok",
		Detail: strings.Repeat("x", MaxRepositoryProbeDetailBytes+1),
	}
	if err := ValidateRepositoryObservation(oversized); err == nil {
		t.Fatal("an oversized detail was accepted")
	}
	bounded := BoundRepositoryProbeDetail(oversized.Detail)
	if len(bounded) > MaxRepositoryProbeDetailBytes {
		t.Fatalf("bounded detail is %d bytes", len(bounded))
	}
	if err := ValidateRepositoryObservation(RepositoryObservation{Class: "authenticated-ok", Detail: bounded}); err != nil {
		t.Fatal(err)
	}
	// Truncation never splits a rune, so a bounded detail stays printable.
	multibyte := BoundRepositoryProbeDetail(strings.Repeat("é", MaxRepositoryProbeDetailBytes))
	if !strings.HasPrefix(strings.Repeat("é", MaxRepositoryProbeDetailBytes), multibyte) {
		t.Fatalf("bounded detail is not a prefix of the original: %q", multibyte)
	}
}
