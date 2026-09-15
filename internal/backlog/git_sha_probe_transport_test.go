package backlog

import (
	"context"
	"strings"
	"testing"
)

func TestPinnedCommitPreservesTransportFailureClassification(t *testing.T) {
	for _, test := range []struct {
		output string
		want   RepositoryReachability
	}{
		{"Permission denied (publickey).", RepositoryAuthenticationFailed},
		{"fatal: repository not found", RepositoryNotFound},
		{"Could not resolve host: example.invalid", RepositoryDNSFailure},
	} {
		runner := &stubRunner{defaults: stubResponse{output: test.output, exit: 128}}
		got, err := ObserveRepository(context.Background(), nil, runner, t.TempDir(), RepositoryProbeKey{Repository: "https://example.invalid/repo.git", Ref: strings.Repeat("a", 40)})
		if err != nil || got.Class != test.want {
			t.Fatalf("got %+v %v, want %s", got, err, test.want)
		}
	}
}
