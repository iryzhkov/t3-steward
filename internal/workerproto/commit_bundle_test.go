package workerproto

import (
	"encoding/json"
	"testing"
)

func TestCommitBundleRequestBounds(t *testing.T) {
	good := CommitBundleRequest{Provenance: json.RawMessage("{}"), MaxBytes: 64 << 20}
	if err := ValidateCommitBundleRequest(good); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []CommitBundleRequest{{MaxBytes: 1}, {Provenance: json.RawMessage("{}"), MaxBytes: 0}, {Provenance: json.RawMessage("{}"), MaxBytes: (64 << 20) + 1}, {Provenance: make([]byte, 65537), MaxBytes: 1}} {
		if err := ValidateCommitBundleRequest(bad); err == nil {
			t.Fatal("accepted invalid request")
		}
	}
}
