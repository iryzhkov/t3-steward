package workerproto

import (
	"errors"
	"strings"
	"testing"
)

func TestArtifactSizeErrorOnlyKnownOverflowAndSafeIdentity(t *testing.T) {
	object := ArtifactObject{ID: "output-secret", Path: "results/credential-secret.txt", Kind: "output", MediaType: "text/plain", Size: 100, SHA256: strings.Repeat("a", 64)}
	err := ValidateArtifactObject(object, 10)
	var size *ArtifactSizeError
	if !errors.As(err, &size) || size.Limit != 10 || size.Observed != 100 || size.Scope != "object" {
		t.Fatalf("known overflow=%v", err)
	}
	if strings.Contains(err.Error(), "secret") || len(err.Error()) > 200 {
		t.Fatalf("unsafe diagnostic=%v", err)
	}
	for _, mutate := range []func(*ArtifactObject){
		func(o *ArtifactObject) { o.Size = -1 },
		func(o *ArtifactObject) { o.Path = "../escape" },
		func(o *ArtifactObject) { o.SHA256 = "invalid" },
	} {
		invalid := object
		mutate(&invalid)
		size = nil
		if err := ValidateArtifactObject(invalid, 10); err == nil || errors.As(err, &size) {
			t.Fatalf("invalid metadata classified as overflow: %v", err)
		}
	}
	size = nil
	if err := ValidateArtifactObject(object, 0); err == nil || errors.As(err, &size) {
		t.Fatalf("invalid limit classified: %v", err)
	}
}
