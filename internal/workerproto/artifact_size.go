package workerproto

import (
	"crypto/sha256"
	"fmt"
)

// ArtifactSizeError identifies a known object or aggregate byte limit rejection.
// Operational failures and invalid limits/metadata do not have this type.
// Identity is a digest, so diagnostics cannot disclose user paths or credentials.
type ArtifactSizeError struct {
	Scope    string
	Limit    int64
	Observed uint64
	Identity string
}

func NewArtifactSizeError(scope string, limit int64, observed uint64, object ArtifactObject) *ArtifactSizeError {
	identity := sha256.Sum256([]byte(object.ID + "\x00" + object.Path))
	return &ArtifactSizeError{Scope: scope, Limit: limit, Observed: observed, Identity: fmt.Sprintf("%x", identity[:8])}
}

func (e *ArtifactSizeError) Error() string {
	return fmt.Sprintf("artifact %s size exceeds limit: limit=%d rejected=%d identity=%s", e.Scope, e.Limit, e.Observed, e.Identity)
}
