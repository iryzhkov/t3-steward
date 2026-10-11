package workerproto

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ArtifactPart describes one ordered physical object of a logical artifact.
type ArtifactPart struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// ArtifactPartObject is the ordinary content-addressed object used to transfer a part.
func ArtifactPartObject(object ArtifactObject, index int) ArtifactObject {
	part := object.Parts[index]
	return ArtifactObject{ID: fmt.Sprintf("part-%s-%d", object.SHA256, index), Path: fmt.Sprintf("%s.part-%d", object.Path, index), Kind: object.Kind, MediaType: object.MediaType, Size: part.Size, SHA256: part.SHA256}
}

// SplitArtifact computes ordered part digests and verifies the original whole-file identity.
// Small objects are returned unchanged. Callers must enforce their aggregate budget first.
func SplitArtifact(reader io.Reader, object ArtifactObject, partLimit int64) (ArtifactObject, error) {
	if partLimit <= 0 || object.Size < 0 {
		return ArtifactObject{}, errors.New("artifact split: invalid size or limit")
	}
	partLimit = min(partLimit, StreamArtifactLimit)
	if object.Size <= partLimit {
		return object, nil
	}
	if object.Size > 1<<30 {
		return ArtifactObject{}, errors.New("artifact split: aggregate size exceeds 1 GiB")
	}
	object.Parts = nil
	whole := sha256.New()
	for remaining := object.Size; remaining > 0; {
		size := min(remaining, partLimit)
		digest := sha256.New()
		n, err := io.CopyN(io.MultiWriter(whole, digest), reader, size)
		if err != nil || n != size {
			return ArtifactObject{}, errors.New("artifact split: truncated content")
		}
		object.Parts = append(object.Parts, ArtifactPart{Size: size, SHA256: hex.EncodeToString(digest.Sum(nil))})
		remaining -= size
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || err != io.EOF {
		return ArtifactObject{}, errors.New("artifact split: trailing content")
	}
	if !strings.EqualFold(hex.EncodeToString(whole.Sum(nil)), object.SHA256) {
		return ArtifactObject{}, errors.New("artifact split: whole checksum mismatch")
	}
	return object, nil
}

func validateArtifactParts(object ArtifactObject, maxBytes int64) error {
	if len(object.Parts) == 0 {
		return NewArtifactSizeError("object", maxBytes, uint64(object.Size), object)
	}
	// Bound metadata as well as bytes; even a malicious list of tiny parts is finite.
	if len(object.Parts) > 4096 || object.Size > 1<<30 {
		return errors.New("artifact parts: excessive contents")
	}
	var total int64
	for _, part := range object.Parts {
		if part.Size <= 0 || part.Size > min(maxBytes, StreamArtifactLimit) || !validSHA256(part.SHA256) || part.Size > object.Size-total {
			return errors.New("artifact parts: invalid size or checksum")
		}
		total += part.Size
	}
	if total != object.Size {
		return errors.New("artifact parts: total size mismatch")
	}
	return nil
}

func verifyArtifactParts(reader io.Reader, object ArtifactObject, maxBytes int64) error {
	whole := sha256.New()
	for index := range object.Parts {
		part := ArtifactPartObject(object, index)
		if err := VerifyArtifact(io.TeeReader(io.LimitReader(reader, part.Size), whole), part, maxBytes); err != nil {
			return fmt.Errorf("artifact part %d: %w", index, err)
		}
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || err != io.EOF {
		return errors.New("artifact parts: trailing content")
	}
	if !strings.EqualFold(hex.EncodeToString(whole.Sum(nil)), object.SHA256) {
		return errors.New("artifact parts: whole checksum mismatch")
	}
	return nil
}
