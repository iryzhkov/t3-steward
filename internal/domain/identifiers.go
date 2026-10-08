package domain

import (
	"errors"
	"fmt"
	"strings"
)

// MaxPathComponentBytes is the longest path component Steward accepts in an
// authored name. It is the NAME_MAX of every filesystem a worker or the
// coordinator stores files on, so a longer component is refused when it is
// written rather than when a worker first tries to create it.
const MaxPathComponentBytes = 255

// MaxIdempotencyKeyBytes is the coordinator's limit on a submission key.
const MaxIdempotencyKeyBytes = 256

// IdempotencyKeyRule is the one message every refusal of a submission key
// uses, on the coordinator and in the client, so that a key refused offline
// is refused in the coordinator's own words.
const IdempotencyKeyRule = "submission idempotency key must be 1-256 trimmed bytes with no control characters"

// ContainsControl reports whether value holds a C0 control character
// (U+0000 to U+001F) or DEL (U+007F). Such bytes are never part of a name a
// person typed on purpose, and printed raw they rewrite the terminal reading
// them.
func ContainsControl(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0
}

// ValidateIdempotencyKey applies IdempotencyKeyRule to a submission key.
func ValidateIdempotencyKey(key string) error {
	if key == "" || strings.TrimSpace(key) != key || len(key) > MaxIdempotencyKeyBytes || ContainsControl(key) {
		return errors.New(IdempotencyKeyRule)
	}
	return nil
}

// ValidateStorageComponent is the rule for a coordinator identity that names
// a directory on disk, such as a run ID inside a cross-run node reference: one
// nonempty path component of at most MaxPathComponentBytes bytes that is not
// "." or "..", contains no separator or control character, and does not
// start with "-", so that it can never be read as an option by a command it
// is passed to. It is deliberately looser than PathSafeID: run IDs written
// before rc119, such as "run:...", carry colons and stay valid references.
func ValidateStorageComponent(value string) error {
	switch {
	case value == "":
		return errors.New("is empty")
	case value == "." || value == "..":
		return errors.New(`must not be "." or ".."`)
	case strings.ContainsAny(value, `/\`):
		return errors.New("must not contain a path separator")
	case ContainsControl(value):
		return errors.New("must not contain control characters")
	case strings.HasPrefix(value, "-"):
		return errors.New("must not start with -")
	case len(value) > MaxPathComponentBytes:
		return fmt.Errorf("must be at most %d bytes", MaxPathComponentBytes)
	}
	return nil
}

// ValidateDerivedIDs refuses any identity that is not PathSafeID. Derived
// identities (DerivedID) are path-safe by construction; the stores that write
// them check anyway, so that a future derivation that is not cannot reach a
// directory name or a git ref unnoticed.
func ValidateDerivedIDs(label string, ids ...string) error {
	for _, id := range ids {
		if !PathSafeID(id) {
			return fmt.Errorf("%s identity %q is not path safe", label, id)
		}
	}
	return nil
}
