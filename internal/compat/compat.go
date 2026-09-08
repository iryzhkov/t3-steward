// Package compat holds the tested T3 server version range. The README's
// compatibility table must match these constants.
package compat

import (
	"fmt"
	"strconv"
	"strings"
)

// Tested range, inclusive. Update both when a new T3 release is verified
// with the end-to-end checklist in docs/t3-protocol.md.
const (
	MinServerVersion = "0.0.38"
	MaxServerVersion = "0.0.38"
)

// Status is the compatibility verdict for one server version.
type Status int

const (
	// Supported means the version is inside the tested range.
	Supported Status = iota
	// Untested means the version is newer than the tested range. The
	// protocol probably works but no release verified it.
	Untested
	// TooOld means the version predates the API surface the watchdog uses.
	TooOld
	// Unknown means the version string could not be parsed.
	Unknown
)

func (s Status) String() string {
	switch s {
	case Supported:
		return "supported"
	case Untested:
		return "untested (newer than the tested range)"
	case TooOld:
		return "too old"
	default:
		return "unknown"
	}
}

// Check classifies a server version string.
func Check(version string) Status {
	v, ok := parse(version)
	if !ok {
		return Unknown
	}
	minV, _ := parse(MinServerVersion)
	maxV, _ := parse(MaxServerVersion)
	if compare(v, minV) < 0 {
		return TooOld
	}
	if compare(v, maxV) > 0 {
		return Untested
	}
	return Supported
}

// ControlAllowed reports whether control actions may run against the
// version, given the user's override.
func ControlAllowed(version string, override bool) (bool, string) {
	st := Check(version)
	switch st {
	case Supported:
		return true, ""
	case Untested:
		if override {
			return true, fmt.Sprintf("T3 %s is newer than the tested range (%s..%s); control enabled by allow_unsupported_version", version, MinServerVersion, MaxServerVersion)
		}
		return false, fmt.Sprintf("T3 %s is newer than the tested range (%s..%s); control actions disabled, monitoring only. Set t3.allow_unsupported_version: true to override", version, MinServerVersion, MaxServerVersion)
	case TooOld:
		if override {
			return true, fmt.Sprintf("T3 %s is older than the minimum %s; control enabled by allow_unsupported_version", version, MinServerVersion)
		}
		return false, fmt.Sprintf("T3 %s is older than the minimum %s; control actions disabled, monitoring only", version, MinServerVersion)
	default:
		if override {
			return true, fmt.Sprintf("T3 version %q could not be parsed; control enabled by allow_unsupported_version", version)
		}
		return false, fmt.Sprintf("T3 version %q could not be parsed; control actions disabled, monitoring only", version)
	}
}

func parse(v string) ([3]int, bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return [3]int{}, false
	}
	var out [3]int
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return [3]int{}, false
		}
		out[i] = n
	}
	return out, true
}

func compare(a, b [3]int) int {
	for i := 0; i < 3; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}
