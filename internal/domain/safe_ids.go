package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"
	"strings"
)

// DerivedID creates a deterministic, domain-separated identity. Callers use a
// fixed namespace and kind and parts from the operation's existing identity.
// Parts must not contain NUL, which separates fields in the versioned preimage.
func DerivedID(namespace, kind string, parts ...string) string {
	sum := sha256.Sum256([]byte("t3-steward/derived-id/v1\x00" + namespace + "\x00" + kind + "\x00" + strings.Join(parts, "\x00")))
	return kind + "-" + hex.EncodeToString(sum[:16])
}

var pathSafeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// PathSafeID reports whether id is a bounded, portable path component.
func PathSafeID(id string) bool { return pathSafeIDPattern.MatchString(id) }

func RerunRunID(key string) string { return DerivedID("rerun", "run", key) }
func RerunTaskID(key string, index int) string {
	return DerivedID("rerun", "task", key, strconv.Itoa(index))
}
func RerunInputID(key string, index int) string {
	return DerivedID("rerun", "input", key, strconv.Itoa(index))
}
func RerunPromptInputID(key string) string { return DerivedID("rerun", "input", key, "prompt") }
func CloneRunID(key string) string         { return DerivedID("clone", "run", key) }
func CloneTaskID(key string, index int) string {
	return DerivedID("clone", "task", key, strconv.Itoa(index))
}
func CloneInputID(key string, index int) string {
	return DerivedID("clone", "input", key, strconv.Itoa(index))
}
func GraphTaskID(key string) string        { return DerivedID("graph", "task", key) }
func GraphPromptInputID(key string) string { return DerivedID("graph", "input", key) }
func FirstAttemptID(namespace, taskID string) string {
	return DerivedID(namespace, "attempt", taskID, "1")
}
func ScheduledAttemptID(runID, taskID string) string {
	return DerivedID("schedule", "attempt", runID, taskID, "1")
}
func RecoveryAttemptID(digest string) string { return DerivedID("recovery", "attempt", digest) }
