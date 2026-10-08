package domain

import (
	"crypto/sha256"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestDerivedIDShapeDeterminismAndSeparation(t *testing.T) {
	key := "key:with/slashes\\ spaces"
	ids := []struct{ kind, id string }{
		{"run", RerunRunID(key)}, {"task", RerunTaskID(key, 0)}, {"input", RerunInputID(key, 0)}, {"input", RerunPromptInputID(key)},
		{"run", CloneRunID(key)}, {"task", CloneTaskID(key, 0)}, {"input", CloneInputID(key, 0)},
		{"task", GraphTaskID(key)}, {"input", GraphPromptInputID(key)},
		{"attempt", FirstAttemptID("rerun", "task:old:0")}, {"attempt", FirstAttemptID("clone", "task:old:0")}, {"attempt", FirstAttemptID("graph", "task:old:0")},
		{"attempt", ScheduledAttemptID("run:old", "task:old")}, {"attempt", RecoveryAttemptID(strings.Repeat("a", 64))},
	}
	seen := map[string]bool{}
	for _, item := range ids {
		if !regexp.MustCompile("^"+item.kind+"-[0-9a-f]{32}$").MatchString(item.id) || !PathSafeID(item.id) || strings.ContainsAny(item.id, ":/\\ \t\n"+string(os.PathListSeparator)) {
			t.Fatalf("unsafe %s", item.id)
		}
		if seen[item.id] {
			t.Fatalf("collision: %s", item.id)
		}
		seen[item.id] = true
	}
	replayed := []string{RerunRunID(key), RerunTaskID(key, 0), RerunInputID(key, 0), RerunPromptInputID(key),
		CloneRunID(key), CloneTaskID(key, 0), CloneInputID(key, 0), GraphTaskID(key), GraphPromptInputID(key),
		FirstAttemptID("rerun", "task:old:0"), FirstAttemptID("clone", "task:old:0"), FirstAttemptID("graph", "task:old:0"),
		ScheduledAttemptID("run:old", "task:old"), RecoveryAttemptID(strings.Repeat("a", 64))}
	for i, id := range replayed {
		if id != ids[i].id {
			t.Fatalf("wrapper %d is not deterministic", i)
		}
	}
	sum := sha256.Sum256([]byte("run\x00" + key + "\x000"))
	submitted := fmt.Sprintf("run-%x", sum[:16])
	if seen[submitted] {
		t.Fatal("submission domain collision")
	}
	pairs := [][2]string{
		{DerivedID("a", "run", "k"), DerivedID("b", "run", "k")},
		{DerivedID("a", "run", "k"), DerivedID("a", "task", "k")},
		{RerunRunID("a"), RerunRunID("b")}, {RerunTaskID(key, 0), RerunTaskID(key, 1)},
		{RerunInputID(key, 0), RerunInputID(key, 1)}, {CloneTaskID(key, 0), CloneTaskID(key, 1)},
		{CloneInputID(key, 0), CloneInputID(key, 1)},
		{DerivedID("a", "run", "a", "b"), DerivedID("a", "run", "ab")},
		{ScheduledAttemptID("a", "b"), ScheduledAttemptID("b", "a")},
		{RecoveryAttemptID(strings.Repeat("a", 64)), RecoveryAttemptID(strings.Repeat("a", 63) + "b")},
	}
	for _, p := range pairs {
		if p[0] == p[1] {
			t.Fatalf("not separated: %v", p)
		}
	}
	preimage := "t3-steward/derived-id/v1\x00rerun\x00run\x00" + key
	expected := sha256.Sum256([]byte(preimage))
	if RerunRunID(key) != fmt.Sprintf("run-%x", expected[:16]) {
		t.Fatal("versioned derivation differs")
	}
}

func TestPathSafeIDBounds(t *testing.T) {
	for _, id := range []string{"a", "Z-._0", strings.Repeat("a", 128)} {
		if !PathSafeID(id) {
			t.Fatalf("rejected %q", id)
		}
	}
	for _, id := range []string{"", "-a", ".a", "_a", strings.Repeat("a", 129), "a:b", "a/b", "a\\b", "a b", "a\n", "é", "a\x00"} {
		if PathSafeID(id) {
			t.Fatalf("accepted %q", id)
		}
	}
}
