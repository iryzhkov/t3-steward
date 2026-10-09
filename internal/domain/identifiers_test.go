package domain

import (
	"strings"
	"testing"
)

func TestValidateIdempotencyKey(t *testing.T) {
	for name, key := range map[string]string{
		"empty":           "",
		"leading space":   " key",
		"trailing tab":    "key\t",
		"257 bytes":       strings.Repeat("k", 257),
		"400 bytes":       strings.Repeat("k", 400),
		"NUL":             "ke\x00y",
		"escape sequence": "key\x1b[2J",
		"inner newline":   "ke\ny",
		"delete":          "key\x7fz",
	} {
		if err := ValidateIdempotencyKey(key); err == nil || err.Error() != IdempotencyKeyRule {
			t.Errorf("%s: err = %v, want %q", name, err, IdempotencyKeyRule)
		}
	}
	for _, key := range []string{"k", "release-2026-10-08", "with inner space", "ключ", strings.Repeat("k", 256)} {
		if err := ValidateIdempotencyKey(key); err != nil {
			t.Errorf("key %q refused: %v", key, err)
		}
	}
}

func TestValidateStorageComponent(t *testing.T) {
	for _, value := range []string{"", ".", "..", "a/b", `a\b`, "-x", "run\x01", "run\n", "run\x7f", strings.Repeat("r", 256)} {
		if err := ValidateStorageComponent(value); err == nil {
			t.Errorf("component %q accepted", value)
		}
	}
	for _, value := range []string{"run-0123abcd", "run:legacy:7", "run:5e1c0b9e-1d2f-4c1a-9a0e-6b1a0d9f9e10", "a.b", strings.Repeat("r", 255)} {
		if err := ValidateStorageComponent(value); err != nil {
			t.Errorf("component %q refused: %v", value, err)
		}
	}
}

func TestParseNodeRefValidatesTheRunPart(t *testing.T) {
	for _, value := range []string{"../probe", "./probe", "-x/probe", "run\x01/probe", "run\n/probe", "run-1/pro\x00be", strings.Repeat("r", 256) + "/probe"} {
		if _, err := ParseNodeRef(value); err == nil {
			t.Errorf("node %q accepted", value)
		} else if strings.ContainsAny(err.Error(), "\x00\x01\n") {
			t.Errorf("refusal of %q is not escaped: %q", value, err.Error())
		}
	}
	for value, want := range map[string]NodeRef{
		"run-0123abcd/probe":  {RunID: "run-0123abcd", TaskID: "probe"},
		"run:legacy:7/probe":  {RunID: "run:legacy:7", TaskID: "probe"},
		"run:legacy:7/sink":   {RunID: "run:legacy:7", TaskID: "sink"},
		"run-1/task:abc:1":    {RunID: "run-1", TaskID: "task:abc:1"},
		"run.with.dots/probe": {RunID: "run.with.dots", TaskID: "probe"},
	} {
		got, err := ParseNodeRef(value)
		if err != nil || got != want {
			t.Errorf("ParseNodeRef(%q) = %+v, %v; want %+v", value, got, err, want)
		}
	}
}

func TestValidateDerivedIDs(t *testing.T) {
	if err := ValidateDerivedIDs("rerun", RerunRunID("key"), RerunTaskID("key", 0), FirstAttemptID("rerun", RerunTaskID("key", 0)), CloneInputID("key", 3)); err != nil {
		t.Fatalf("derived identities refused: %v", err)
	}
	err := ValidateDerivedIDs("rerun", RerunRunID("key"), "rerun:task:a")
	if err == nil || !strings.Contains(err.Error(), `rerun identity "rerun:task:a" is not path safe`) {
		t.Fatalf("err = %v", err)
	}
}
