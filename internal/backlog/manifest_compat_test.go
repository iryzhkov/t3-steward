package backlog

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// preSupervisionValidateRefusal is the exact output of the release before
// campaign supervision when it is asked to validate the supervised example.
// It was produced by building the binary at origin/main 94a28a0 and running
//
//	t3-steward campaign validate docs/examples/campaign/supervised-three-node
//
// which exited 1. It is reproduced here verbatim because it is the
// compatibility contract for an older peer: a coordinator or a CLI that does
// not implement supervision must refuse a supervised manifest outright, rather
// than ignore the keys it does not understand and run the campaign as if it
// were unsupervised. The mechanism is decoder.KnownFields(true) in
// ParseManifest, which is why the refusal names the two unknown top-level
// fields, the lines they appear on and the Go type they were missing from.
//
// The same text is printed for --json, because the decode fails before any
// document is composed.
const preSupervisionValidateRefusal = "error: docs/examples/campaign/supervised-three-node: " +
	"campaign: decode workflow manifest: yaml: unmarshal errors:\n" +
	"  line 69: field supervision not found in type backlog.Manifest\n" +
	"  line 82: field gates not found in type backlog.Manifest"

// supervisedExampleManifest is the shipped supervised example, read from the
// repository so that the recorded refusal above keeps describing the file it
// was recorded against.
func supervisedExampleManifest(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "examples", "campaign", "supervised-three-node", "workflow.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestPreSupervisionPeerRefusesASupervisedManifest keeps the recorded refusal
// truthful. The refusal names two line numbers, so it describes this example
// only for as long as the example declares supervision and gates on exactly
// those lines. Moving them is allowed; leaving the recorded contract behind is
// not, and this test is what makes that a failure rather than a silent drift.
func TestPreSupervisionPeerRefusesASupervisedManifest(t *testing.T) {
	lines := strings.Split(string(supervisedExampleManifest(t)), "\n")
	for _, expected := range []struct {
		line  int
		field string
	}{{69, "supervision:"}, {82, "gates:"}} {
		if expected.line > len(lines) {
			t.Fatalf("supervised example has %d lines, refusal names line %d", len(lines), expected.line)
		}
		if got := lines[expected.line-1]; got != expected.field {
			t.Fatalf("supervised example line %d = %q, want %q; the recorded refusal\n%s\nno longer describes this file",
				expected.line, got, expected.field, preSupervisionValidateRefusal)
		}
	}
	for _, fragment := range []string{
		"decode workflow manifest",
		"field supervision not found in type backlog.Manifest",
		"field gates not found in type backlog.Manifest",
	} {
		if !strings.Contains(preSupervisionValidateRefusal, fragment) {
			t.Fatalf("recorded refusal is missing %q", fragment)
		}
	}
	// The current binary must of course accept what the old one refuses,
	// otherwise the refusal above would prove nothing about supervision.
	if _, err := ParseManifest(supervisedExampleManifest(t)); err != nil {
		t.Fatalf("this binary refused the supervised example: %v", err)
	}
}

// TestUnknownTopLevelManifestFieldIsRefusedByName exercises the mechanism that
// produced the recorded refusal, inside this package and without a second
// binary. A field this build does not know about is refused by name and by
// line, which is exactly what supervision and gates were to the previous
// release.
//
// Since the version-aware refusal, the message also names the release that
// refused the field and says that a newer release may be required, and it
// keeps the yaml decoder's own text after that, so the recorded refusal above
// stays recognisable in what a newer binary prints.
func TestUnknownTopLevelManifestFieldIsRefusedByName(t *testing.T) {
	t.Cleanup(func() { SetReleaseVersion("dev") })
	SetReleaseVersion("0.11.0-rc.99-test")
	raw := strings.Join([]string{
		"version: 2",
		"name: unknown-field-example",
		"project: example-project",
		"tasks:",
		"  only:",
		"    prompt_file: prompts/only.md",
		"supervision_from_a_later_release:",
		"  route:",
		"    instance: claudeAgent",
		"",
	}, "\n")
	_, err := ParseManifest([]byte(raw))
	if err == nil {
		t.Fatal("an unknown top-level field was accepted")
	}
	for _, fragment := range []string{
		"decode workflow manifest",
		"field supervision_from_a_later_release (line 7) is not supported by this release 0.11.0-rc.99-test; a newer t3-steward release may be required",
		"line 7: field supervision_from_a_later_release not found in type backlog.Manifest",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("refusal %q is missing %q", err, fragment)
		}
	}
	if strings.Index(err.Error(), "is not supported by this release") > strings.Index(err.Error(), "not found in type") {
		t.Fatalf("the version advice must come before the yaml text: %q", err)
	}
}

// preSupervisionManifest is the manifest shape of the release before campaign
// supervision: every field of Manifest except supervision and gates. Decoding
// the supervised example into it is what an older binary does, so the refusal
// it produces is the refusal an older binary built from this code would print.
type preSupervisionManifest struct {
	Version     int                     `yaml:"version"`
	Name        string                  `yaml:"name"`
	Class       string                  `yaml:"class"`
	Placement   ManifestPlacement       `yaml:"placement"`
	Resources   ManifestResources       `yaml:"resources"`
	Preflight   ManifestPreflight       `yaml:"preflight"`
	Environment ManifestEnvironment     `yaml:"environment"`
	Inputs      []string                `yaml:"inputs"`
	Routes      []ManifestRoute         `yaml:"routes"`
	Tasks       map[string]ManifestTask `yaml:"tasks"`
}

// TestPreSupervisionShapedDecoderRefusesTheSupervisedExampleByVersion proves
// the two halves of the compatibility contract together: an older-shaped
// decoder still refuses the supervised example outright, and the refusal it
// prints names the release that refused it and the two unknown fields with
// their lines, followed by the decoder text the recorded refusal was made of.
func TestPreSupervisionShapedDecoderRefusesTheSupervisedExampleByVersion(t *testing.T) {
	t.Cleanup(func() { SetReleaseVersion("dev") })
	SetReleaseVersion("0.11.0-rc.56")
	var older preSupervisionManifest
	err := decodeManifestStrict(supervisedExampleManifest(t), &older)
	if err == nil {
		t.Fatal("an older-shaped decoder accepted the supervised example")
	}
	for _, fragment := range []string{
		"decode workflow manifest",
		"field supervision (line 69) is not supported by this release 0.11.0-rc.56; a newer t3-steward release may be required",
		"field gates (line 82) is not supported by this release 0.11.0-rc.56; a newer t3-steward release may be required",
		"yaml: unmarshal errors:",
		"line 69: field supervision not found in type backlog.preSupervisionManifest",
		"line 82: field gates not found in type backlog.preSupervisionManifest",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("refusal %q is missing %q", err, fragment)
		}
	}
	var unknown *UnknownManifestFieldError
	if !errors.As(err, &unknown) || len(unknown.Fields) != 2 || unknown.Fields[0] != "supervision" || unknown.Fields[1] != "gates" {
		t.Fatalf("refusal does not carry the unknown fields structurally: %#v", err)
	}
}

// TestGatesWithoutSupervisionIsRefused is the other half of the authoring
// contract an older peer relies on: a manifest may not carry gates alone. If
// it could, a campaign would declare review points that nothing is configured
// to decide, and a run would hold forever with no overseer to release it.
func TestGatesWithoutSupervisionIsRefused(t *testing.T) {
	raw := strings.Join([]string{
		"version: 2",
		"name: gates-without-supervision",
		"project: example-project",
		"tasks:",
		"  produce:",
		"    prompt_file: prompts/produce.md",
		"  consume:",
		"    prompt_file: prompts/consume.md",
		"    needs: [produce]",
		"gates:",
		"  review:",
		"    after: [produce]",
		"    before: [consume]",
		"",
	}, "\n")
	if _, err := ParseManifest([]byte(raw)); err == nil {
		t.Fatal("gates without supervision were accepted")
	}
}
