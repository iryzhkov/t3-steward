package backlog

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestReviewExecutionProfileDecodedKeys(t *testing.T) {
	binary := func(s string) string { return "!!binary " + base64.StdEncoding.EncodeToString([]byte(s)) }
	base := executionManifestYAML()
	canonical := func(raw string) string {
		t.Helper()
		m, err := ParseManifest([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{}))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	golden := canonical(base)
	for name, raw := range map[string]string{
		"binary-all": func() string {
			raw := base
			for _, key := range []string{"execution", "resources", "max_turns", "memory_mb", "scratch_mb"} {
				raw = strings.ReplaceAll(raw, key+":", binary(key)+":")
			}
			return raw
		}(),
		"explicit-str":            strings.ReplaceAll(base, "max_turns:", "!!str max_turns:"),
		"custom-tag":              strings.ReplaceAll(base, "max_turns:", "!local max_turns:"),
		"direct-decoded-override": strings.ReplaceAll(base, "max_turns: 9", "<<: {"+binary("max_turns")+": 32.9}, max_turns: 9"),
		"binary-direct-override":  strings.ReplaceAll(base, "max_turns: 9", "<<: {max_turns: 32.9}, "+binary("max_turns")+": 9"),
		"sequence-decoded-first":  strings.ReplaceAll(base, "max_turns: 9", "<<: [{"+binary("max_turns")+": 9}, {max_turns: 32.9}]"),
		"alias-key-sequence":      strings.ReplaceAll(base, "max_turns: 9", "<<: [{&turn max_turns: 9}, {*turn: 32.9}]"),
		"nested-binary-resource":  strings.ReplaceAll(base, "memory_mb: 512", "<<: [{<<: {"+binary("memory_mb")+": 512}}, {memory_mb: 512.75}]"),
	} {
		t.Run(name, func(t *testing.T) {
			if raw == base {
				t.Fatal("fixture unchanged")
			}
			if canonical(raw) != golden {
				t.Fatal("decoded key changed canonical profile")
			}
		})
	}
	legacy := declaredManifestYAML()
	want := canonical(legacy)
	for _, key := range []string{"required", "role", "id"} {
		t.Run("legacy-binary-"+key, func(t *testing.T) {
			raw := strings.ReplaceAll(legacy, key+":", binary(key)+":")
			if canonical(raw) != want {
				t.Fatal("legacy canonical bytes changed")
			}
		})
	}
}

func TestReviewExecutionProfileDecodedKeyRefusalBeforeMutation(t *testing.T) {
	binary := func(s string) string { return "!!binary " + base64.StdEncoding.EncodeToString([]byte(s)) }
	base := executionManifestYAML()
	cases := map[string]string{
		"decoded-duplicate":           strings.ReplaceAll(base, "max_turns: 9", "max_turns: 9, "+binary("max_turns")+": 9"),
		"decoded-unknown-later-merge": strings.ReplaceAll(base, "max_turns: 9", "<<: [{max_turns: 9}, {"+binary("unknown")+": true}]"),
		"malformed-binary":            strings.ReplaceAll(base, "max_turns: 9", "!!binary '%%%': 9"),
		"custom-tag-fractional":       strings.ReplaceAll(base, "max_turns: 9", "!local max_turns: 32.9"),
		"str-tag-fractional":          strings.ReplaceAll(base, "max_turns: 9", "!!str max_turns: 32.9"),
		"sequence-decoded-first-bad":  strings.ReplaceAll(base, "max_turns: 9", "<<: [{"+binary("max_turns")+": 32.9}, {max_turns: 9}]"),
		"alias-decoded-first-bad":     strings.ReplaceAll(base, "max_turns: 9", "<<: [{&turn "+binary("max_turns")+": 32.9}, {*turn: 9}]"),
		"tagged-merge-nonmarker":      strings.ReplaceAll(base, "max_turns: 9", "!!merge max_turns: 32.9"),
		"quoted-merge-not-merge":      strings.ReplaceAll(base, "max_turns: 9", "'<<': {max_turns: 9}"),
		"binary-merge-not-merge":      strings.ReplaceAll(base, "max_turns: 9", binary("<<")+": {max_turns: 9}"),
		"legacy-binary-null-merge":    strings.ReplaceAll(declaredManifestYAML(), "required: true}", "required: true, <<: [{"+binary("execution")+": null}]}"),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if raw == base {
				t.Fatal("fixture unchanged")
			}
			if _, err := ParseManifest([]byte(raw)); err == nil {
				t.Fatal("unsafe decoded key accepted")
			} else {
				t.Log(err)
			}
			f := executionFixture(t)
			db := stagingSQL(t, f)
			before := independentDeclaredTables(t, db)
			rewriteBundleManifest(t, f.source, raw)
			_, err := (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source)
			if err == nil {
				t.Fatal("permanent admission accepted")
			}
			if before != independentDeclaredTables(t, db) {
				t.Fatal("refusal changed logical SQL/native audit")
			}
		})
	}
}
