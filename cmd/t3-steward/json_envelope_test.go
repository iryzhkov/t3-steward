package main

import (
	"encoding/json"
	"testing"
)

// F-8: the --json error envelope was printed by half the verbs (backlog,
// schedules, coordinator identity) and only for transport-classified errors;
// campaign and wait returned their errors straight to main, which printed
// prose to stderr and nothing to stdout, although the campaign help promises
// the envelope on every verb. The envelope is now applied once, in run(), for
// every command whose arguments contain --json.
func TestRunPrintsJSONErrorEnvelopeOnEveryVerb(t *testing.T) {
	decode := func(t *testing.T, label string, out string) map[string]any {
		t.Helper()
		assertExactlyOneJSONDocument(t, label, []byte(out))
		var document map[string]any
		if err := json.Unmarshal([]byte(out), &document); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if document["kind"] != "error" || document["version"] == "" || document["message"] == "" {
			t.Fatalf("%s: envelope %v lacks kind, version or message", label, document)
		}
		return document
	}

	t.Run("campaign check against an unreachable coordinator", func(t *testing.T) {
		root := campaignFixture(t)
		var err error
		output := captureStdout(t, func() {
			err = run([]string{"campaign", "check", root, "--json"})
		})
		if err == nil {
			t.Fatal("campaign check reached a coordinator from the test environment")
		}
		document := decode(t, "campaign check", output)
		if class, _ := document["class"].(string); class == "" {
			t.Fatalf("envelope %v has no class", document)
		}
	})

	t.Run("wait add with a bad flag is an unclassified error", func(t *testing.T) {
		var err error
		output := captureStdout(t, func() {
			err = run([]string{"wait", "add", "--task", "current", "--no-such-flag", "--json"})
		})
		if err == nil {
			t.Fatal("wait add accepted an unknown flag")
		}
		document := decode(t, "wait add", output)
		if document["class"] != "error" {
			t.Fatalf("envelope %v does not carry class \"error\"", document)
		}
	})

	t.Run("nothing reaches stdout without --json", func(t *testing.T) {
		for _, args := range [][]string{
			{"wait", "add", "--task", "current", "--no-such-flag"},
			{"campaign", "check", campaignFixture(t)},
		} {
			var err error
			output := captureStdout(t, func() {
				err = run(args)
			})
			if err == nil {
				t.Fatalf("%v succeeded in the test environment", args)
			}
			if output != "" {
				t.Fatalf("%v wrote to stdout without --json: %q", args, output)
			}
		}
	})
}
