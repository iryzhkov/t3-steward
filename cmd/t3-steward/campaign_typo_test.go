package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// A misspelt campaign command used to be refused with advice about recovery
// living under "backlog", which is no longer true and did not name the command
// the caller meant. The refusal now suggests the nearby command, and every
// refusal points at the help that lists them all.
func TestUnknownCampaignCommandSuggestsTheNearbyCommand(t *testing.T) {
	for _, tc := range []struct{ command, want string }{
		{"valdate", `did you mean "validate"?`},
		{"sumbit", `did you mean "submit"?`},
		{"frobnicate", `"t3-steward campaign help" lists every command`},
	} {
		var out bytes.Buffer
		err := campaignTestCLI(t, &out).run(context.Background(), []string{tc.command})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("campaign %s = %v, want %q", tc.command, err, tc.want)
		}
	}
}
