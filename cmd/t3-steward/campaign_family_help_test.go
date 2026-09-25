package main

import (
	"strings"
	"testing"
)

// The campaign family page and the campaign submit page, tied together.
//
// Stage 3 made submit notify the calling thread by default and refuse a
// submission no thread resolves for, which makes --no-notify the flag an
// unattended caller needs. The verb's own page says so; the family page kept
// the synopsis it had, so "t3-steward campaign --help" offered --notify-thread
// as the way to be woken and never mentioned the default or the opt-out. That
// is the shape M-3 had on the wait family page, so it is tied the same way
// rather than left to agree by hand.

// campaignFamilyBlock is one command's entry on the family page: the synopsis
// line that starts with the command word and every continuation line under it.
func campaignFamilyBlock(t *testing.T, command string) string {
	t.Helper()
	lines := strings.Split(campaignCommandUsage, "\n")
	start := -1
	for i, line := range lines {
		switch {
		case start < 0 && strings.HasPrefix(line, "  "+command+" "):
			start = i
		case start >= 0 && !strings.HasPrefix(line, "   "):
			return strings.Join(lines[start:i], "\n")
		}
	}
	if start < 0 {
		t.Fatalf("the campaign family page has no %q synopsis", command)
	}
	return strings.Join(lines[start:], "\n")
}

func TestTheCampaignFamilyPageOffersTheFlagsItsVerbPagesDo(t *testing.T) {
	// The verbs whose whole synopsis the family page spells out. supervision
	// is deliberately not one: its page carries the flags and the family page
	// says so in the line that names it.
	for _, command := range []string{"validate", "plan", "check", "submit", "rerun"} {
		t.Run(command, func(t *testing.T) {
			block := campaignFamilyBlock(t, command)
			page, found := helpPageFor("campaign " + command)
			if !found {
				t.Fatalf("there is no \"campaign %s\" page for the family page to agree with", command)
			}
			for _, usage := range page.Usage {
				for _, flag := range helpFlagPattern.FindAllString(usage, -1) {
					if !strings.Contains(block, flag) {
						t.Errorf("the campaign family page's %s entry does not offer %s:\n%s", command, flag, block)
					}
				}
			}
		})
	}
}

// supervision is a help topic, answered by a special case rather than from the
// topic set, and the family page and the unknown-topic refusal both list it.
func TestTheCampaignHelpTopicsListSupervision(t *testing.T) {
	if !strings.Contains(campaignCommandUsage, "routes,\nsupervision.") {
		t.Error("the family page's topic line does not list supervision")
	}
	_, err := admitCampaignHelp(&strings.Builder{}, []string{"help", "no-such-topic"})
	if err == nil || !strings.Contains(err.Error(), "supervision") {
		t.Errorf("the unknown-topic refusal does not list supervision: %v", err)
	}
}

// The default is the half a synopsis cannot carry, and it is the half that
// decides whether an unattended caller has to pass anything at all.
func TestTheCampaignFamilyPageStatesTheNotificationDefault(t *testing.T) {
	for _, want := range []string{"notified by default", "--no-notify"} {
		if !strings.Contains(campaignCommandUsage, want) {
			t.Errorf("the campaign family page does not say %q", want)
		}
	}
	if strings.Contains(campaignCommandUsage, "or pass --notify-thread to be woken") {
		t.Error("the campaign family page still offers notification as the opt-in it stopped being")
	}
}
