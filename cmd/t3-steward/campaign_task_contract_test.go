package main

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

func TestCampaignFullHelpIncludesTaskContract(t *testing.T) {
	var out strings.Builder
	handled, err := admitCampaignHelp(&out, []string{"--help", "full"})
	if !handled || err != nil {
		t.Fatalf("help: handled=%v err=%v", handled, err)
	}
	if !strings.Contains(out.String(), strings.TrimSpace(backlog.TaskContract)) || !strings.Contains(out.String(), "Executor prompt template:") {
		t.Fatal("campaign --help full must include the shared contract and executor template")
	}
}
