package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestThreadWhoamiMatchesSubmitIdentity(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, key := range providerSessionKeys {
		t.Setenv(key, "")
	}
	for name, value := range injectedIdentity() {
		t.Setenv(name, value)
	}
	cfg := config.Config{}
	cli := campaignCLI{resolveThread: func(explicit string) (string, error) { return resolveThread(cfg, explicit) }}
	want, err := cli.campaignNotifyThread("campaign submit", "current")
	if err != nil {
		t.Fatal(err)
	}
	for _, asJSON := range []bool{false, true} {
		var out bytes.Buffer
		if err := threadWhoami(cfg, asJSON, &out); err != nil {
			t.Fatal(err)
		}
		expected := want + "\n"
		if asJSON {
			expected = "{\"threadId\":\"" + want + "\"}\n"
		}
		if out.String() != expected {
			t.Fatalf("json=%v output=%q want=%q", asJSON, out.String(), expected)
		}
	}
}

func TestThreadWhoamiMatchesSubmitUnresolvedError(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, key := range providerSessionKeys {
		t.Setenv(key, "")
	}
	for _, key := range domain.TaskWaitEnvironmentNames() {
		t.Setenv(key, "")
	}
	cfg := config.Config{}
	cli := campaignCLI{resolveThread: func(explicit string) (string, error) { return resolveThread(cfg, explicit) }}
	_, want := cli.campaignNotifyThread("campaign submit", "current")
	if want == nil {
		t.Fatal("unresolvable environment resolved")
	}
	for _, asJSON := range []bool{false, true} {
		var out bytes.Buffer
		err := threadWhoami(cfg, asJSON, &out)
		if err == nil || err.Error() != want.Error() {
			t.Fatalf("error=%v want=%v", err, want)
		}
		if out.Len() != 0 {
			t.Fatalf("unexpected output %q", out.String())
		}
	}
}

func TestThreadWhoamiDispatchAndHelp(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, key := range providerSessionKeys {
		t.Setenv(key, "")
	}
	for _, key := range domain.TaskWaitEnvironmentNames() {
		t.Setenv(key, "")
	}
	flags := throwawayBacklogFlags(t)
	var out bytes.Buffer
	// --json reaches the resolver through the command parser.
	err := cmdThreadWhoami(flags, []string{"--json"}, &out)
	if err == nil || !strings.Contains(err.Error(), "no caller session") {
		t.Fatalf("error=%v", err)
	}
	if err := cmdThread(flags, []string{"whoami"}); err == nil || !strings.Contains(err.Error(), "no caller session") {
		t.Fatalf("dispatch error=%v", err)
	}
	for _, args := range [][]string{{"extra"}, {"--unknown"}, {"--json", "--json"}} {
		if err := cmdThreadWhoami(flags, args, &out); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if !strings.Contains(threadUsage, "whoami [--json]") {
		t.Fatal("family help omits whoami")
	}
	answered, err := admitFamilyHelp(&out, []string{"thread"}, []string{"whoami", "--help", "full"})
	if err != nil || !answered || !strings.Contains(out.String(), "threadId") {
		t.Fatalf("help answered=%v error=%v output=%q", answered, err, out.String())
	}
}
