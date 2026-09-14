package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// D1. The readiness check is what campaign submit runs by default, so a remote
// client that cannot perform it can only submit with --allow-unverified, the
// flag the help tells agents never to use. Every declared query kind is a read
// view, so this asserts the whole set rather than the one kind that was missing:
// the next kind added to the type is covered without anyone remembering to come
// back here.
func TestRemoteAdminMayPerformEveryDeclaredQueryKind(t *testing.T) {
	remote := backlogadmin.Principal{ID: "remote:admin:omarchy-pc", Roles: []string{backlogadmin.RemoteAdminRole}}
	authorizer := localAdminAuthorizer{}
	for _, kind := range backlogadmin.QueryKinds() {
		if err := authorizer.Authorize(context.Background(), remote, backlogadmin.Action{Kind: kind}); err != nil {
			t.Fatalf("remote-admin refused the %q read: %v", kind, err)
		}
	}
	if !backlogadmin.IsQueryKind(backlogadmin.QueryViability) {
		t.Fatal("the viability query is not a declared query kind")
	}
	if backlogadmin.IsQueryKind("worker-enrollment") {
		t.Fatal("worker enrollment is not a read view and must not be listed as one")
	}
}

// D1, second half. No refusal may tell the caller to go and run the command on
// the coordinator instead: an agent that follows that opens a shell on the
// coordinator's owner account, which is what the remote carrier exists to stop.
func TestRefusalsNeverSendTheCallerToTheCoordinatorHost(t *testing.T) {
	remote := backlogadmin.Principal{ID: "remote:admin:omarchy-pc", Roles: []string{backlogadmin.RemoteAdminRole}}
	authorizer := localAdminAuthorizer{}
	refusals := []string{}
	for _, action := range []backlogadmin.Action{
		{Kind: backlogadmin.QueryKind("worker-enrollment")},
		{Kind: backlogadmin.QueryKind("invented")},
		{Kind: backlogadmin.QueryKind("command"), CommandKind: "rotate-coordinator-epoch"},
	} {
		err := authorizer.Authorize(context.Background(), remote, action)
		if err == nil {
			t.Fatalf("remote-admin was authorized for %+v", action)
		}
		refusals = append(refusals, err.Error())
	}
	for _, refusal := range refusals {
		for _, forbidden := range []string{
			"run it on the coordinator",
			"run worker enroll on the coordinator",
			"ssh ",
		} {
			if strings.Contains(refusal, forbidden) {
				t.Fatalf("refusal %q sends the caller to the coordinator host", refusal)
			}
		}
	}
}

// The same rule applied to the whole tree: no message an operator or an agent
// can see may instruct anyone to run a steward command on the coordinator host,
// because the only way to follow that instruction from a client is a remote
// shell.
func TestNoSourceStringSendsACallerToTheCoordinatorHost(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	var offenders []string
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.Contains(line, "on the coordinator host") &&
				(strings.Contains(line, "run it") || strings.Contains(line, "run worker")) {
				offenders = append(offenders, path+": "+strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) != 0 {
		t.Fatalf("messages instruct the caller to run a command on the coordinator host: %v", offenders)
	}
}

// D2. One coordinator client declares one ssh destination and one identity, so
// a forced command that pins the operation confines that client to a single
// operation. campaign submit needs two, the readiness query and the submission,
// so the endpoint must accept an invocation with no operation word.
func TestCoordinatorExchangeAcceptsNoOperationWord(t *testing.T) {
	// Reaching the config refusal proves the operation word was accepted; an
	// unknown operation is refused before the configuration is ever read.
	err := cmdCoordinatorExchange(globalFlags{}, "")
	if err == nil || !strings.Contains(err.Error(), "explicit operator-controlled --config") {
		t.Fatalf("no operation word: %v", err)
	}
	if err := cmdCoordinatorExchange(globalFlags{}, "nonsense"); err == nil ||
		!strings.Contains(err.Error(), "operation must be one of") {
		t.Fatalf("unknown operation word: %v", err)
	}
	// The dispatcher accepts zero or one word and refuses two.
	if err := run([]string{"coordinator-exchange", "query", "extra"}); err == nil ||
		!strings.Contains(err.Error(), "at most one fixed operation") {
		t.Fatalf("two operation words: %v", err)
	}
	if err := run([]string{"coordinator-exchange"}); err == nil ||
		!strings.Contains(err.Error(), "explicit operator-controlled --config") {
		t.Fatalf("bare invocation: %v", err)
	}
}

// D3. The forced-command lines the runbook documents are executed verbatim by
// sshd, so one that cannot parse is a runbook that cannot be followed. Global
// flags are parsed after the command word, so --config must follow it.
func TestDocumentedForcedCommandLinesParse(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "docs", "backlog-v2-operations.md"))
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, "coordinator-exchange") && !strings.Contains(line, "worker-exchange") {
			continue
		}
		if !strings.Contains(line, "command=") {
			continue
		}
		found++
		// A forced command reaching the binary with --config ahead of the
		// command word exits with `unknown command "--config"`, because the
		// command word is read first and the flags after it.
		command := line[strings.Index(line, "command=\"")+len("command=\""):]
		command = command[:strings.Index(command, "\"")]
		fields := strings.Fields(command)
		if len(fields) < 2 {
			t.Fatalf("forced command %q has no command word", command)
		}
		if strings.HasPrefix(fields[1], "-") {
			t.Fatalf("forced command %q puts a flag before the command word; it exits with unknown command %q",
				command, fields[1])
		}
		if fields[1] != "coordinator-exchange" && fields[1] != "worker-exchange" {
			t.Fatalf("forced command %q does not invoke a restricted endpoint", command)
		}
	}
	if found == 0 {
		t.Fatal("the operations guide documents no forced-command line")
	}
}
