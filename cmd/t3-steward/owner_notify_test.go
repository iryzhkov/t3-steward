package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/ownernotify"
)

// A webhook file that became unsafe after the configuration was loaded
// disables that channel with one error line and leaves the coordinator, and
// every other channel, running. The line names the file, not the URL.
func TestOwnerNotificationSinksSkipAChannelThatCannotStart(t *testing.T) {
	dir := t.TempDir()
	webhook := filepath.Join(dir, "discord-webhook")
	if err := os.WriteFile(webhook, []byte("https://discord.com/api/webhooks/1/s3cr3t-token-abcdefghijklmnop"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(webhook, 0o644); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	sinks := ownerNotificationSinks(config.Notifications{
		Discord: &config.DiscordNotifications{WebhookURLFile: webhook},
		Command: &config.CommandNotifications{Argv: []string{"/bin/true"}},
	}, slog.New(slog.NewTextHandler(&logs, nil)))
	if len(sinks) != 1 || sinks[0].Name() != ownernotify.CommandSinkName {
		t.Fatalf("sinks = %v", sinks)
	}
	if !strings.Contains(logs.String(), "discord owner notifications are disabled") || strings.Contains(logs.String(), "s3cr3t") {
		t.Fatalf("log = %s", logs.String())
	}
	// No channel at all starts nothing and stops cleanly.
	startOwnerNotifier(t.Context(), config.Notifications{}, nil, slog.New(slog.NewTextHandler(&logs, nil)))()
}
