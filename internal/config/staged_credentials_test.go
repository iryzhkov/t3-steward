//go:build linux

package config

import (
	"path/filepath"
	"testing"
)

func TestStagedConfigNotificationSemantics(t *testing.T) {
	stagedTestHome(t)
	cfg := Default()
	cfg.Notifications.Discord = &DiscordNotifications{WebhookURLFile: filepath.Join(t.TempDir(), "missing-credential")}
	path, _ := stagedConfig(t, cfg)
	if _, err := ValidateStagedFile(path); err != nil {
		t.Fatal("non-coordinator notification settings need no credential read:", err)
	}
	cfg.Notifications.Discord.Events = []string{"SECRET-invalid-event"}
	path, _ = stagedConfig(t, cfg)
	stagedRefused(t, path)
	cfg.Notifications.Discord = nil
	cfg.Notifications.Command = &CommandNotifications{Argv: []string{"SECRET-relative-program"}}
	path, _ = stagedConfig(t, cfg)
	stagedRefused(t, path)
	cfg.Notifications.Command = nil
	cfg.Notifications.WorkerDownAfter = 0
	path, _ = stagedConfig(t, cfg)
	stagedRefused(t, path)
}
