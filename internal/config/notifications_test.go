package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testWebhook = "https://discord.com/api/webhooks/123456/s3cr3t-token-abcdefghijklmnop"

func writeNotificationConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeWebhookFile(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "discord-webhook")
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

// A private webhook file loads; the URL is readable on demand and is nowhere
// in the loaded configuration itself.
func TestDiscordNotificationsLoadFromAPrivateFile(t *testing.T) {
	webhook := writeWebhookFile(t, testWebhook+"\n", 0o600)
	cfg, err := Load(writeNotificationConfig(t, "notifications:\n  discord:\n    webhook_url_file: "+webhook+
		"\n    events: [run-failed, needs-input]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Notifications.Discord == nil || len(cfg.Notifications.Discord.Events) != 2 {
		t.Fatalf("discord = %+v", cfg.Notifications.Discord)
	}
	got, err := cfg.Notifications.Discord.WebhookURL()
	if err != nil || got != testWebhook {
		t.Fatalf("WebhookURL() = %q, %v", got, err)
	}
	if !cfg.Notifications.Desktop {
		t.Fatal("declaring discord changed the desktop default")
	}
}

// Every refusal names the file and the rule, and none of them quotes the URL.
func TestDiscordNotificationsRefuseUnsafeOrInvalidConfiguration(t *testing.T) {
	for name, tc := range map[string]struct {
		config func(t *testing.T) string
		want   string
	}{
		"readable by others": {
			config: func(t *testing.T) string { return "webhook_url_file: " + writeWebhookFile(t, testWebhook, 0o644) },
			want:   "has mode 0644, want 0600",
		},
		"missing file": {
			config: func(t *testing.T) string { return "webhook_url_file: " + filepath.Join(t.TempDir(), "absent") },
			want:   "must be a regular 0600 file",
		},
		"no file named": {
			config: func(*testing.T) string { return "events: [run-failed]" },
			want:   "webhook_url_file is required",
		},
		"relative path": {
			config: func(*testing.T) string { return "webhook_url_file: discord-webhook" },
			want:   "must be an absolute path",
		},
		"not https": {
			config: func(t *testing.T) string {
				return "webhook_url_file: " + writeWebhookFile(t, strings.Replace(testWebhook, "https", "http", 1), 0o600)
			},
			want: "does not hold an https URL",
		},
		"two lines": {
			config: func(t *testing.T) string {
				return "webhook_url_file: " + writeWebhookFile(t, testWebhook+"\n"+testWebhook, 0o600)
			},
			want: "must contain exactly one URL",
		},
		"unknown event": {
			config: func(t *testing.T) string {
				return "webhook_url_file: " + writeWebhookFile(t, testWebhook, 0o600) + "\n    events: [run-exploded]"
			},
			want: `unknown event "run-exploded"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeNotificationConfig(t, "notifications:\n  discord:\n    "+tc.config(t)+"\n"))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "s3cr3t-token") || strings.Contains(err.Error(), "/api/webhooks/") {
				t.Fatalf("the refusal quotes the webhook: %v", err)
			}
		})
	}
}

func TestCommandNotificationsNeedAnAbsoluteProgram(t *testing.T) {
	for body, want := range map[string]string{
		"argv: []":                           "argv needs at least the program",
		"argv: [notify-owner]":               "must be an absolute path",
		"argv: [/bin/true]\n    events: [x]": `unknown event "x"`,
	} {
		_, err := Load(writeNotificationConfig(t, "notifications:\n  command:\n    "+body+"\n"))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err = %v, want %q", body, err, want)
		}
	}
	cfg, err := Load(writeNotificationConfig(t, "notifications:\n  command:\n    argv: [/bin/true, --quiet]\n"))
	if err != nil || cfg.Notifications.Command == nil || len(cfg.Notifications.Command.Argv) != 2 {
		t.Fatalf("command = %+v, %v", cfg.Notifications.Command, err)
	}
}

// The shipped sample declares no owner channel, so a fresh install sends
// nothing anywhere until its operator says where.
func TestSampleDeclaresNoOwnerChannel(t *testing.T) {
	cfg, err := Load(writeNotificationConfig(t, Sample()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Notifications.Discord != nil || cfg.Notifications.Command != nil {
		t.Fatalf("sample enables an owner channel: %+v", cfg.Notifications)
	}
}
