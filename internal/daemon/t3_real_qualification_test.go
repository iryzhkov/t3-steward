package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/compat"
	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/source/providerlog"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// TestRealT3Qualification is opt-in: it spends provider quota and starts a real
// server, never a stub. Install the pinned npm package in a separate prefix and
// set T3_QUALIFICATION_BINARY to its absolute CLI path. Every runtime/data file
// is disposable; tokens and raw provider metadata are never logged or retained.
func TestRealT3Qualification(t *testing.T) {
	binary := os.Getenv("T3_QUALIFICATION_BINARY")
	if binary == "" {
		t.Skip("set T3_QUALIFICATION_BINARY for isolated real-server qualification")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("qualification binary must be absolute")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	data := t.TempDir()
	workspace := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	server := exec.CommandContext(ctx, binary, "start", "--host", "127.0.0.1", "--port", fmt.Sprint(port), "--base-dir", data, "--no-browser")
	// Nil streams use /dev/null directly, so inherited provider descriptors
	// cannot keep exec copy goroutines alive after the server is stopped.
	server.WaitDelay = 5 * time.Second
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- server.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = server.Process.Kill()
			<-done
		}
	})
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	unauth := t3api.New(url, t3api.StaticToken(""), time.Second)
	await := func(label string, check func() bool) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) && ctx.Err() == nil {
			if check() {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("%s not observed before deadline", label)
	}
	await("server descriptor", func() bool { d, e := unauth.Descriptor(ctx); return e == nil && d.ServerVersion == "0.0.45" })
	if allowed, _ := compat.ControlAllowed("0.0.45", false); !allowed {
		t.Fatal("qualified server version must allow control without an override")
	}
	tokenCmd := exec.CommandContext(ctx, binary, "auth", "session", "issue", "--base-dir", data, "--ttl", "15m", "--label", "isolated-qualification", "--token-only")
	tokenBytes, err := tokenCmd.Output()
	if err != nil {
		t.Fatal("isolated token issuance failed")
	}
	client := t3api.New(url, t3api.StaticToken(strings.TrimSpace(string(tokenBytes))), 10*time.Second)
	session, err := client.SessionInfo(ctx)
	if err != nil || !session.Authenticated {
		t.Fatal("authenticated session check failed")
	}
	if _, err := client.ShellSnapshot(ctx); err != nil {
		t.Fatal("authenticated shell check failed")
	}
	t.Log("PASS authenticated descriptor/session/shell on isolated T3 0.0.45")
	steward := os.Getenv("T3_QUALIFICATION_STEWARD_BINARY")
	if steward == "" || !filepath.IsAbs(steward) {
		t.Fatal("set absolute T3_QUALIFICATION_STEWARD_BINARY to the checkout build")
	}
	configPath := filepath.Join(t.TempDir(), "config.json")
	checkConfig := map[string]any{
		"t3":         map[string]any{"url": url, "data_dir": data, "t3_binary": binary, "token_command": []string{binary, "auth", "session", "issue", "--base-dir", data, "--ttl", "15m", "--label", "isolated-check", "--token-only"}},
		"state_path": filepath.Join(t.TempDir(), "check.sqlite"),
	}
	checkStore, err := sqlite.OpenMigrated(checkConfig["state_path"].(string))
	if err != nil {
		t.Fatal(err)
	}
	checkStore.Close()
	configBytes, _ := json.Marshal(checkConfig)
	if err := os.WriteFile(configPath, configBytes, 0600); err != nil {
		t.Fatal(err)
	}
	check := exec.CommandContext(ctx, steward, "check", "--config", configPath)
	check.Dir = workspace
	check.Env = append(os.Environ(), "XDG_CONFIG_HOME="+t.TempDir(), "XDG_STATE_HOME="+t.TempDir())
	check.Stdout = io.Discard
	check.Stderr = io.Discard
	if err := check.Run(); err != nil {
		t.Fatalf("isolated t3-steward check failed: %v", err)
	}
	t.Log("PASS t3-steward check and compatibility gate without unsupported-version override")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	control := t3control.New(client, logger, false)
	project, err := control.EnsureProject(ctx, t3control.ManagedProject{Key: "isolated-qualification", Title: "Disposable qualification", WorkspaceRoot: workspace})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("PASS managed project creation and observed metadata")
	provider := os.Getenv("T3_QUALIFICATION_PROVIDER")
	if provider == "" {
		provider = "codex"
	}
	if provider != "codex" && provider != "claudeAgent" {
		t.Fatal("qualification provider must be codex or claudeAgent")
	}
	model := os.Getenv("T3_QUALIFICATION_MODEL")
	if model == "" {
		model = "gpt-6.1-sol"
		if provider == "claudeAgent" {
			model = "claude-sonnet-4-6"
		}
	}
	control.SendThreadEnvironment = true
	threadID, err := control.CreateAndStartThread(ctx, t3control.NewThreadInput{
		ProjectID: project, Title: "Disposable quota qualification",
		ModelSelection: map[string]any{"instanceId": provider, "model": model},
		Environment:    map[string]string{"T3_QUALIFICATION_SENTINEL": "isolated-sentinel"},
		Prompt:         "This is an isolated compatibility test. Do not access any other project or service. Use the shell once to write PRESENT to environment-observation.txt if T3_QUALIFICATION_SENTINEL equals isolated-sentinel, otherwise ABSENT. Then execute sleep 180 in the shell and remain working until interrupted. If additional warning messages arrive, acknowledge them briefly and continue sleeping. Do not finish early.",
	})
	if err != nil {
		t.Fatal(err)
	}
	get := func() *domain.Thread {
		th, e := control.GetThread(ctx, threadID)
		if e != nil {
			return nil
		}
		return th
	}
	await("real provider running turn", func() bool { th := get(); return th != nil && th.Running && th.TurnID != "" })
	t.Logf("PASS real %s running turn", provider)
	await("environment observation", func() bool {
		b, e := os.ReadFile(filepath.Join(workspace, "environment-observation.txt"))
		return e == nil && (strings.TrimSpace(string(b)) == "PRESENT" || strings.TrimSpace(string(b)) == "ABSENT")
	})
	envResult, _ := os.ReadFile(filepath.Join(workspace, "environment-observation.txt"))
	t.Logf("thread.create environment observed: %s; production setting remains false", strings.TrimSpace(string(envResult)))
	// Keep only the canonical quota envelope and normalized limits, never native
	// account metadata. A live event is parsed in memory and its shape is checked.
	await("actual provider quota event", func() bool {
		files, _ := filepath.Glob(filepath.Join(data, "userdata/logs/provider/events.*.log"))
		for _, file := range files {
			b, e := os.ReadFile(file)
			if e != nil {
				continue
			}
			for _, line := range strings.Split(string(b), "\n") {
				if !strings.Contains(line, "] CANON: ") || !strings.Contains(line, providerlog.EventType) {
					continue
				}
				snaps, e := providerlog.ParseLine(line)
				if e == nil && len(snaps) > 0 && snaps[0].Key.ProviderInstanceID == provider {
					return true
				}
			}
		}
		return false
	})
	t.Logf("PASS actual new-provider %s event parsing", provider)
	cfg := config.Default()
	cfg.Policy.DryRun = false
	cfg.Policy.StopVerifyTimeout = config.Duration(15 * time.Second)
	cfg.Notifications.Desktop = false
	cfg.Resume.Enabled = true
	cfg.Resume.CoordinatorThreadsOnly = false
	cfg.Resume.ResetSettleDelay = 0
	cfg.Resume.IntervalBetweenThreads = 0
	cfg.Resume.ProbeAfterReset = 0
	cfg.Messages.Warn = "QUALIFICATION_WARN: acknowledge and continue sleeping."
	cfg.Messages.Drain = "QUALIFICATION_DRAIN: acknowledge and continue sleeping until interrupted."
	cfg.Resume.Prompt = "Reply QUALIFICATION_RESUMED and finish this disposable test turn."
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "steward.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	d := New(cfg, logger, store, control, nil)
	clock := time.Now().UTC()
	d.SetClock(func() time.Time { return clock })
	d.Poll(ctx)
	reset := clock.Add(time.Hour)
	window := "primary"
	duration := 10080
	if provider == "claudeAgent" {
		window = "five_hour"
		duration = 300
	}
	inject := func(used float64, resetAt time.Time) {
		t.Helper()
		body := map[string]any{"type": providerlog.EventType, "eventId": fmt.Sprintf("isolated-%d", clock.UnixNano()), "provider": provider, "providerInstanceId": provider, "threadId": threadID, "createdAt": clock.Format(time.RFC3339Nano), "payload": map[string]any{"limits": map[string]any{"windows": []any{map[string]any{"id": window, "kind": "weekly", "label": "Qualification", "usedPercent": used, "windowDurationMins": duration, "resetsAt": resetAt.Format(time.RFC3339Nano)}}}}}
		b, _ := json.Marshal(body)
		snaps, e := providerlog.ParseJSON(b, time.Time{})
		if e != nil || len(snaps) != 1 {
			t.Fatal("injected normalized quota did not parse")
		}
		d.HandleSnapshot(ctx, snaps[0])
	}
	messageSeen := func(text string) bool {
		detail, e := client.ThreadDetail(ctx, threadID, 20)
		if e != nil {
			return false
		}
		for _, m := range detail.Messages {
			if m.Role == "user" && strings.Contains(m.Text, text) {
				return true
			}
		}
		return false
	}
	inject(86, reset)
	await("86 percent warning user message", func() bool { return messageSeen("QUALIFICATION_WARN") })
	t.Log("PASS 86 percent warning projected as user message")
	clock = clock.Add(10 * time.Minute)
	d.Poll(ctx)
	inject(91, reset)
	await("91 percent drain user message", func() bool { return messageSeen("QUALIFICATION_DRAIN") })
	t.Log("PASS 91 percent drain projected as user message")
	clock = clock.Add(10 * time.Minute)
	d.Poll(ctx)
	inject(96, reset)
	await("96 percent running-turn interruption", func() bool { th := get(); return th != nil && th.TurnState == "interrupted" })
	stopped := get()
	t.Log("PASS 96 percent latestTurn.state interrupted")
	clock = reset.Add(time.Minute)
	inject(4, reset.Add(168*time.Hour))
	clock = clock.Add(time.Minute)
	inject(4, reset.Add(168*time.Hour))
	d.Poll(ctx)
	await("reset resume new turn", func() bool { th := get(); return th != nil && th.TurnID != "" && th.TurnID != stopped.TurnID })
	await("resume completed", func() bool { th := get(); return th != nil && th.TurnState == "completed" })
	t.Log("PASS fresh reset below 50 percent resumes a new completed turn")
	th := get()
	if err := control.StopThread(ctx, *th, t3control.StopSession); err != nil {
		t.Fatal(err)
	}
	await("session stop projected", func() bool { th := get(); return th != nil && th.SessionStatus == "stopped" })
	t.Log("PASS orchestration session stop observed stopped")
}
