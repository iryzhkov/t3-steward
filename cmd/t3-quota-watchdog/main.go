// Command t3-quota-watchdog monitors provider quota windows reported by T3
// Code's providers and stops T3 sessions before the quota is exhausted.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/compat"
	"github.com/iryzhkov/t3-quota-watchdog/internal/config"
	t3control "github.com/iryzhkov/t3-quota-watchdog/internal/control/t3"
	"github.com/iryzhkov/t3-quota-watchdog/internal/daemon"
	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
	"github.com/iryzhkov/t3-quota-watchdog/internal/platform"

	"github.com/iryzhkov/t3-quota-watchdog/internal/source/providerlog"
	"github.com/iryzhkov/t3-quota-watchdog/internal/store/sqlite"
	"github.com/iryzhkov/t3-quota-watchdog/internal/t3api"
)

// Set by GoReleaser through -ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const usage = `t3-quota-watchdog - stop T3 Code agent sessions before the provider quota runs out.

Usage:
  t3-quota-watchdog <command> [flags]

Commands:
  init               Write a commented configuration file and create the state directory.
  check              Verify the T3 connection, token, version and provider logs.
  run                Run the watchdog in the foreground.
  status             Show bucket states, resume intents and recent actions.
  replay <file>      Feed recorded quota events through the policy engine (no T3 needed).
  report             Consumption by peak/off-peak hours, hour of day, model and thread.
  forecast           Interactive-demand map by weekday and hour, and current backlog headroom.
  export             Print this host's readings and token samples as JSON for another host's report.
  install-service    Install a per-user background service (Linux systemd).
  uninstall-service  Remove the background service.
  version            Print the version.

Global flags:
  --config PATH      Configuration file (default: $XDG_CONFIG_HOME/t3-quota-watchdog/config.yaml)
  --dry-run          Force dry-run mode regardless of the configuration.
  --log-level LEVEL  debug, info, warn or error.

Environment variables prefixed with T3_QUOTA_WATCHDOG_ override the configuration
file (for example T3_QUOTA_WATCHDOG_T3_URL, T3_QUOTA_WATCHDOG_DRY_RUN).
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type globalFlags struct {
	configPath string
	dryRun     bool
	noDryRun   bool
	logLevel   string
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	cmd := args[0]
	rest := args[1:]
	switch cmd {
	case "-h", "--help", "help":
		fmt.Print(usage)
		return nil
	case "version", "--version", "-v":
		fmt.Printf("t3-quota-watchdog %s (commit %s, built %s, %s/%s, tested with T3 %s..%s)\n",
			version, commit, date, runtime.GOOS, runtime.GOARCH, compat.MinServerVersion, compat.MaxServerVersion)
		return nil
	}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	var g globalFlags
	paths, err := config.DefaultPaths()
	if err != nil {
		return err
	}
	fs.StringVar(&g.configPath, "config", paths.ConfigFile, "configuration file")
	fs.BoolVar(&g.dryRun, "dry-run", false, "force dry-run mode")
	fs.BoolVar(&g.noDryRun, "no-dry-run", false, "disable dry-run mode for this run (overrides the configuration)")
	fs.StringVar(&g.logLevel, "log-level", "", "log level")
	var (
		force     bool
		enable    bool
		limit     int
		t3URL     string
		dataDir   string
		asJSON    bool
		speed     float64
		showAll   bool
		fromState bool
		rf        reportFlags
	)
	switch cmd {
	case "install-service":
		fs.BoolVar(&force, "force", false, "overwrite an existing service definition")
		fs.BoolVar(&enable, "enable", false, "enable and start the service now")
	case "init":
		fs.BoolVar(&force, "force", false, "overwrite an existing configuration file")
		fs.StringVar(&t3URL, "t3-url", "", "T3 server URL to write into the configuration")
		fs.StringVar(&dataDir, "t3-data-dir", "", "T3 data directory to write into the configuration")
	case "status":
		fs.IntVar(&limit, "limit", 20, "number of recent actions to show")
		fs.BoolVar(&asJSON, "json", false, "print JSON")
		fs.BoolVar(&showAll, "all", false, "include resumed and cancelled intents")
	case "export":
		fs.IntVar(&rf.days, "days", 14, "period to export, in days")
		fs.BoolVar(&rf.fromLogs, "from-logs", false, "also scan the provider logs")
	case "forecast":
		fs.IntVar(&rf.days, "days", 56, "history to learn from, in days")
		fs.StringVar(&rf.bucket, "bucket", "", "only buckets whose key contains this text")
		fs.BoolVar(&rf.fromLogs, "from-logs", false, "also scan the provider logs")
		fs.BoolVar(&rf.doImport, "import", false, "store scanned observations in the state database")
		fs.StringVar(&rf.remotes, "remotes", "", "comma-separated SSH hosts to merge (default: report.remotes)")
		fs.BoolVar(&rf.local, "local", false, "ignore configured remotes")
		fs.BoolVar(&rf.asJSON, "json", false, "print JSON")
	case "report":
		fs.IntVar(&rf.days, "days", 14, "period to report, in days")
		fs.StringVar(&rf.bucket, "bucket", "", "only buckets whose key contains this text")
		fs.StringVar(&rf.peak, "peak", "", "peak schedule, local time (default: report.peak in the configuration)")
		fs.StringVar(&rf.remotes, "remotes", "", "comma-separated SSH hosts to merge (default: report.remotes)")
		fs.BoolVar(&rf.local, "local", false, "ignore configured remotes")
		fs.BoolVar(&rf.fromLogs, "from-logs", false, "also scan the provider logs (rotated files included)")
		fs.BoolVar(&rf.doImport, "import", false, "store scanned observations in the state database")
		fs.BoolVar(&rf.asJSON, "json", false, "print JSON")
	case "replay":
		fs.Float64Var(&speed, "speed", 0, "sleep between events scaled by this factor (0 = no sleep)")
		fs.BoolVar(&fromState, "with-state", false, "use the real state database instead of a temporary one")
		fs.BoolVar(&enable, "resume", false, "enable automatic resume during the replay")
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}

	switch cmd {
	case "init":
		return cmdInit(g, paths, force, t3URL, dataDir)
	case "check":
		return cmdCheck(g)
	case "run":
		return cmdRun(g)
	case "status":
		return cmdStatus(g, limit, asJSON, showAll)
	case "replay":
		if fs.NArg() != 1 {
			return errors.New("replay needs exactly one file argument")
		}
		return cmdReplay(g, fs.Arg(0), speed, fromState, enable)
	case "export":
		return cmdExport(g, rf.days, rf.fromLogs)
	case "forecast":
		return cmdForecast(g, rf)
	case "report":
		return cmdReport(g, rf)
	case "install-service":
		return cmdInstallService(g, force, enable)
	case "uninstall-service":
		return cmdUninstallService()
	default:
		return fmt.Errorf("unknown command %q (try --help)", cmd)
	}
}

func loadConfig(g globalFlags) (config.Config, error) {
	cfg, err := config.Load(g.configPath)
	if err != nil {
		return cfg, err
	}
	if g.dryRun {
		cfg.Policy.DryRun = true
	}
	if g.noDryRun {
		cfg.Policy.DryRun = false
	}
	if g.logLevel != "" {
		cfg.LogLevel = g.logLevel
	}
	return cfg, cfg.Validate()
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

// connect builds the API client from the configuration.
func connect(cfg config.Config, logger *slog.Logger) (*t3api.Client, string, error) {
	dataDir, err := cfg.ResolveDataDir()
	if err != nil {
		return nil, "", err
	}
	baseURL, err := t3api.DiscoverURL(cfg.T3.URL, dataDir)
	if err != nil {
		return nil, dataDir, err
	}
	var tokens t3api.TokenSource
	switch {
	case cfg.T3.Token != "":
		tokens = t3api.StaticToken(cfg.T3.Token)
	case len(cfg.T3.TokenCommand) > 0:
		tokens = &t3api.CommandToken{Argv: cfg.T3.TokenCommand, TTL: cfg.T3.TokenTTL.D()}
	default:
		bin, err := t3api.FindT3Binary(cfg.T3.T3Binary)
		if err != nil {
			return nil, dataDir, fmt.Errorf("%w (or set t3.token / t3.token_command)", err)
		}
		argv := t3api.DefaultTokenArgv(bin, cfg.T3.TokenTTL.D())
		if cfg.T3.DataDir != "" {
			argv = append(argv, "--base-dir", dataDir)
		}
		tokens = &t3api.CommandToken{Argv: argv, TTL: cfg.T3.TokenTTL.D()}
		logger.Debug("token source", "command", strings.Join(argv, " "))
	}
	return t3api.New(baseURL, tokens, cfg.T3.RequestTimeout.D()), dataDir, nil
}

func cmdInit(g globalFlags, paths config.Paths, force bool, t3URL, dataDir string) error {
	if err := os.MkdirAll(paths.StateDir, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	configPath := g.configPath
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	if _, err := os.Stat(configPath); err == nil && !force {
		fmt.Printf("Configuration already exists at %s (use --force to overwrite).\n", configPath)
	} else {
		content := config.Sample()
		if t3URL != "" {
			content = strings.Replace(content, "  url: \"\"", fmt.Sprintf("  url: %q", t3URL), 1)
		}
		if dataDir != "" {
			content = strings.Replace(content, "  data_dir: \"\"", fmt.Sprintf("  data_dir: %q", dataDir), 1)
		}
		if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
			return fmt.Errorf("write configuration: %w", err)
		}
		fmt.Printf("Wrote %s (dry-run on, automatic resume off).\n", configPath)
	}
	fmt.Printf("State directory: %s\n", paths.StateDir)

	// Discovery report.
	cfg := config.Default()
	if t3URL != "" {
		cfg.T3.URL = t3URL
	}
	if dataDir != "" {
		cfg.T3.DataDir = dataDir
	}
	resolvedDataDir, err := cfg.ResolveDataDir()
	if err == nil {
		fmt.Printf("T3 data directory: %s\n", resolvedDataDir)
		if url, err := t3api.DiscoverURL(cfg.T3.URL, resolvedDataDir); err == nil {
			fmt.Printf("T3 server URL: %s\n", url)
		} else {
			fmt.Printf("T3 server URL: not discovered (%v). Set t3.url in the configuration if the server runs elsewhere.\n", err)
		}
		if _, err := os.Stat(t3api.ProviderLogDir(resolvedDataDir)); err != nil {
			fmt.Printf("Provider log directory %s does not exist yet; it appears after the first provider session.\n", t3api.ProviderLogDir(resolvedDataDir))
		}
	}
	if bin, err := t3api.FindT3Binary(""); err == nil {
		fmt.Printf("t3 CLI: %s\n", bin)
	} else {
		fmt.Printf("t3 CLI: not found (%v)\n", err)
	}
	fmt.Printf("\nNext: validate the connection with\n  t3-quota-watchdog check --config %s\n", configPath)
	return nil
}

func cmdCheck(g globalFlags) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	logger := newLogger("warn")
	ok := true
	fail := func(format string, args ...any) {
		ok = false
		fmt.Printf("FAIL  "+format+"\n", args...)
	}
	pass := func(format string, args ...any) { fmt.Printf("ok    "+format+"\n", args...) }
	warn := func(format string, args ...any) { fmt.Printf("warn  "+format+"\n", args...) }

	if cfg.Path != "" {
		if _, err := os.Stat(cfg.Path); err == nil {
			pass("configuration %s", cfg.Path)
		} else {
			warn("configuration %s not found; defaults in use (run init)", cfg.Path)
		}
	}
	pass("policy: warn %.0f%% / drain %.0f%% / stop %.0f%%, grace %s, dry_run=%v, resume=%v",
		cfg.Policy.WarnPercent, cfg.Policy.DrainPercent, cfg.Policy.StopPercent, cfg.Policy.GracePeriod.D(), cfg.Policy.DryRun, cfg.Resume.Enabled)

	client, dataDir, err := connect(cfg, logger)
	if err != nil {
		fail("%v", err)
		return errors.New("check failed")
	}
	pass("T3 data directory %s", dataDir)
	logDir := t3api.ProviderLogDir(dataDir)
	if entries, err := os.ReadDir(logDir); err != nil {
		warn("provider log directory %s: %v (it appears after the first provider session)", logDir, err)
	} else {
		n := 0
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "events.") && strings.HasSuffix(e.Name(), ".log") {
				n++
			}
		}
		pass("provider logs: %d files in %s", n, logDir)
		if n > 0 {
			f, err := os.Open(logDir)
			if err == nil {
				f.Close()
			}
		}
	}
	pass("T3 server URL %s", client.BaseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	desc, err := client.Descriptor(ctx)
	if err != nil {
		fail("environment descriptor: %v", err)
		return errors.New("check failed")
	}
	st := compat.Check(desc.ServerVersion)
	allowed, reason := compat.ControlAllowed(desc.ServerVersion, cfg.T3.AllowUnsupportedVersion)
	switch {
	case st == compat.Supported:
		pass("T3 server %s (%s, %s/%s): %s", desc.ServerVersion, desc.Label, desc.Platform.OS, desc.Platform.Arch, st)
	case allowed:
		warn("%s", reason)
	default:
		warn("%s", reason)
	}
	session, err := client.SessionInfo(ctx)
	if err != nil {
		fail("authentication: %v", err)
		return errors.New("check failed")
	}
	scopes := strings.Join(session.Scopes, " ")
	if scopes == "" {
		scopes = strings.TrimSpace(string(session.Raw))
	}
	pass("authenticated; scopes: %s", scopes)
	if !strings.Contains(scopes, "orchestration:operate") {
		warn("token lacks orchestration:operate; the watchdog can monitor but not warn, stop or resume")
	}
	threads, err := t3control.New(client, logger, true).ListThreads(ctx)
	if err != nil {
		fail("shell snapshot: %v", err)
		return errors.New("check failed")
	}
	running := 0
	instances := map[string]int{}
	for _, t := range threads {
		if t.Running {
			running++
		}
		instances[t.ProviderInstanceID]++
	}
	pass("shell snapshot: %d threads, %d running, provider instances: %v", len(threads), running, instances)

	// Latest known quota state from the logs.
	statePath, err := cfg.ResolveStatePath()
	if err == nil {
		if store, err := sqlite.Open(statePath); err == nil {
			buckets, _ := store.ListBuckets(ctx)
			store.Close()
			if len(buckets) == 0 {
				warn("no quota buckets recorded yet in %s; they appear once a provider reports usage", statePath)
			} else {
				for _, b := range buckets {
					pass("bucket %s: %.0f%% phase=%s", b.Key, b.UsedPercent, b.Phase)
				}
			}
		} else {
			fail("state database %s: %v", statePath, err)
		}
	}
	if !ok {
		return errors.New("check failed")
	}
	fmt.Println("\nAll checks passed. Start with `t3-quota-watchdog run` (dry-run unless policy.dry_run is false).")
	return nil
}

func cmdRun(g globalFlags) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)
	logger.Info("t3-quota-watchdog starting", "version", version, "config", cfg.Path)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return err
	}
	defer store.Close()

	client, dataDir, err := connect(cfg, logger)
	if err != nil {
		return err
	}
	control := t3control.New(client, logger, cfg.Policy.DryRun)
	usageCh := make(chan domain.UsageSample, 256)
	d := daemon.New(cfg, logger, store, control, providerlog.NewTailer(providerlog.Options{
		Dir:          t3api.ProviderLogDir(dataDir),
		ScanInterval: cfg.Polling.LogScanInterval.D(),
		Logger:       logger,
		Usage:        usageCh,
	}, store))
	d.Usage = usageCh

	// Version gate, retried with backoff until the server answers.
	backoff := time.Second
	for {
		cctx, cancel := context.WithTimeout(ctx, cfg.T3.RequestTimeout.D())
		desc, err := client.Descriptor(cctx)
		cancel()
		if err == nil {
			allowed, reason := compat.ControlAllowed(desc.ServerVersion, cfg.T3.AllowUnsupportedVersion)
			d.ControlAllowed = allowed
			d.ControlReason = reason
			logger.Info("connected to T3", "url", client.BaseURL, "server_version", desc.ServerVersion, "label", desc.Label, "control_allowed", allowed)
			if reason != "" {
				logger.Warn(reason)
			}
			break
		}
		logger.Warn("T3 server not reachable; retrying", "url", client.BaseURL, "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > cfg.Polling.ReconnectMaxDelay.D() {
			backoff = cfg.Polling.ReconnectMaxDelay.D()
		}
	}
	return d.Run(ctx)
}

func cmdStatus(g globalFlags, limit int, asJSON, showAll bool) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	if _, err := os.Stat(statePath); err != nil {
		return fmt.Errorf("no state database at %s (has the watchdog run yet?)", statePath)
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx := context.Background()
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		return err
	}
	var intents []domain.ResumeIntent
	if showAll {
		intents, err = store.ListResumeIntents(ctx)
	} else {
		intents, err = store.ListResumeIntents(ctx, domain.ResumePending, domain.ResumeEligible, domain.ResumeResuming, domain.ResumeFailed)
	}
	if err != nil {
		return err
	}
	actions, err := store.RecentActions(ctx, limit)
	if err != nil {
		return err
	}
	if asJSON {
		return printJSON(map[string]any{"buckets": buckets, "resumeIntents": intents, "recentActions": actions})
	}
	fmt.Printf("Buckets (%d):\n", len(buckets))
	now := time.Now()
	for _, b := range buckets {
		reset := "no reset time"
		if b.ResetsAt != nil {
			reset = fmt.Sprintf("resets %s (%s)", b.ResetsAt.Local().Format("2006-01-02 15:04"), relative(*b.ResetsAt, now))
		}
		fmt.Printf("  %-45s %5.0f%%  %-9s %s  observed %s\n", b.Key, b.UsedPercent, b.Phase, reset, relative(b.ObservedAt, now))
	}
	fmt.Printf("\nResume intents (%d):\n", len(intents))
	for _, i := range intents {
		fmt.Printf("  %s  %-10s stopped %s  %s\n", i.ThreadID, i.Status, relative(i.StoppedAt, now), i.Reason)
	}
	fmt.Printf("\nRecent actions (%d):\n", len(actions))
	for _, a := range actions {
		dry := ""
		if a.DryRun {
			dry = " [dry-run]"
		}
		e := ""
		if a.Err != "" {
			e = "  ERROR: " + a.Err
		}
		fmt.Printf("  %s  %-6s%s %s %s %s%s\n", a.At.Local().Format("01-02 15:04:05"), a.Kind, dry, a.Bucket, a.ThreadID, a.Detail, e)
	}
	return nil
}

func relative(t, now time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := now.Sub(t)
	if d < 0 {
		return "in " + (-d).Round(time.Minute).String()
	}
	return d.Round(time.Second).String() + " ago"
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// cmdReplay feeds a recorded provider log or JSONL file through the policy
// engine with a fake T3 that reports one running thread per provider
// instance seen, so decisions can be inspected without a server.
func cmdReplay(g globalFlags, file string, speed float64, withState, resume bool) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	cfg.Policy.DryRun = true
	if resume {
		cfg.Resume.Enabled = true
	}
	logger := newLogger(cfg.LogLevel)
	snaps, err := providerlog.ReadFile(file)
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		return fmt.Errorf("%s contains no %s records", file, providerlog.EventType)
	}
	var store *sqlite.Store
	if withState {
		statePath, err := cfg.ResolveStatePath()
		if err != nil {
			return err
		}
		store, err = sqlite.Open(statePath)
		if err != nil {
			return err
		}
	} else {
		store, err = sqlite.Open(":memory:")
		if err != nil {
			return err
		}
	}
	defer store.Close()
	fake := &replayControl{}
	for _, s := range snaps {
		fake.ensure(s.Key.ProviderInstanceID)
	}
	d := daemon.New(cfg, logger, store, fake, nil)
	// Replay time follows the events so that expiry and grace timers behave
	// as they did when the events were recorded.
	var clock time.Time
	d.SetClock(func() time.Time { return clock })
	ctx := context.Background()
	fmt.Printf("Replaying %d snapshots from %s (thresholds %.0f/%.0f/%.0f, grace %s)\n\n",
		len(snaps), file, cfg.Policy.WarnPercent, cfg.Policy.DrainPercent, cfg.Policy.StopPercent, cfg.Policy.GracePeriod.D())
	// Timers are evaluated at one-minute steps between events so that a
	// grace period expires at the recorded time, not at the next event.
	step := func(until time.Time) {
		for clock.Before(until) {
			next := clock.Add(time.Minute)
			if next.After(until) {
				next = until
			}
			clock = next
			d.Tick(ctx)
		}
	}
	for i, s := range snaps {
		if i > 0 && speed > 0 {
			time.Sleep(time.Duration(float64(s.ObservedAt.Sub(snaps[i-1].ObservedAt)) * speed))
		}
		if clock.IsZero() {
			clock = s.ObservedAt
		}
		step(s.ObservedAt)
		clock = s.ObservedAt.Add(time.Millisecond)
		d.HandleSnapshot(ctx, s)
		d.Poll(ctx)
	}
	// Let a pending resume play out after the last event.
	if cfg.Resume.Enabled {
		step(clock.Add(cfg.Resume.ResetSettleDelay.D() + time.Minute))
		d.Poll(ctx)
	}
	actions, err := store.RecentActions(ctx, 1000)
	if err != nil {
		return err
	}
	fmt.Println()
	for i := len(actions) - 1; i >= 0; i-- {
		a := actions[i]
		fmt.Printf("%s  %-6s %s %s %s\n", a.At.UTC().Format(time.RFC3339), a.Kind, a.Bucket, a.ThreadID, a.Detail)
	}
	buckets, _ := store.ListBuckets(ctx)
	fmt.Println()
	for _, b := range buckets {
		fmt.Printf("final: %s %.0f%% phase=%s epoch=%s\n", b.Key, b.UsedPercent, b.Phase, b.Epoch)
	}
	return nil
}

// replayControl is a fake T3 with one perpetually running thread per
// provider instance.
type replayControl struct {
	threads []domain.Thread
}

func (r *replayControl) ensure(instance string) {
	for _, t := range r.threads {
		if t.ProviderInstanceID == instance {
			return
		}
	}
	r.threads = append(r.threads, domain.Thread{
		ID: "replay-" + instance, Title: "replay thread (" + instance + ")", ProviderInstanceID: instance,
		Model: "replay-model", Running: true, TurnState: "running", TurnID: "turn-" + instance,
		ModelSelection: map[string]any{"instanceId": instance, "model": "replay-model"},
	})
}

func (r *replayControl) ListThreads(context.Context) ([]domain.Thread, error) { return r.threads, nil }
func (r *replayControl) GetThread(_ context.Context, id string) (*domain.Thread, error) {
	for _, t := range r.threads {
		if t.ID == id {
			t := t
			return &t, nil
		}
	}
	return nil, nil
}
func (r *replayControl) LastUserMessageAt(context.Context, string) (*time.Time, error) {
	return nil, nil
}
func (r *replayControl) WarnThread(_ context.Context, t domain.Thread, w domain.Warning) error {
	fmt.Printf("  -> %s message to %s:\n     %s\n", w.Kind, t.Title, w.Text)
	return nil
}
func (r *replayControl) StopThread(_ context.Context, t domain.Thread, mode t3control.StopMode) error {
	fmt.Printf("  -> stop %s (%s)\n", t.Title, mode)
	return nil
}
func (r *replayControl) ResumeThread(_ context.Context, t domain.Thread, _ string) error {
	fmt.Printf("  -> resume %s\n", t.Title)
	return nil
}

func cmdInstallService(g globalFlags, force, enable bool) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	mgr, err := platform.Current()
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	exe, _ = filepath.EvalSymlinks(exe)
	configPath, _ := filepath.Abs(cfg.Path)
	if _, err := os.Stat(configPath); err != nil {
		return fmt.Errorf("configuration %s does not exist; run `t3-quota-watchdog init` first", configPath)
	}
	res, err := mgr.Install(platform.InstallOptions{Binary: exe, ConfigPath: configPath, Force: force, Enable: enable, DryRun: cfg.Policy.DryRun})
	if err != nil {
		return err
	}
	fmt.Printf("Wrote %s\n", res.Path)
	if !enable {
		fmt.Println("\nEnable and inspect it with:")
		for _, c := range res.Commands {
			fmt.Println("  " + c)
		}
	}
	for _, n := range res.Notes {
		fmt.Println("\n" + n)
	}
	return nil
}

func cmdUninstallService() error {
	mgr, err := platform.Current()
	if err != nil {
		return err
	}
	res, err := mgr.Uninstall()
	if err != nil {
		return err
	}
	fmt.Printf("Removed %s\n", res.Path)
	for _, n := range res.Notes {
		fmt.Println(n)
	}
	return nil
}
