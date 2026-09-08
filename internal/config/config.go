// Package config loads and validates the watchdog configuration.
//
// Precedence, highest first: command-line flags, environment variables
// prefixed with T3_QUOTA_WATCHDOG_, the configuration file, then platform
// defaults and T3 discovery.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// EnvPrefix is the prefix of every environment variable the watchdog reads.
const EnvPrefix = "T3_QUOTA_WATCHDOG_"

// Duration is a time.Duration that unmarshals from YAML strings like "60s".
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", raw, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

// D returns the duration as time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// T3 configures how the watchdog reaches the T3 server.
type T3 struct {
	// URL is the base URL of the T3 server. Empty means discover it from the
	// server-runtime.json file in the data directory.
	URL string `yaml:"url"`
	// DataDir is the T3 base directory (the parent of userdata). Empty means
	// T3CODE_HOME or ~/.t3.
	DataDir string `yaml:"data_dir"`
	// Token is a bearer access token. Prefer TokenCommand so that nothing
	// long-lived sits in the configuration file.
	Token string `yaml:"token"`
	// TokenCommand is a shell-free argv that prints a bearer token on stdout.
	// Empty means run `t3 auth session issue --token-only --ttl <ttl>`.
	TokenCommand []string `yaml:"token_command"`
	// TokenTTL is the lifetime requested from the default token command and
	// the refresh interval for any command-issued token.
	TokenTTL Duration `yaml:"token_ttl"`
	// T3Binary is the t3 CLI used by the default token command. Empty means
	// look it up on PATH and in the usual mise/npm locations.
	T3Binary string `yaml:"t3_binary"`
	// AllowUnsupportedVersion lets control actions run against a T3 server
	// outside the tested compatibility range.
	AllowUnsupportedVersion bool `yaml:"allow_unsupported_version"`
	// RequestTimeout bounds each HTTP request.
	RequestTimeout Duration `yaml:"request_timeout"`
}

// Policy configures the thresholds and the safety switches.
type Policy struct {
	WarnPercent  float64  `yaml:"warn_percent"`
	DrainPercent float64  `yaml:"drain_percent"`
	StopPercent  float64  `yaml:"stop_percent"`
	GracePeriod  Duration `yaml:"grace_period"`
	RearmPercent float64  `yaml:"rearm_percent"`
	// RearmObservations is how many consecutive observations below
	// rearm_percent rearm a bucket that has no reset time.
	RearmObservations int `yaml:"rearm_observations"`
	// DryRun logs every decision but never sends messages, interrupts or
	// resumes. It is on by default.
	DryRun bool `yaml:"dry_run"`
	// StopMode selects the T3 operation for a hard stop: "interrupt" ends
	// the running turn, "session-stop" ends the provider session.
	StopMode string `yaml:"stop_mode"`
	// EscalateToSessionStop retries a failed interrupt with a session stop.
	EscalateToSessionStop bool `yaml:"escalate_to_session_stop"`
	// StopVerifyTimeout is how long to wait for a thread to leave the
	// running state after a stop before retrying.
	StopVerifyTimeout Duration `yaml:"stop_verify_timeout"`
	// StopRetries is how many times a stop is retried.
	StopRetries int `yaml:"stop_retries"`
	// StopNewSessions stops threads that start running while a bucket that
	// applies to them is already in the stopped phase.
	StopNewSessions bool `yaml:"stop_new_sessions"`
	// IgnoreWindows lists window names (case-insensitive substrings) that are
	// recorded but never trigger actions.
	IgnoreWindows []string `yaml:"ignore_windows"`
	// MaxSnapshotAge discards observations older than this at startup, so a
	// daemon that was down for a long time does not act on history.
	MaxSnapshotAge Duration `yaml:"max_snapshot_age"`
	// HistoryRetention is how long quota observations and token samples are
	// kept for reports.
	HistoryRetention Duration `yaml:"history_retention"`
}

// Resume configures automatic resumption.
type Resume struct {
	Enabled bool `yaml:"enabled"`
	// BelowPercent is the usage every bucket that caused a stop must be
	// below before the thread resumes.
	BelowPercent float64 `yaml:"below_percent"`
	// ResetConfirmationRequired demands a fresh provider snapshot after the
	// reset time; the wall clock alone never resumes anything.
	ResetConfirmationRequired bool `yaml:"reset_confirmation_required"`
	// ResetSettleDelay is the wait after a confirmed reset before the first
	// resume.
	ResetSettleDelay Duration `yaml:"reset_settle_delay"`
	// IntervalBetweenThreads staggers resumes of one provider instance.
	IntervalBetweenThreads Duration `yaml:"interval_between_threads"`
	// MaxConcurrentPerProvider bounds resumes in flight per provider instance.
	MaxConcurrentPerProvider int `yaml:"max_concurrent_per_provider"`
	// CoordinatorThreadsOnly is kept for configuration compatibility. T3
	// has no child threads, so every thread is a coordinator.
	CoordinatorThreadsOnly bool `yaml:"coordinator_threads_only"`
	// RequireCheckpoint is reserved; the watchdog cannot verify checkpoints.
	RequireCheckpoint bool `yaml:"require_checkpoint"`
	// Prompt is the message sent to a resumed thread.
	Prompt string `yaml:"prompt"`
	// MaxIntentAge cancels intents older than this.
	MaxIntentAge Duration `yaml:"max_intent_age"`
}

// Polling configures how often the watchdog reads T3 state.
type Polling struct {
	// SnapshotInterval is how often the shell snapshot is fetched.
	SnapshotInterval Duration `yaml:"snapshot_interval"`
	// ReconnectMaxDelay caps the backoff after a failed T3 request.
	ReconnectMaxDelay Duration `yaml:"reconnect_max_delay"`
	// LogScanInterval is the fallback poll of the provider log directory for
	// platforms or filesystems where fsnotify misses events.
	LogScanInterval Duration `yaml:"log_scan_interval"`
}

// Override changes thresholds for matching buckets.
type Override struct {
	Match struct {
		// Provider matches the T3 provider instance id (glob).
		Provider string `yaml:"provider"`
		// LimitName matches the limit name or id (glob, case-insensitive).
		LimitName string `yaml:"limit_name"`
		// Window matches the window name (glob, case-insensitive).
		Window string `yaml:"window"`
	} `yaml:"match"`
	WarnPercent  *float64  `yaml:"warn_percent"`
	DrainPercent *float64  `yaml:"drain_percent"`
	StopPercent  *float64  `yaml:"stop_percent"`
	GracePeriod  *Duration `yaml:"grace_period"`
}

// Messages holds the texts sent to threads.
type Messages struct {
	Warn  string `yaml:"warn"`
	Drain string `yaml:"drain"`
}

// Notifications configures desktop notifications.
type Notifications struct {
	Desktop bool `yaml:"desktop"`
}

// Config is the full configuration.
type Config struct {
	T3            T3            `yaml:"t3"`
	Policy        Policy        `yaml:"policy"`
	Resume        Resume        `yaml:"resume"`
	Polling       Polling       `yaml:"polling"`
	Overrides     []Override    `yaml:"overrides"`
	Messages      Messages      `yaml:"messages"`
	Notifications Notifications `yaml:"notifications"`
	// StatePath is the SQLite database. Empty means the platform default.
	StatePath string `yaml:"state_path"`
	// LogLevel is debug, info, warn or error.
	LogLevel string `yaml:"log_level"`

	// Path is where the configuration was read from, for diagnostics.
	Path string `yaml:"-"`
}

// DefaultWarnMessage is the warning delivered at the warn threshold.
const DefaultWarnMessage = `Provider quota warning from the T3 quota watchdog: "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Do not start new subagents. Ask active subagents to checkpoint and return their results, consolidate the current work, then stop at a clean point.`

// DefaultDrainMessage is the message delivered at the drain threshold.
const DefaultDrainMessage = `Provider quota is nearly exhausted (T3 quota watchdog): "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Stop spawning subagents now. Cancel or finish active subagents, collect their results, write a short checkpoint of the current state and remaining work, then stop. The session will be interrupted in {{.GracePeriod}} if it is still running.`

// DefaultResumePrompt is sent to a resumed thread.
const DefaultResumePrompt = `The provider quota has recovered (T3 quota watchdog). Resume the interrupted task from the latest checkpoint. First inspect the current thread, repository state, and any partial results. Do not assume previous subagents are still running. Continue only the unfinished work, and create new subagents only when needed.`

// Default returns the shipped defaults.
func Default() Config {
	var c Config
	c.T3.TokenTTL = Duration(1 * time.Hour)
	c.T3.RequestTimeout = Duration(30 * time.Second)
	c.Policy.WarnPercent = 85
	c.Policy.DrainPercent = 90
	c.Policy.StopPercent = 95
	c.Policy.GracePeriod = Duration(60 * time.Second)
	c.Policy.RearmPercent = 50
	c.Policy.RearmObservations = 2
	c.Policy.DryRun = true
	c.Policy.StopMode = "interrupt"
	c.Policy.EscalateToSessionStop = true
	c.Policy.StopVerifyTimeout = Duration(20 * time.Second)
	c.Policy.StopRetries = 3
	c.Policy.StopNewSessions = true
	c.Policy.IgnoreWindows = []string{"overage"}
	c.Policy.MaxSnapshotAge = Duration(12 * time.Hour)
	c.Policy.HistoryRetention = Duration(90 * 24 * time.Hour)
	c.Resume.Enabled = false
	c.Resume.BelowPercent = 50
	c.Resume.ResetConfirmationRequired = true
	c.Resume.ResetSettleDelay = Duration(2 * time.Minute)
	c.Resume.IntervalBetweenThreads = Duration(45 * time.Second)
	c.Resume.MaxConcurrentPerProvider = 1
	c.Resume.CoordinatorThreadsOnly = true
	c.Resume.Prompt = DefaultResumePrompt
	c.Resume.MaxIntentAge = Duration(14 * 24 * time.Hour)
	c.Polling.SnapshotInterval = Duration(15 * time.Second)
	c.Polling.ReconnectMaxDelay = Duration(30 * time.Second)
	c.Polling.LogScanInterval = Duration(10 * time.Second)
	c.Messages.Warn = DefaultWarnMessage
	c.Messages.Drain = DefaultDrainMessage
	c.Notifications.Desktop = true
	c.LogLevel = "info"
	return c
}

// Load reads the configuration file when it exists, applies environment
// overrides, and validates the result. A missing file is not an error: the
// defaults apply.
func Load(path string) (Config, error) {
	c := Default()
	c.Path = path
	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			if err := yaml.Unmarshal(data, &c); err != nil {
				return c, fmt.Errorf("parse %s: %w", path, err)
			}
		case errors.Is(err, os.ErrNotExist):
			// Defaults apply.
		default:
			return c, fmt.Errorf("read %s: %w", path, err)
		}
	}
	if err := c.applyEnv(); err != nil {
		return c, err
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// applyEnv overrides fields from T3_QUOTA_WATCHDOG_* variables.
func (c *Config) applyEnv() error {
	str := func(name string, target *string) {
		if v, ok := os.LookupEnv(EnvPrefix + name); ok {
			*target = v
		}
	}
	boolean := func(name string, target *bool) error {
		v, ok := os.LookupEnv(EnvPrefix + name)
		if !ok {
			return nil
		}
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%s%s: %w", EnvPrefix, name, err)
		}
		*target = parsed
		return nil
	}
	float := func(name string, target *float64) error {
		v, ok := os.LookupEnv(EnvPrefix + name)
		if !ok {
			return nil
		}
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("%s%s: %w", EnvPrefix, name, err)
		}
		*target = parsed
		return nil
	}
	dur := func(name string, target *Duration) error {
		v, ok := os.LookupEnv(EnvPrefix + name)
		if !ok {
			return nil
		}
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s%s: %w", EnvPrefix, name, err)
		}
		*target = Duration(parsed)
		return nil
	}
	str("T3_URL", &c.T3.URL)
	str("T3_DATA_DIR", &c.T3.DataDir)
	str("T3_TOKEN", &c.T3.Token)
	str("T3_BINARY", &c.T3.T3Binary)
	str("STATE_PATH", &c.StatePath)
	str("LOG_LEVEL", &c.LogLevel)
	str("STOP_MODE", &c.Policy.StopMode)
	for _, err := range []error{
		boolean("DRY_RUN", &c.Policy.DryRun),
		boolean("RESUME_ENABLED", &c.Resume.Enabled),
		boolean("DESKTOP_NOTIFICATIONS", &c.Notifications.Desktop),
		boolean("ALLOW_UNSUPPORTED_VERSION", &c.T3.AllowUnsupportedVersion),
		boolean("STOP_NEW_SESSIONS", &c.Policy.StopNewSessions),
		float("WARN_PERCENT", &c.Policy.WarnPercent),
		float("DRAIN_PERCENT", &c.Policy.DrainPercent),
		float("STOP_PERCENT", &c.Policy.StopPercent),
		float("REARM_PERCENT", &c.Policy.RearmPercent),
		dur("GRACE_PERIOD", &c.Policy.GracePeriod),
		dur("SNAPSHOT_INTERVAL", &c.Polling.SnapshotInterval),
		dur("TOKEN_TTL", &c.T3.TokenTTL),
	} {
		if err != nil {
			return err
		}
	}
	return nil
}

// Validate rejects configurations the daemon must not run with.
func (c *Config) Validate() error {
	p := c.Policy
	if !(p.WarnPercent < p.DrainPercent && p.DrainPercent < p.StopPercent && p.StopPercent <= 100) {
		return fmt.Errorf("policy: warn_percent < drain_percent < stop_percent <= 100 is required (got %v, %v, %v)",
			p.WarnPercent, p.DrainPercent, p.StopPercent)
	}
	if p.WarnPercent <= 0 {
		return errors.New("policy: warn_percent must be positive")
	}
	if p.RearmPercent <= 0 || p.RearmPercent >= p.WarnPercent {
		return fmt.Errorf("policy: rearm_percent must be between 0 and warn_percent (got %v)", p.RearmPercent)
	}
	if p.RearmObservations < 1 {
		return errors.New("policy: rearm_observations must be at least 1")
	}
	if p.GracePeriod < 0 {
		return errors.New("policy: grace_period must not be negative")
	}
	switch p.StopMode {
	case "interrupt", "session-stop":
	default:
		return fmt.Errorf("policy: stop_mode must be \"interrupt\" or \"session-stop\" (got %q)", p.StopMode)
	}
	if p.StopRetries < 0 {
		return errors.New("policy: stop_retries must not be negative")
	}
	for i, o := range c.Overrides {
		warn, drain, stop := p.WarnPercent, p.DrainPercent, p.StopPercent
		if o.WarnPercent != nil {
			warn = *o.WarnPercent
		}
		if o.DrainPercent != nil {
			drain = *o.DrainPercent
		}
		if o.StopPercent != nil {
			stop = *o.StopPercent
		}
		if !(warn < drain && drain < stop && stop <= 100) {
			return fmt.Errorf("overrides[%d]: warn < drain < stop <= 100 is required after applying the override (got %v, %v, %v)", i, warn, drain, stop)
		}
		if o.Match.Provider == "" && o.Match.LimitName == "" && o.Match.Window == "" {
			return fmt.Errorf("overrides[%d]: match needs at least one of provider, limit_name, window", i)
		}
	}
	r := c.Resume
	if r.BelowPercent <= 0 || r.BelowPercent >= p.WarnPercent {
		return fmt.Errorf("resume: below_percent must be between 0 and warn_percent (got %v)", r.BelowPercent)
	}
	if r.MaxConcurrentPerProvider < 1 {
		return errors.New("resume: max_concurrent_per_provider must be at least 1")
	}
	if r.IntervalBetweenThreads < 0 || r.ResetSettleDelay < 0 {
		return errors.New("resume: delays must not be negative")
	}
	if strings.TrimSpace(r.Prompt) == "" {
		return errors.New("resume: prompt must not be empty")
	}
	if c.Polling.SnapshotInterval.D() < time.Second {
		return errors.New("polling: snapshot_interval must be at least 1s")
	}
	if c.Polling.LogScanInterval.D() < time.Second {
		return errors.New("polling: log_scan_interval must be at least 1s")
	}
	if c.T3.TokenTTL.D() < time.Minute {
		return errors.New("t3: token_ttl must be at least 1m")
	}
	if c.Policy.HistoryRetention.D() < 24*time.Hour {
		return errors.New("policy: history_retention must be at least 24h")
	}
	if c.T3.RequestTimeout.D() <= 0 {
		return errors.New("t3: request_timeout must be positive")
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "warning", "error":
	default:
		return fmt.Errorf("log_level must be debug, info, warn or error (got %q)", c.LogLevel)
	}
	if strings.TrimSpace(c.Messages.Warn) == "" || strings.TrimSpace(c.Messages.Drain) == "" {
		return errors.New("messages: warn and drain must not be empty")
	}
	return nil
}

// Paths are the resolved platform locations.
type Paths struct {
	ConfigDir  string
	ConfigFile string
	StateDir   string
	StateFile  string
}

// DefaultPaths resolves the XDG (or platform) locations for the current user.
func DefaultPaths() (Paths, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return Paths{}, fmt.Errorf("resolve user config dir: %w", err)
	}
	stateDir := os.Getenv("XDG_STATE_HOME")
	if stateDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Paths{}, fmt.Errorf("resolve home dir: %w", err)
		}
		stateDir = filepath.Join(home, ".local", "state")
	}
	p := Paths{
		ConfigDir: filepath.Join(configDir, "t3-quota-watchdog"),
		StateDir:  filepath.Join(stateDir, "t3-quota-watchdog"),
	}
	p.ConfigFile = filepath.Join(p.ConfigDir, "config.yaml")
	p.StateFile = filepath.Join(p.StateDir, "state.db")
	return p, nil
}

// ResolveStatePath returns the configured state path or the platform default.
func (c *Config) ResolveStatePath() (string, error) {
	if c.StatePath != "" {
		return c.StatePath, nil
	}
	p, err := DefaultPaths()
	if err != nil {
		return "", err
	}
	return p.StateFile, nil
}

// ResolveDataDir returns the T3 base directory: the configured one,
// T3CODE_HOME, or ~/.t3.
func (c *Config) ResolveDataDir() (string, error) {
	if c.T3.DataDir != "" {
		return expandHome(c.T3.DataDir)
	}
	if v := strings.TrimSpace(os.Getenv("T3CODE_HOME")); v != "" {
		return expandHome(v)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, ".t3"), nil
}

func expandHome(p string) (string, error) {
	if p == "~" || strings.HasPrefix(p, "~/") || strings.HasPrefix(p, "~\\") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		return filepath.Join(home, p[1:]), nil
	}
	return filepath.Abs(p)
}

// Sample renders the fully commented sample configuration.
func Sample() string {
	return sampleConfig
}
