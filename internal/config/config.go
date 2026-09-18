// Package config loads and validates the watchdog configuration.
//
// Precedence, highest first: command-line flags, environment variables
// prefixed with T3_STEWARD_, the configuration file, then platform
// defaults and T3 discovery.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"gopkg.in/yaml.v3"
)

// EnvPrefix is the prefix of every environment variable the watchdog reads.
const EnvPrefix = "T3_STEWARD_"

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
	// TokenFile names a private file (mode 0600) whose content is the bearer
	// token. It is read at load when Token is empty, so a host whose t3 CLI
	// has gone away can still reach its T3 server without putting the token
	// in the configuration file itself.
	TokenFile string `yaml:"token_file"`
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
	// SendThreadEnvironment adds the task execution identity to the
	// thread.create command as an environment object.
	//
	// It is off by default because no tested T3 release verifies that field,
	// and the dispatch result carries a sequence number only, so a caller
	// cannot tell from the response whether it was honoured, ignored, or would
	// have been rejected. A server that rejects unknown fields would fail every
	// uncontained dispatch, identically on retry. Tasks do not need it: the
	// worker writes the same identity into the prepared workspace. Turn it on
	// only after verifying the field against the deployed T3 version, as
	// docs/t3-protocol.md describes.
	SendThreadEnvironment bool `yaml:"send_thread_environment"`
	// RequestTimeout bounds each HTTP request.
	RequestTimeout Duration `yaml:"request_timeout"`
}

// WindowOverride relabels or rescopes one provider window.
type WindowOverride struct {
	// Label is the human-readable limit name used in messages.
	Label string `yaml:"label"`
	// Model limits the window to threads whose model id contains it.
	Model string `yaml:"model"`
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
	// IgnoreWindows lists window names (globs, case-insensitive) that are
	// recorded but never trigger actions.
	IgnoreWindows []string `yaml:"ignore_windows"`
	// Windows relabels or rescopes provider windows by name: a display
	// label and a model selector (substring of the model id) that limits
	// the window to matching threads.
	Windows map[string]WindowOverride `yaml:"windows"`
	// MaxSnapshotAge discards observations older than this at startup, so a
	// daemon that was down for a long time does not act on history.
	MaxSnapshotAge Duration `yaml:"max_snapshot_age"`
	// RateWindow is how much recent history estimates the burn rate; zero
	// disables the projected-exhaustion ladder.
	RateWindow Duration `yaml:"rate_window"`
	// WarnETA, DrainETA and StopETA escalate when the projected time to
	// exhaustion at the current burn rate falls below them and the window
	// does not reset first.
	WarnETA  Duration `yaml:"warn_eta"`
	DrainETA Duration `yaml:"drain_eta"`
	StopETA  Duration `yaml:"stop_eta"`
	// ResetExemption suppresses warnings and stops when the window resets
	// within this long: stopping then saves nothing.
	ResetExemption Duration `yaml:"reset_exemption"`
	// RunwayMargin: with a known burn rate, no action fires while the
	// projected runway covers this many times the time to the reset.
	RunwayMargin float64 `yaml:"runway_margin"`
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
	// ProbeAfterReset: when the reset time has passed by this long and no
	// reading has confirmed it (nothing is running to produce one), one
	// stopped thread per provider is resumed to obtain that reading. Zero
	// disables the probe.
	ProbeAfterReset Duration `yaml:"probe_after_reset"`
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
		// MinWindowDuration matches a reported window length at least this long.
		// Zero disables the duration filter; unknown durations do not match.
		MinWindowDuration Duration `yaml:"min_window_duration"`
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

// Report configures the consumption report.
type Report struct {
	// Peak is the schedule treated as peak hours, local time.
	Peak string `yaml:"peak"`
	// Remotes are SSH hosts running the watchdog whose data the report
	// merges in (same provider account on several machines).
	Remotes []string `yaml:"remotes"`
}

// Backlog configures the quota-gated task runner and its forecast.
type Backlog struct {
	// Enabled turns the runner on. Tasks are markdown files in Dir.
	Enabled bool `yaml:"enabled"`
	// Dir holds the task files; empty means <config dir>/backlog.
	Dir string `yaml:"dir"`
	// QuietFor is how long no interactive thread must have run before a
	// gated task starts.
	QuietFor Duration `yaml:"quiet_for"`
	// LongWindowCap is the usage ceiling on windows longer than a day
	// (weekly limits), in percent.
	LongWindowCap float64 `yaml:"long_window_cap_percent"`
	// HistoryDays is how much history feeds the forecast.
	HistoryDays int `yaml:"history_days"`
	// Preamble precedes every task prompt; empty means the built-in text.
	Preamble string `yaml:"preamble"`
	// HostName is how tasks refer to this machine (default: the OS host
	// name). "local" and "localhost" always mean this machine.
	HostName string `yaml:"host_name"`
	// DefaultHost runs tasks that name no host. Empty means this machine;
	// another host's SSH alias forwards them there.
	DefaultHost string `yaml:"default_host"`
	// SafetyMargin is the percent of a window always left unused.
	SafetyMargin float64 `yaml:"safety_margin_percent"`
	// FallbackPerHour is the interactive demand assumed for an hour slot
	// with too little history, in percent of the window per hour.
	FallbackPerHour float64 `yaml:"fallback_per_hour_percent"`
	// Quantile of past occurrences the forecast covers (0.8 = a heavy
	// week rather than the average one).
	Quantile float64 `yaml:"quantile"`
	// MinSamples is how many past occurrences a slot needs before its
	// forecast is used instead of the fallback.
	MinSamples int `yaml:"min_samples"`
}

// UIArchive configures reversible settlement-based T3 UI archiving.
type UIArchive struct {
	Enabled         bool     `yaml:"enabled"`
	DryRun          bool     `yaml:"dry_run"`
	BackgroundAfter Duration `yaml:"background_after"`
	UserAfter       Duration `yaml:"user_after"`
	MaxPerPass      int      `yaml:"max_per_pass"`
}

// Archive configures cold storage of finished threads.
type Archive struct {
	Enabled bool `yaml:"enabled"`
	// After is how long a thread must have gone without an update.
	After Duration `yaml:"after"`
	// Destination is a directory, or host:/path reached over SSH.
	Destination string `yaml:"destination"`
	// At is the local time of day the daily run starts.
	At string `yaml:"at"`
	// DeleteFromT3 deletes the thread from T3 once its bundle is verified.
	DeleteFromT3 bool `yaml:"delete_from_t3"`
	// RemoveLocal removes provider logs and transcripts once verified.
	RemoveLocal bool `yaml:"remove_local"`
	// KeepTranscripts keeps a bundled transcript on disk until it is this
	// old, because other tools (toolfeedback) read recent transcripts.
	KeepTranscripts Duration `yaml:"keep_transcripts"`
	// TranscriptDirs are searched for provider transcripts; empty means
	// ~/.claude/projects and ~/.codex/sessions.
	TranscriptDirs []string `yaml:"transcript_dirs"`
	// MaxPerRun bounds the threads archived per daily run.
	MaxPerRun int `yaml:"max_per_run"`
}

// BacklogV2 configures the disabled-by-default coordinator runtime.
type BacklogV2 struct {
	Mode              string                    `yaml:"mode"`
	Coordinator       V2Coordinator             `yaml:"coordinator"`
	LocalWorker       V2LocalWorker             `yaml:"local_worker"`
	Workers           map[string]V2Worker       `yaml:"workers"`
	Projects          map[string]V2Project      `yaml:"projects"`
	SetupProfiles     map[string]V2SetupProfile `yaml:"setup_profiles"`
	QuotaPools        map[string]V2QuotaPool    `yaml:"quota_pools"`
	CoordinatorClient V2CoordinatorClient       `yaml:"coordinator_client"`
	Storage           V2Storage                 `yaml:"storage"`
	Transport         V2Transport               `yaml:"transport"`
	MessageLimits     V2MessageLimits           `yaml:"message_limits"`
	Freshness         V2Freshness               `yaml:"freshness"`
	Leases            V2Leases                  `yaml:"leases"`
	Scheduling        V2Scheduling              `yaml:"scheduling"`
	StartupAdmission  string                    `yaml:"startup_admission"`
}

type V2Coordinator struct {
	ID string `yaml:"id"`
	// AdminClients are the remote admin clients this coordinator will accept
	// through the restricted coordinator-exchange command, by client
	// principal. A client that is not listed here cannot be authenticated,
	// because the coordinator would have no credential to verify it against.
	AdminClients map[string]V2AdminClient `yaml:"admin_clients"`
}

// V2AdminClient is one remote admin client's credential reference. The value
// behind the reference is resolved at use, never stored in configuration.
type V2AdminClient struct {
	Credential string `yaml:"credential"`
	// Supervisor marks a client that acts as a campaign overseer. Such a client
	// is relayed under the supervisor role instead of remote-admin, which is
	// strictly narrower: the coordinator's own authorizer binds it to the one
	// run and the one activation epoch it was woken for, and refuses every
	// coordinator command outright. The default is false, which is the ordinary
	// deployment in which no client is a supervisor.
	Supervisor bool `yaml:"supervisor"`
}

// V2CoordinatorClient points a host that is not the coordinator at the
// coordinator it administers. Its presence is what selects the remote carrier;
// a coordinator-local client keeps using its own owner-only socket.
type V2CoordinatorClient struct {
	CoordinatorID  string          `yaml:"coordinator_id"`
	Address        string          `yaml:"address"`
	Connection     string          `yaml:"connection"`
	RemoteCommand  string          `yaml:"remote_command"`
	Credential     string          `yaml:"credential"`
	RequestTimeout Duration        `yaml:"request_timeout"`
	MessageLimits  V2MessageLimits `yaml:"message_limits"`
}

// Configured reports whether an operator declared the block at all. A partly
// filled block is a configuration error, not an absent one.
func (c V2CoordinatorClient) Configured() bool {
	return strings.TrimSpace(c.CoordinatorID) != "" || strings.TrimSpace(c.Address) != "" ||
		strings.TrimSpace(c.Credential) != ""
}

// V2LocalWorker fixes the authority identity used by restricted worker commands.
type V2LocalWorker struct {
	ID    string `yaml:"id"`
	Epoch string `yaml:"epoch"`
	// CoordinatorEpoch is an optional floor. The worker adopts the epoch of
	// every authenticated coordinator envelope, never going backwards, so a
	// coordinator restart needs no configuration change here.
	CoordinatorEpoch int64 `yaml:"coordinator_epoch"`
}

type V2Containment struct {
	RuntimePaths   []string `yaml:"runtime_paths"`
	Node           string   `yaml:"node"`
	T3Entry        string   `yaml:"t3_entry"`
	OpenCodeBinary string   `yaml:"opencode_binary"`
	ProviderHosts  []string `yaml:"provider_hosts"`
}

type V2Worker struct {
	Containment   *V2Containment        `yaml:"containment,omitempty"`
	Connection    string                `yaml:"connection" json:"connection,omitempty"`
	Address       string                `yaml:"address"`
	Epoch         string                `yaml:"epoch"`
	AcceptBacklog bool                  `yaml:"accept_backlog"`
	Capabilities  []string              `yaml:"capabilities"`
	Providers     map[string]V2Provider `yaml:"providers"`
	Credential    string                `yaml:"credential"`
	// CPUClass is the operator-assigned performance class of this host:
	// low, medium or high. It is a static capability floor and a user
	// decision. It is never computed from a benchmark and never derived from
	// live load, and it is separate from Executors, which is what may be
	// reserved, and from the pressure a worker observes about itself.
	CPUClass string `yaml:"cpu_class"`
	// Executors is this worker's allocatable executor capacity.
	Executors V2Executors `yaml:"executors"`
}

// V2Executors is the allocatable capacity of one worker's executor pool: how
// many attempts may run at once and the sized resources those slots draw from.
//
// This limit is independent of V2QuotaPool.MaxConcurrent, which bounds
// provider sessions. An attempt holds both before it starts, and neither is
// derived from the other.
type V2Executors struct {
	Slots     int     `yaml:"slots"`
	CPUUnits  float64 `yaml:"cpu_units"`
	MemoryMB  int     `yaml:"memory_mb"`
	ScratchMB int     `yaml:"scratch_mb"`
}

// CPU class values an operator may assign to a worker.
const (
	CPUClassLow    = "low"
	CPUClassMedium = "medium"
	CPUClassHigh   = "high"
)

// FleetWorkerProfile is the initial operator-assigned capacity profile of one
// fleet host.
type FleetWorkerProfile struct {
	CPUClass  string
	Executors V2Executors
}

// FleetWorkerProfiles is the initial fleet mapping: normandy low, homelab
// medium, omarchy-pc high, with the concurrency each host is expected to
// sustain.
//
// This mapping is a user decision that describes intended use, not a benchmark
// result, and nothing in the steward may change it: raising or lowering a
// class is an operator edit to the catalog. The slot counts are the
// qualification floors (two concurrent light attempts on normandy, four mixed
// on homelab, eight light plus build work on omarchy-pc). They are floors to
// sustain rather than ceilings to enforce, so a deployment may configure more.
func FleetWorkerProfiles() map[string]FleetWorkerProfile {
	return map[string]FleetWorkerProfile{
		"normandy":   {CPUClass: CPUClassLow, Executors: V2Executors{Slots: 2}},
		"homelab":    {CPUClass: CPUClassMedium, Executors: V2Executors{Slots: 4}},
		"omarchy-pc": {CPUClass: CPUClassHigh, Executors: V2Executors{Slots: 8}},
	}
}

// ValidCPUClass reports whether a configured class is one of the three values.
// An unset class is accepted here and reported by placement instead, so that a
// worker without a declared class is excluded by a named reason rather than
// silently treated as capable.
func ValidCPUClass(class string) bool {
	switch class {
	case "", CPUClassLow, CPUClassMedium, CPUClassHigh:
		return true
	default:
		return false
	}
}

type V2Provider struct {
	Models    []string `yaml:"models"`
	QuotaPool string   `yaml:"quota_pool"`
}

type V2Project struct {
	DirectoryResources []directoryresource.Binding `yaml:"directory_resources" json:"directoryResources,omitempty"`
	Type               string                      `yaml:"type" json:"type,omitempty"`
	Repository         string                      `yaml:"repository"`
	DefaultRef         string                      `yaml:"default_ref"`
	T3Project          string                      `yaml:"t3_project"`
	SetupProfile       string                      `yaml:"setup_profile"`
	Workers            []string                    `yaml:"workers"`
	Credentials        []string                    `yaml:"credentials"`
	ResourceLocks      []string                    `yaml:"resource_locks"`
}

type V2SetupProfile struct {
	Commands []string `yaml:"commands"`
	Timeout  Duration `yaml:"timeout"`
}

type V2QuotaPool struct {
	Provider      string `yaml:"provider"`
	MaxConcurrent int    `yaml:"max_concurrent"`
}

type V2Storage struct {
	Bundles    string `yaml:"bundles"`
	Artifacts  string `yaml:"artifacts"`
	Workspaces string `yaml:"workspaces"`
	// Retention is how long a worker keeps finished attempt workspaces and
	// their journal records before pruning them (default 72h).
	Retention Duration `yaml:"retention"`
}

type V2Transport struct {
	Kind           string   `yaml:"kind"`
	RequestTimeout Duration `yaml:"request_timeout"`
}

type V2MessageLimits struct {
	MaxBytes         int64 `yaml:"max_bytes"`
	MaxFiles         int   `yaml:"max_files"`
	MaxArtifactBytes int64 `yaml:"max_artifact_bytes"`
}

type V2Freshness struct {
	WorkerMaxAge Duration `yaml:"worker_max_age"`
	QuotaMaxAge  Duration `yaml:"quota_max_age"`
}

type V2Leases struct {
	Duration      Duration `yaml:"duration"`
	RenewInterval Duration `yaml:"renew_interval"`
}

type V2Scheduling struct {
	Interval   Duration `yaml:"interval"`
	CatchUpMax int      `yaml:"catch_up_max"`
}

// Config is the full configuration.
type Config struct {
	coordinatorFleetApplied bool
	// defaultedFleetProjects records the fleet projects loaded with an empty
	// V2Project because the operator file binds nothing for them. Read it
	// through DefaultedFleetProjects.
	defaultedFleetProjects []string
	// QuotaChecks controls this host watchdog and, on a coordinator, fleet-wide
	// scheduler admission and throttling. Nil defaults to enabled.
	QuotaChecks *bool `yaml:"quota_checks"`
	Wait        struct {
		// DryRun holds shell and node wake delivery; nil defaults to false.
		// It does not inherit watchdog policy.dry_run.
		DryRun *bool `yaml:"dry_run"`
		// QuotaChecks gates wake delivery on quota health; nil defaults to true.
		QuotaChecks *bool `yaml:"quota_checks"`
	} `yaml:"wait"`
	T3            T3            `yaml:"t3"`
	Policy        Policy        `yaml:"policy"`
	Resume        Resume        `yaml:"resume"`
	Polling       Polling       `yaml:"polling"`
	Overrides     []Override    `yaml:"overrides"`
	Messages      Messages      `yaml:"messages"`
	Notifications Notifications `yaml:"notifications"`
	Report        Report        `yaml:"report"`
	Backlog       Backlog       `yaml:"backlog"`
	BacklogV2     BacklogV2     `yaml:"backlog_v2"`
	Archive       Archive       `yaml:"archive"`
	UIArchive     UIArchive     `yaml:"ui_archive"`
	// StatePath is the SQLite database. Empty means the platform default.
	StatePath string `yaml:"state_path"`
	// LogLevel is debug, info, warn or error.
	LogLevel string `yaml:"log_level"`

	// Path is where the configuration was read from, for diagnostics.
	Path string `yaml:"-"`
}

// DefaultWarnMessage is the warning delivered at the warn threshold.
const DefaultWarnMessage = `T3 steward quota advisory: "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Continue the current user task and keep work focused. This advisory does not ask you to stop, checkpoint, or cancel active subagents. A separate drain notice will explicitly request a checkpoint and stop if quota becomes critically low.`

// DefaultDrainMessage is the message delivered at the drain threshold.
const DefaultDrainMessage = `T3 steward quota drain: checkpoint and pause. "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Do not start new work or subagents. Ask active subagents to checkpoint and return partial results promptly. Preserve completed work and write a brief checkpoint covering current state, partial results, and remaining work. Then end your turn. T3 steward will interrupt any still-running turn in {{.GracePeriod}}.`

// DefaultResumePrompt is sent to a resumed thread.
const DefaultResumePrompt = `T3 steward quota recovery: quota is available again. Resume the unfinished user task from the latest checkpoint. First inspect current user instructions, repository state, and saved partial results. Verify whether previous subagents are still running before relying on them. Continue only unfinished work within the current authorized scope.`

// QuotaChecksEnabled is independent of simulation/dry-run policy.
func (c Config) QuotaChecksEnabled() bool { return c.QuotaChecks == nil || *c.QuotaChecks }

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
	// Claude reports the Fable weekly limit under this key; the Claude
	// dashboard shows it as the Fable 7-day limit.
	c.Policy.Windows = map[string]WindowOverride{
		"seven_day_overage_included": {Label: "Claude 7-day (Fable)", Model: "fable"},
	}
	c.Policy.MaxSnapshotAge = Duration(12 * time.Hour)
	c.Policy.RateWindow = Duration(10 * time.Minute)
	c.Policy.WarnETA = Duration(30 * time.Minute)
	c.Policy.DrainETA = Duration(15 * time.Minute)
	c.Policy.StopETA = Duration(5 * time.Minute)
	c.Policy.ResetExemption = Duration(10 * time.Minute)
	c.Policy.RunwayMargin = 1.5
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
	c.Resume.ProbeAfterReset = Duration(5 * time.Minute)
	c.Polling.SnapshotInterval = Duration(15 * time.Second)
	c.Polling.ReconnectMaxDelay = Duration(30 * time.Second)
	c.Polling.LogScanInterval = Duration(10 * time.Second)
	c.Messages.Warn = DefaultWarnMessage
	c.Messages.Drain = DefaultDrainMessage
	c.Notifications.Desktop = true
	c.Report.Peak = "Mon-Fri 09:00-17:00"
	c.UIArchive = UIArchive{Enabled: true, BackgroundAfter: Duration(2 * time.Hour), UserAfter: Duration(24 * time.Hour), MaxPerPass: 10}
	c.Archive.After = Duration(48 * time.Hour)
	c.Archive.At = "03:30"
	c.Archive.DeleteFromT3 = true
	c.Archive.RemoveLocal = true
	c.Archive.MaxPerRun = 50
	c.Archive.KeepTranscripts = Duration(14 * 24 * time.Hour)
	c.Backlog.QuietFor = Duration(30 * time.Minute)
	c.Backlog.LongWindowCap = 80
	c.Backlog.HistoryDays = 56
	c.Backlog.SafetyMargin = 10
	c.Backlog.FallbackPerHour = 10
	c.Backlog.Quantile = 0.8
	c.Backlog.MinSamples = 3
	c.BacklogV2.Mode = "disabled"
	c.BacklogV2.StartupAdmission = "closed"
	c.BacklogV2.Transport.Kind = "ssh"
	c.BacklogV2.Transport.RequestTimeout = Duration(30 * time.Second)
	c.BacklogV2.MessageLimits.MaxBytes = 4 << 20
	c.BacklogV2.MessageLimits.MaxFiles = 1000
	c.BacklogV2.MessageLimits.MaxArtifactBytes = 1 << 30
	c.BacklogV2.Freshness.WorkerMaxAge = Duration(1 * time.Minute)
	c.BacklogV2.Freshness.QuotaMaxAge = Duration(1 * time.Minute)
	c.BacklogV2.Leases.Duration = Duration(2 * time.Minute)
	c.BacklogV2.Leases.RenewInterval = Duration(30 * time.Second)
	c.BacklogV2.Scheduling.Interval = Duration(10 * time.Second)
	c.BacklogV2.Scheduling.CatchUpMax = 100
	c.LogLevel = "info"
	return c
}

// LoadFile reads and validates local configuration without applying environment
// overrides. Authority-bearing restricted endpoints use it so their identity,
// epochs, and connection settings can only come from the operator-controlled file.
func LoadFile(path string) (Config, error) {
	c, err := loadFile(path)
	if err != nil {
		return c, err
	}
	if err := c.applyCoordinatorFleet(coordinatorClientHome()); err != nil {
		return c, err
	}
	if err := c.applyCoordinatorClientBootstrap(coordinatorClientHome()); err != nil {
		return c, err
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func loadFile(path string) (Config, error) {
	c := Default()
	c.Path = path
	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			decoder := yaml.NewDecoder(strings.NewReader(string(data)))
			decoder.KnownFields(true)
			if err := decoder.Decode(&c); err != nil {
				return c, fmt.Errorf("parse %s: %w", path, err)
			}
			var extra any
			if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
				if err == nil {
					err = errors.New("multiple YAML documents are not allowed")
				}
				return c, fmt.Errorf("parse %s: %w", path, err)
			}
		case errors.Is(err, os.ErrNotExist):
			// Defaults apply.
		default:
			return c, fmt.Errorf("read %s: %w", path, err)
		}
	}
	c.migrateQuotaDefaults()
	return c, nil
}

// Load reads the configuration file when it exists, applies environment
// overrides, and validates the result. A missing file is not an error: the
// defaults apply.
func Load(path string) (Config, error) {
	c, err := loadFile(path)
	if err != nil {
		return c, err
	}
	if err := c.applyEnv(); err != nil {
		return c, err
	}
	if err := c.T3.loadTokenFile(); err != nil {
		return c, err
	}
	// The UpKeeper-owned file fills the coordinator client only when
	// config.yaml declares none, so the operator's file keeps one author and
	// an explicit block there still wins.
	if err := c.applyCoordinatorFleet(coordinatorClientHome()); err != nil {
		return c, err
	}
	if err := c.applyCoordinatorClientBootstrap(coordinatorClientHome()); err != nil {
		return c, err
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// loadTokenFile fills Token from TokenFile when no token was given directly.
// A token given in the file or the environment wins, so the file is the
// fallback for a host that keeps the secret out of its configuration file.
//
// The file must be a private regular file: it grants access to the T3 server,
// so one that anyone else could read or replace is refused rather than read.
func (t *T3) loadTokenFile() error {
	if t.Token != "" || t.TokenFile == "" {
		return nil
	}
	path := t.TokenFile
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("t3: token_file %s: %w", path, err)
		}
		path = filepath.Join(home, path[2:])
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("t3: token_file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("t3: token_file %s is not a regular file", path)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("t3: token_file %s has mode %04o, want 0600; refusing to read a token that is not private", path, perm)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("t3: token_file: %w", err)
	}
	token := strings.TrimSpace(string(content))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return fmt.Errorf("t3: token_file %s must contain one non-empty token", path)
	}
	t.Token = token
	return nil
}

// applyEnv overrides fields from T3_STEWARD_* variables.
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
	str("T3_TOKEN_FILE", &c.T3.TokenFile)
	str("T3_BINARY", &c.T3.T3Binary)
	str("STATE_PATH", &c.StatePath)
	str("LOG_LEVEL", &c.LogLevel)
	str("BACKLOG_V2_MODE", &c.BacklogV2.Mode)
	str("BACKLOG_V2_COORDINATOR_ID", &c.BacklogV2.Coordinator.ID)
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
	if p.RateWindow > 0 && !(p.StopETA < p.DrainETA && p.DrainETA < p.WarnETA) {
		return fmt.Errorf("policy: stop_eta < drain_eta < warn_eta is required (got %s, %s, %s)", p.StopETA.D(), p.DrainETA.D(), p.WarnETA.D())
	}
	if p.ResetExemption < 0 {
		return errors.New("policy: reset_exemption must not be negative")
	}
	if p.RunwayMargin < 1 {
		return fmt.Errorf("policy: runway_margin must be at least 1 (got %v)", p.RunwayMargin)
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
		if o.Match.MinWindowDuration < 0 {
			return fmt.Errorf("overrides[%d]: min_window_duration must not be negative", i)
		}
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
		if o.Match.Provider == "" && o.Match.LimitName == "" && o.Match.Window == "" && o.Match.MinWindowDuration == 0 {
			return fmt.Errorf("overrides[%d]: match needs at least one of provider, limit_name, window, min_window_duration", i)
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
	if c.UIArchive.Enabled && (c.UIArchive.BackgroundAfter.D() <= 0 || c.UIArchive.UserAfter.D() <= 0 || c.UIArchive.MaxPerPass < 1 || c.UIArchive.MaxPerPass > 100) {
		return errors.New("ui_archive: positive delays and max_per_pass 1..100 are required")
	}
	if c.Archive.Enabled {
		if strings.TrimSpace(c.Archive.Destination) == "" {
			return errors.New("archive: destination is required when enabled")
		}
		if c.Archive.After.D() < time.Hour {
			return errors.New("archive: after must be at least 1h")
		}
		var hh, mm int
		if _, err := fmt.Sscanf(c.Archive.At, "%d:%d", &hh, &mm); err != nil || hh < 0 || hh > 23 || mm < 0 || mm > 59 {
			return fmt.Errorf("archive: at must be HH:MM (got %q)", c.Archive.At)
		}
	}
	if c.Backlog.SafetyMargin < 0 || c.Backlog.SafetyMargin >= 100 {
		return errors.New("backlog: safety_margin_percent must be between 0 and 100")
	}
	if c.Backlog.Quantile <= 0 || c.Backlog.Quantile > 1 {
		return errors.New("backlog: quantile must be between 0 and 1")
	}
	if c.Backlog.MinSamples < 1 {
		return errors.New("backlog: min_samples must be at least 1")
	}
	if c.Backlog.LongWindowCap <= 0 || c.Backlog.LongWindowCap > 100 {
		return errors.New("backlog: long_window_cap_percent must be between 0 and 100")
	}
	if c.Backlog.HistoryDays < 1 {
		return errors.New("backlog: history_days must be at least 1")
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
	return c.validateBacklogV2()
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
		ConfigDir: filepath.Join(configDir, appName),
		StateDir:  filepath.Join(stateDir, appName),
	}
	// The project was renamed from t3-quota-watchdog; adopt its directories
	// once, when the new ones do not exist yet.
	migrateDir(filepath.Join(configDir, legacyAppName), p.ConfigDir)
	migrateDir(filepath.Join(stateDir, legacyAppName), p.StateDir)
	p.ConfigFile = filepath.Join(p.ConfigDir, "config.yaml")
	p.StateFile = filepath.Join(p.StateDir, "state.db")
	return p, nil
}

// Directory names, current and before the rename.
const (
	appName       = "t3-steward"
	legacyAppName = "t3-quota-watchdog"
)

func migrateDir(old, current string) {
	if _, err := os.Stat(current); err == nil {
		return
	}
	if info, err := os.Stat(old); err != nil || !info.IsDir() {
		return
	}
	_ = os.Rename(old, current)
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

// ResolveBacklogDir returns the task directory.
func (c *Config) ResolveBacklogDir() (string, error) {
	if c.Backlog.Dir != "" {
		return expandHome(c.Backlog.Dir)
	}
	p, err := DefaultPaths()
	if err != nil {
		return "", err
	}
	return filepath.Join(p.ConfigDir, "backlog"), nil
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
