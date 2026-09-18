package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/user"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const bucketUsage = `Usage: t3-steward bucket <command> [flags]

Inspect and rearm the quota buckets in this host's state database. The
watchdog's policy engine owns a bucket's phase; this verb is the operator's
hand on it, and every change it makes is recorded as an action with the actor
and the reason, visible in status and in bucket list.

Commands:
  list [--json]                  Every bucket: key, phase, used percent, when it
                                 was observed, when it resets, when it recovered,
                                 when it was stopped and when it was last probed.
  rearm <key> --reason TEXT [--force] [--json]
                                 Set the bucket's phase to normal with the
                                 recovery time now, clear the stop and drain
                                 bookkeeping, and record a rearm action. The
                                 worker treats it as a confirmed recovery, so
                                 its resume rules apply as after a real reset:
                                 a paused owned attempt resumes after the
                                 settle delay without waiting for a reading
                                 only when the stored usage is below
                                 resume.below_percent and policy.warn_percent;
                                 a rearm above either only reopens the bucket
                                 for the next reading, and the output says
                                 which case applies. Refused when the stored
                                 usage is at or above stop_percent unless
                                 --force is given, because the next reading
                                 would stop the bucket again.

The key is what bucket list and status print, for example
claudeAgent/claude/five_hour. A rearm does not invent a reading: the stored
percentage stays, and the next reading rearms or re-stops the bucket honestly.
A stored phase the loaded thresholds would not produce is lowered by the
watchdog itself when it starts; rearm is for the cases that rule does not
cover, such as a provider that extended the quota mid-window.

Exit codes: 0 done, 1 refused or failed.
`

// bucketRow is one line of bucket list, in both renderings.
type bucketRow struct {
	Key               string               `json:"key"`
	Phase             domain.Phase         `json:"phase"`
	UsedPercent       float64              `json:"usedPercent"`
	Healthy           bool                 `json:"healthy"`
	ObservedAt        time.Time            `json:"observedAt"`
	ResetsAt          *time.Time           `json:"resetsAt"`
	RecoveredAt       *time.Time           `json:"recoveredAt"`
	StoppedAt         *time.Time           `json:"stoppedAt"`
	ProbedAt          *time.Time           `json:"probedAt"`
	AppliedThresholds *domain.ThresholdSet `json:"appliedThresholds"`
	// LastRearm is the newest rearm action recorded for the bucket, from
	// the engine, the load-time re-derivation or an operator, with its
	// reason; nil when none is in the recent audit rows.
	LastRearm *domain.ActionRecord `json:"lastRearm"`
}

func cmdBucket(g globalFlags, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Print(bucketUsage)
		return nil
	}
	switch args[0] {
	case "list":
		return cmdBucketList(g, args[1:])
	case "rearm":
		return cmdBucketRearm(g, args[1:])
	default:
		return fmt.Errorf("unknown bucket command %q; see t3-steward bucket --help", args[0])
	}
}

// openBucketStore opens the host's state database read-write, which is what
// a rearm needs; the watchdog may be running on it at the same time.
func openBucketStore(g globalFlags) (config.Config, *sqlite.Store, error) {
	cfg, err := loadConfig(g)
	if err != nil {
		return cfg, nil, err
	}
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return cfg, nil, err
	}
	if _, err := os.Stat(statePath); err != nil {
		return cfg, nil, fmt.Errorf("no state database at %s (has the watchdog run yet?)", statePath)
	}
	store, err := sqlite.Open(statePath)
	return cfg, store, err
}

func cmdBucketList(g globalFlags, args []string) error {
	fs := flag.NewFlagSet("bucket list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("bucket list takes no arguments (got %v)", fs.Args())
	}
	_, store, err := openBucketStore(g)
	if err != nil {
		return err
	}
	defer store.Close()
	states, err := store.ListBuckets(context.Background())
	if err != nil {
		return err
	}
	actions, err := store.RecentActions(context.Background(), 500)
	if err != nil {
		return err
	}
	lastRearm := map[string]*domain.ActionRecord{}
	for i := range actions {
		a := actions[i]
		if a.Kind == domain.ActionRearm && lastRearm[a.Bucket] == nil {
			lastRearm[a.Bucket] = &a
		}
	}
	rows := make([]bucketRow, 0, len(states))
	for _, st := range states {
		rows = append(rows, bucketRow{
			Key: st.Key.String(), Phase: st.Phase, UsedPercent: st.UsedPercent, Healthy: st.Healthy,
			ObservedAt: st.ObservedAt, ResetsAt: st.ResetsAt, RecoveredAt: st.RecoveredAt,
			StoppedAt: st.StoppedAt, ProbedAt: st.ProbedAt, AppliedThresholds: st.AppliedThresholds,
			LastRearm: lastRearm[st.Key.String()],
		})
	}
	if *asJSON {
		return printJSON(map[string]any{"buckets": rows})
	}
	now := time.Now()
	if len(rows) == 0 {
		fmt.Println("No buckets recorded yet; they appear once a provider reports usage.")
		return nil
	}
	for _, r := range rows {
		fmt.Printf("%s\n", r.Key)
		fmt.Printf("  phase %s at %.0f%%, observed %s, %s\n", r.Phase, r.UsedPercent, relative(r.ObservedAt, now), resetText(r.ResetsAt, now))
		fmt.Printf("  recovered %s, stopped %s, probed %s, thresholds %s\n",
			optionalRelative(r.RecoveredAt, now), optionalRelative(r.StoppedAt, now), optionalRelative(r.ProbedAt, now), thresholdText(r.AppliedThresholds))
		if r.LastRearm != nil {
			fmt.Printf("  last rearm %s: %s\n", relative(r.LastRearm.At, now), r.LastRearm.Detail)
		}
	}
	return nil
}

func cmdBucketRearm(g globalFlags, args []string) error {
	fs := flag.NewFlagSet("bucket rearm", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	reason := fs.String("reason", "", "why the bucket is rearmed (recorded with the action)")
	force := fs.Bool("force", false, "rearm even when the stored usage is at or above stop_percent")
	asJSON := fs.Bool("json", false, "print JSON")
	// The key comes first, before the flags, so parse from the second word.
	var key string
	rest := args
	if len(rest) > 0 && len(rest[0]) > 0 && rest[0][0] != '-' {
		key = rest[0]
		rest = rest[1:]
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if key == "" && fs.NArg() > 0 {
		key = fs.Arg(0)
		if fs.NArg() > 1 {
			return fmt.Errorf("bucket rearm takes one key (got %v)", fs.Args())
		}
	} else if fs.NArg() > 0 {
		return fmt.Errorf("bucket rearm takes one key (got %s and %v)", key, fs.Args())
	}
	if key == "" {
		return fmt.Errorf("bucket rearm needs the bucket key, for example claudeAgent/claude/five_hour; see bucket list")
	}
	if *reason == "" {
		return fmt.Errorf("bucket rearm needs --reason TEXT; it is recorded with the action")
	}
	cfg, store, err := openBucketStore(g)
	if err != nil {
		return err
	}
	defer store.Close()
	now := time.Now()
	result, err := daemon.RearmBucket(context.Background(), cfg, store, daemon.RearmRequest{
		Key: domain.ParseBucketKey(key), Actor: actorName(), Reason: *reason, Force: *force,
	}, now)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(result)
	}
	fmt.Printf("%s: %s at %.0f%% -> normal at %.0f%%\n", result.Before.Key, result.Before.Phase, result.Before.UsedPercent, result.After.UsedPercent)
	fmt.Printf("  before: stopped %s, recovered %s, %s\n", optionalRelative(result.Before.StoppedAt, now), optionalRelative(result.Before.RecoveredAt, now), resetText(result.Before.ResetsAt, now))
	fmt.Printf("  after:  recovered %s; the next reading decides again\n", result.After.RecoveredAt.Local().Format(time.RFC3339))
	fmt.Printf("  recorded: %s\n", result.Action.Detail)
	if result.ResumeEligible {
		fmt.Printf("  paused attempts on this bucket may resume after %s\n", cfg.Resume.ResetSettleDelay.D())
	} else {
		fmt.Printf("  paused attempts will not resume until a reading below %.0f%% (resume.below_percent) and %.0f%% (policy.warn_percent) lands; the rearm still reopens the bucket\n",
			result.ResumeBlockedBy.BelowPercent, result.ResumeBlockedBy.WarnPercent)
	}
	return nil
}

// actorName is user@host, for the action record.
func actorName() string {
	name := os.Getenv("USER")
	if u, err := user.Current(); err == nil && u.Username != "" {
		name = u.Username
	}
	if name == "" {
		name = "operator"
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "localhost"
	}
	return name + "@" + host
}

func optionalRelative(t *time.Time, now time.Time) string {
	if t == nil {
		return "never"
	}
	return relative(*t, now)
}

func resetText(t *time.Time, now time.Time) string {
	if t == nil {
		return "no reset time"
	}
	return fmt.Sprintf("resets %s (%s)", t.Local().Format("2006-01-02 15:04"), relative(*t, now))
}

func thresholdText(t *domain.ThresholdSet) string {
	if t == nil {
		return "not recorded"
	}
	return t.String()
}
