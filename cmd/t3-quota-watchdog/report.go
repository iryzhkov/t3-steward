package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/config"
	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
	"github.com/iryzhkov/t3-quota-watchdog/internal/report"
	"github.com/iryzhkov/t3-quota-watchdog/internal/source/providerlog"
	"github.com/iryzhkov/t3-quota-watchdog/internal/store/sqlite"
	"github.com/iryzhkov/t3-quota-watchdog/internal/t3api"
)

type reportFlags struct {
	days     int
	bucket   string
	peak     string
	fromLogs bool
	doImport bool
	asJSON   bool
	remotes  string
	local    bool
}

// exportFile is the interchange format between hosts: everything one host
// knows about the period.
type exportFile struct {
	Version      int                    `json:"version"`
	Host         string                 `json:"host"`
	From         time.Time              `json:"from"`
	To           time.Time              `json:"to"`
	Observations []domain.Observation   `json:"observations"`
	Usage        []domain.UsageSample   `json:"usage"`
	Threads      map[string]threadEntry `json:"threads"`
}

type threadEntry struct {
	Title string `json:"title"`
	Model string `json:"model"`
}

// collect gathers this host's observations, usage samples and thread index
// for the period, optionally scanning the provider logs.
func collect(ctx context.Context, cfg config.Config, from, to time.Time, fromLogs, doImport bool) (*exportFile, *sqlite.Store, error) {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return nil, nil, err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return nil, nil, err
	}
	observations, err := store.Observations(ctx, from, to)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	usage, err := store.UsageSamples(ctx, from, to)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	host, _ := os.Hostname()
	out := &exportFile{Version: 1, Host: host, From: from, To: to, Threads: map[string]threadEntry{}}
	for _, o := range observations {
		if o.Model != "" {
			out.Threads[o.ThreadID] = threadEntry{Model: o.Model}
		}
	}
	logger := newLogger("error")
	if client, _, err := connect(cfg, logger); err == nil {
		cctx, cancel := context.WithTimeout(ctx, cfg.T3.RequestTimeout.D())
		if threads, err := fullSnapshot(cctx, client); err == nil {
			for _, t := range threads {
				out.Threads[t.ID] = threadEntry{Title: t.Title, Model: t.Model}
			}
		} else {
			fmt.Fprintf(os.Stderr, "note: thread titles unavailable (%v)\n", err)
		}
		cancel()
	}
	if fromLogs {
		dataDir, err := cfg.ResolveDataDir()
		if err != nil {
			store.Close()
			return nil, nil, err
		}
		scanned, scannedUsage, err := scanProviderLogs(t3api.ProviderLogDir(dataDir), from)
		if err != nil {
			store.Close()
			return nil, nil, err
		}
		observations = mergeObservations(observations, scanned)
		usage = mergeUsage(usage, scannedUsage)
		fmt.Fprintf(os.Stderr, "%s: scanned provider logs: %d observations, %d usage samples\n", host, len(scanned), len(scannedUsage))
	}
	for i := range observations {
		if observations[i].Model == "" {
			observations[i].Model = out.Threads[observations[i].ThreadID].Model
		}
	}
	for i := range usage {
		if usage[i].Model == "" {
			usage[i].Model = out.Threads[usage[i].ThreadID].Model
		}
	}
	if doImport {
		for _, o := range observations {
			_ = store.RecordObservation(ctx, o)
		}
		for _, u := range usage {
			_ = store.RecordUsage(ctx, u)
		}
		fmt.Fprintf(os.Stderr, "%s: imported into %s\n", host, statePath)
	}
	out.Observations = observations
	out.Usage = usage
	return out, store, nil
}

// cmdExport prints this host's data as JSON for another host's report.
func cmdExport(g globalFlags, days int, fromLogs bool) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	now := time.Now()
	data, store, err := collect(context.Background(), cfg, now.Add(-time.Duration(days)*24*time.Hour), now.Add(time.Hour), fromLogs, false)
	if err != nil {
		return err
	}
	defer store.Close()
	enc := json.NewEncoder(os.Stdout)
	return enc.Encode(data)
}

// fetchRemote runs `export` on another host over SSH.
func fetchRemote(ctx context.Context, host string, days int, fromLogs bool) (*exportFile, error) {
	args := fmt.Sprintf("t3-quota-watchdog export --days %d", days)
	if fromLogs {
		args += " --from-logs"
	}
	// A login shell so that ~/.local/bin is on PATH on the remote side.
	cmd := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", host, "bash", "-lc", "'"+args+"'")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s: %v: %s", host, err, strings.TrimSpace(stderr.String()))
	}
	if msg := strings.TrimSpace(stderr.String()); msg != "" {
		fmt.Fprintln(os.Stderr, msg)
	}
	var data exportFile
	if err := json.Unmarshal(stdout.Bytes(), &data); err != nil {
		return nil, fmt.Errorf("%s: decode export: %w", host, err)
	}
	if data.Host == "" {
		data.Host = host
	}
	return &data, nil
}

// cmdReport builds the consumption breakdown from the state database,
// optionally merged with a full scan of the provider logs and with the
// exports of other hosts.
func cmdReport(g globalFlags, f reportFlags) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	if f.peak == "" {
		f.peak = cfg.Report.Peak
	}
	peak, err := report.ParseSchedule(f.peak)
	if err != nil {
		return err
	}
	now := time.Now()
	from := now.Add(-time.Duration(f.days) * 24 * time.Hour)
	ctx := context.Background()

	local, store, err := collect(ctx, cfg, from, now.Add(time.Hour), f.fromLogs, f.doImport)
	if err != nil {
		return err
	}
	defer store.Close()

	sources := []string{local.Host + " (local)"}
	observations := local.Observations
	usage := local.Usage
	titles := map[string]string{}
	for id, t := range local.Threads {
		titles[id] = t.Title
	}
	remotes := cfg.Report.Remotes
	if f.remotes != "" {
		remotes = strings.Split(f.remotes, ",")
	}
	if f.local {
		remotes = nil
	}
	for _, host := range remotes {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		data, err := fetchRemote(rctx, host, f.days, f.fromLogs)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: %v\n", err)
			sources = append(sources, host+" (unavailable)")
			continue
		}
		sources = append(sources, data.Host)
		observations = mergeObservations(observations, data.Observations)
		usage = mergeUsage(usage, data.Usage)
		for id, t := range data.Threads {
			if t.Title != "" {
				titles[id] = t.Title + " @" + data.Host
			}
		}
	}
	if f.bucket != "" {
		var filtered []domain.Observation
		for _, o := range observations {
			if strings.Contains(o.Key.String(), f.bucket) {
				filtered = append(filtered, o)
			}
		}
		observations = filtered
	}
	rep := report.Build(report.Input{
		Observations: observations, Usage: usage, Location: time.Local, Peak: peak,
		From: from, To: now, ThreadTitles: titles,
	})
	rep.Sources = sources
	if f.asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	report.Render(os.Stdout, rep, time.Local)
	return nil
}

// fullSnapshot lists every thread, archived ones included.
func fullSnapshot(ctx context.Context, client *t3api.Client) ([]domain.Thread, error) {
	threads, err := client.ThreadIndex(ctx)
	if err != nil {
		// The full read model can be large; fall back to the live shell.
		snap, serr := client.ShellSnapshot(ctx)
		if serr != nil {
			return nil, err
		}
		threads = snap.Threads
	}
	out := make([]domain.Thread, 0, len(threads))
	for _, t := range threads {
		sel := t.Model()
		out = append(out, domain.Thread{ID: t.ID, Title: t.Title, Model: sel.Model, ProviderInstanceID: sel.InstanceID})
	}
	return out, nil
}

// scanProviderLogs reads every provider log file, rotated backups
// included, and returns the observations and usage samples after from.
func scanProviderLogs(dir string, from time.Time) ([]domain.Observation, []domain.UsageSample, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("read provider log directory: %w", err)
	}
	var obs []domain.Observation
	var usage []domain.UsageSample
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "events.") || !strings.Contains(name, ".log") {
			continue
		}
		snaps, samples, err := providerlog.ScanFile(filepath.Join(dir, name))
		if err != nil {
			fmt.Fprintf(os.Stderr, "note: %s: %v\n", name, err)
			continue
		}
		for _, s := range snaps {
			if s.ObservedAt.Before(from) {
				continue
			}
			obs = append(obs, domain.Observation{
				Key: s.Key, ObservedAt: s.ObservedAt, UsedPercent: s.UsedPercent, ResetsAt: s.ResetsAt,
				EventID: s.SourceEventID, ThreadID: s.ThreadID,
			})
		}
		for _, u := range samples {
			if !u.ObservedAt.Before(from) {
				usage = append(usage, u)
			}
		}
	}
	sort.SliceStable(obs, func(i, j int) bool { return obs[i].ObservedAt.Before(obs[j].ObservedAt) })
	sort.SliceStable(usage, func(i, j int) bool { return usage[i].ObservedAt.Before(usage[j].ObservedAt) })
	return obs, usage, nil
}

func mergeObservations(a, b []domain.Observation) []domain.Observation {
	seen := map[string]bool{}
	out := make([]domain.Observation, 0, len(a)+len(b))
	for _, o := range append(a, b...) {
		k := o.Key.String() + "#" + o.EventID
		if o.EventID != "" && seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, o)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ObservedAt.Before(out[j].ObservedAt) })
	return out
}

func mergeUsage(a, b []domain.UsageSample) []domain.UsageSample {
	seen := map[string]bool{}
	out := make([]domain.UsageSample, 0, len(a)+len(b))
	for _, u := range append(a, b...) {
		if u.SourceEventID != "" && seen[u.SourceEventID] {
			continue
		}
		seen[u.SourceEventID] = true
		out = append(out, u)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ObservedAt.Before(out[j].ObservedAt) })
	return out
}
