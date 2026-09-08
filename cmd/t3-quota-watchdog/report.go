package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

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
}

// cmdReport builds the consumption breakdown from the state database,
// optionally merged with a full scan of the provider logs.
func cmdReport(g globalFlags, f reportFlags) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	peak, err := report.ParseSchedule(f.peak)
	if err != nil {
		return err
	}
	now := time.Now()
	from := now.Add(-time.Duration(f.days) * 24 * time.Hour)
	ctx := context.Background()

	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return err
	}
	defer store.Close()

	observations, err := store.Observations(ctx, from, now.Add(time.Hour))
	if err != nil {
		return err
	}
	usage, err := store.UsageSamples(ctx, from, now.Add(time.Hour))
	if err != nil {
		return err
	}

	// Thread id to model and title, from the T3 server when reachable.
	models := map[string]string{}
	titles := map[string]string{}
	for _, o := range observations {
		if o.Model != "" {
			models[o.ThreadID] = o.Model
		}
	}
	logger := newLogger("error")
	if client, _, err := connect(cfg, logger); err == nil {
		cctx, cancel := context.WithTimeout(ctx, cfg.T3.RequestTimeout.D())
		if snap, err := fullSnapshot(cctx, client); err == nil {
			for _, t := range snap {
				models[t.ID] = t.Model
				titles[t.ID] = t.Title
			}
		} else {
			fmt.Fprintf(os.Stderr, "note: thread titles unavailable (%v)\n", err)
		}
		cancel()
	}

	if f.fromLogs {
		dataDir, err := cfg.ResolveDataDir()
		if err != nil {
			return err
		}
		scanned, scannedUsage, err := scanProviderLogs(t3api.ProviderLogDir(dataDir), from)
		if err != nil {
			return err
		}
		observations = mergeObservations(observations, scanned)
		usage = mergeUsage(usage, scannedUsage)
		fmt.Fprintf(os.Stderr, "scanned provider logs: %d observations, %d usage samples\n", len(scanned), len(scannedUsage))
	}
	for i := range observations {
		if observations[i].Model == "" {
			observations[i].Model = models[observations[i].ThreadID]
		}
	}
	for i := range usage {
		if usage[i].Model == "" {
			usage[i].Model = models[usage[i].ThreadID]
		}
	}
	if f.doImport {
		n := 0
		for _, o := range observations {
			if err := store.RecordObservation(ctx, o); err == nil {
				n++
			}
		}
		for _, u := range usage {
			_ = store.RecordUsage(ctx, u)
		}
		fmt.Fprintf(os.Stderr, "imported into %s\n", statePath)
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
	if f.asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	report.Render(os.Stdout, rep, time.Local)
	return nil
}

// fullSnapshot lists every thread, archived ones included, from the
// orchestration read model.
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
