package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The worker's schedule and the coordinator's maximum load from YAML; an
// absent block means the defaults.
func TestProviderResumeLoadsAndDefaults(t *testing.T) {
	if schedule := Default().BacklogV2.ProviderResume.Schedule(); schedule != nil {
		t.Fatalf("default schedule = %v, want nil for the built-in default", schedule)
	}
	if resumes, delay := Default().BacklogV2.ProviderResume.Maximum(); resumes != domain.DefaultProviderResumeMax || delay != domain.MaxProviderResumeDelay {
		t.Fatalf("default maximum = %d %s", resumes, delay)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "backlog_v2:\n  provider_resume:\n    backoff: [30s, 2m]\n    max_resumes: 0\n    max_delay: 10m\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if schedule := loaded.BacklogV2.ProviderResume.Schedule(); len(schedule) != 2 || schedule[0] != 30*time.Second || schedule[1] != 2*time.Minute {
		t.Fatalf("schedule = %v", schedule)
	}
	if resumes, delay := loaded.BacklogV2.ProviderResume.Maximum(); resumes != 0 || delay != 10*time.Minute {
		t.Fatalf("maximum = %d %s; an explicit zero must disable resuming", resumes, delay)
	}
}

// A schedule or a maximum outside the ceilings is refused by name.
func TestProviderResumeRefusesValuesOutsideTheCeilings(t *testing.T) {
	tooMany := 11
	negative := -1
	for name, resume := range map[string]V2ProviderResume{
		"zero delay":          {Backoff: []Duration{0}},
		"delay over ceiling":  {Backoff: []Duration{Duration(7 * time.Hour)}},
		"too many delays":     {Backoff: make([]Duration, 11)},
		"too many resumes":    {MaxResumes: &tooMany},
		"negative resumes":    {MaxResumes: &negative},
		"max delay too large": {MaxDelay: Duration(7 * time.Hour)},
	} {
		cfg := validBacklogV2Config(t)
		cfg.BacklogV2.ProviderResume = resume
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "backlog_v2.provider_resume") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
