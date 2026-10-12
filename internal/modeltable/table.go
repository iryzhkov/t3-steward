// Package modeltable loads measured model data for future route-policy generation.
// It does not participate in runtime route selection.
package modeltable

import (
	"bytes"
	_ "embed"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/iryzhkov/t3-steward/internal/review"
	"gopkg.in/yaml.v3"
)

//go:embed model-table.yaml
var shipped []byte

const Schema = "model-table/v1"

// Table keeps measurements separate from ordered role preferences. A preference
// need not have a measurement: unmeasured values must never be inferred.
type Table struct {
	Schema  string  `yaml:"schema"`
	Entries []Entry `yaml:"entries"`
	Roles   []Role  `yaml:"roles"`
}

type Candidate struct {
	Route  string `yaml:"route"`
	Effort string `yaml:"effort"`
}

type Entry struct {
	Candidate `yaml:",inline"`
	Quality   *float64 `yaml:"quality"`
	CostUSD   *float64 `yaml:"cost_usd"`
	Source    string   `yaml:"source"`
	Recorded  string   `yaml:"recorded"`
}

type Role struct {
	Name string `yaml:"name"`
	// Floor is an optional quality score in [0, 100]. Nil means no floor.
	Floor      *float64    `yaml:"floor,omitempty"`
	Preference []Candidate `yaml:"preference"`
}

// Load returns a fresh, validated copy of the repository's static table.
func Load() (*Table, error) { return Parse(shipped) }

// Parse decodes exactly one YAML document and rejects unknown fields.
func Parse(raw []byte) (*Table, error) {
	var table Table
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&table); err != nil {
		return nil, fmt.Errorf("invalid model table: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("model table must contain exactly one YAML document")
	}
	if err := table.Validate(); err != nil {
		return nil, err
	}
	return &table, nil
}

// Validate checks the model-table/v1 contract, without catalog authorization or
// routing enforcement. Only low, medium and high are allowed, even for models
// whose installed route policy permits other efforts.
func (t *Table) Validate() error {
	if t.Schema != Schema {
		return fmt.Errorf("unsupported model table schema %q; use %s", t.Schema, Schema)
	}
	if len(t.Entries) == 0 {
		return fmt.Errorf("model table needs at least one entry")
	}
	seenEntries := map[Candidate]bool{}
	for i, e := range t.Entries {
		if err := validCandidate(e.Candidate); err != nil {
			return fmt.Errorf("entry %d: %w", i+1, err)
		}
		if seenEntries[e.Candidate] {
			return fmt.Errorf("duplicate entry %s at %s", e.Route, e.Effort)
		}
		seenEntries[e.Candidate] = true
		for _, metric := range []struct {
			name  string
			value *float64
		}{
			{"quality", e.Quality}, {"cost_usd", e.CostUSD},
		} {
			if metric.value == nil {
				return fmt.Errorf("entry %s at %s: missing %s", e.Route, e.Effort, metric.name)
			}
			if !finite(*metric.value) || *metric.value < 0 {
				return fmt.Errorf("entry %s at %s: %s must be finite and nonnegative", e.Route, e.Effort, metric.name)
			}
		}
		if e.Source == "" {
			return fmt.Errorf("entry %s at %s: source is required", e.Route, e.Effort)
		}
		if _, err := time.Parse("2006-01-02", e.Recorded); err != nil {
			return fmt.Errorf("entry %s at %s: recorded must be a YYYY-MM-DD date", e.Route, e.Effort)
		}
	}
	if len(t.Roles) == 0 {
		return fmt.Errorf("model table needs at least one role")
	}
	seenRoles := map[string]bool{}
	for _, r := range t.Roles {
		if !review.IDPattern.MatchString(r.Name) {
			return fmt.Errorf("invalid role %q", r.Name)
		}
		if seenRoles[r.Name] {
			return fmt.Errorf("duplicate role %q", r.Name)
		}
		seenRoles[r.Name] = true
		if r.Floor != nil && (!finite(*r.Floor) || *r.Floor < 0 || *r.Floor > 100) {
			return fmt.Errorf("role %s: floor must be in [0, 100]", r.Name)
		}
		if len(r.Preference) == 0 {
			return fmt.Errorf("role %s needs at least one preference", r.Name)
		}
		seen := map[Candidate]bool{}
		for _, c := range r.Preference {
			if err := validCandidate(c); err != nil {
				return fmt.Errorf("role %s: %w", r.Name, err)
			}
			if seen[c] {
				return fmt.Errorf("role %s: duplicate preference %s at %s", r.Name, c.Route, c.Effort)
			}
			seen[c] = true
		}
	}
	return nil
}

func validCandidate(c Candidate) error {
	if !review.ValidRoute(c.Route) {
		return fmt.Errorf("malformed route %q; use INSTANCE/MODEL", c.Route)
	}
	switch c.Effort {
	case "low", "medium", "high":
		return nil
	default:
		return fmt.Errorf("effort %q is refused; use low, medium or high", c.Effort)
	}
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
