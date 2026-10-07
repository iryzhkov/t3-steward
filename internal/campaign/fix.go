package campaign

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

const FixLineageSchema = "steward.fix-lineage/v1"

// WriteFixChain publishes a fully validated private staging directory.
// The sibling reservation serializes writers of this output path; existing
// output paths are refused, including empty directories and symbolic links.
func WriteFixChain(out string, unit CompiledUnit, callerLimits ...Limits) (string, error) {
	limits := DefaultLimits
	if len(callerLimits) > 1 {
		return "", fmt.Errorf("only one set of campaign limits is allowed")
	}
	if len(callerLimits) == 1 {
		limits = callerLimits[0]
	}
	if err := limits.validate(); err != nil {
		return "", err
	}
	target, err := filepath.Abs(out)
	if err != nil {
		return "", err
	}
	seen := map[string]bool{}
	for _, f := range unit.Files {
		p := f.Path
		if p == "" || path.Clean(p) != p || path.IsAbs(p) || strings.Contains(p, "\\") || p == ".." || strings.HasPrefix(p, "../") || seen[p] {
			return target, fmt.Errorf("invalid or duplicate fix path %q", p)
		}
		seen[p] = true
	}
	if !seen[ManifestFileName] {
		return target, fmt.Errorf("fix workflow is missing")
	}
	if _, err = os.Lstat(target); err == nil {
		return target, fmt.Errorf("fix output %s already exists", target)
	} else if !os.IsNotExist(err) {
		return target, err
	}
	parent := filepath.Dir(target)
	if err = os.MkdirAll(parent, 0700); err != nil {
		return target, err
	}
	reservation := target + ".fix-lock"
	if err = os.Mkdir(reservation, 0700); err != nil {
		return target, fmt.Errorf("reserve fix output: %w", err)
	}
	defer os.Remove(reservation)
	staging, err := os.MkdirTemp(parent, ".fix-staging-")
	if err != nil {
		return target, err
	}
	defer os.RemoveAll(staging)
	for _, f := range unit.Files {
		p := filepath.Join(staging, filepath.FromSlash(f.Path))
		if err = os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			return target, err
		}
		if err = os.WriteFile(p, f.Content, 0600); err != nil {
			return target, fmt.Errorf("write %s: %w", f.Path, err)
		}
	}
	if _, err = Prepare(staging, limits); err != nil {
		return target, err
	}
	if _, err = os.Lstat(target); err == nil {
		return target, fmt.Errorf("fix output %s already exists", target)
	} else if !os.IsNotExist(err) {
		return target, err
	}
	return target, os.Rename(staging, target)
}

type FixLineageEntry struct {
	Run              string `json:"run"`
	ReviewTask       string `json:"reviewTask"`
	ReviewedCommit   string `json:"reviewedCommit"`
	Verdict          string `json:"verdict"`
	BlockingFindings int    `json:"blockingFindings"`
}
type FixLineage struct {
	Schema            string            `json:"schema"`
	RootRun           string            `json:"rootRun"`
	RootProducingTask string            `json:"rootProducingTask"`
	RootReviewTask    string            `json:"rootReviewTask"`
	RoundLimit        int               `json:"roundLimit"`
	RoundsUsedBefore  int               `json:"roundsUsedBefore"`
	RoundsDeclared    int               `json:"roundsDeclared"`
	Gate              *domain.TaskGate  `json:"gate"`
	History           []FixLineageEntry `json:"history"`
}

// FixSource is resolved provenance, not a transport or an authority to submit.
type FixSource struct {
	RunID          string
	ProducerRunID  string
	Workflow       domain.Workflow
	Producer       domain.Task
	Review         domain.Task
	ReviewedCommit string
}
type FixOptions struct {
	Name    string
	Lineage FixLineage
	Gate    *domain.TaskGate
	Brief   []CompiledFile
	Context []CompiledFile
}

//go:embed fix_templates/*.tmpl
var fixTemplateFiles embed.FS
var fixTemplates = template.Must(template.New("fix").Option("missingkey=error").ParseFS(fixTemplateFiles, "fix_templates/*.tmpl"))

var fixReservedBasenames = map[string]bool{
	"workflow.yaml": true, "rules.md": true, "lineage.json": true, "prompt.md": true,
	"fix1.md": true, "fix2.md": true, "review2.md": true, "review3.md": true,
}

// ValidateFixContextNames refuses collisions before a caller contacts a coordinator.
func ValidateFixContextNames(paths []string) error {
	seen := map[string]bool{}
	for _, p := range paths {
		base := path.Base(strings.ReplaceAll(p, "\\", "/"))
		if base == "." || base == "/" || base == "" || fixReservedBasenames[base] || seen[base] {
			return fmt.Errorf("fix context basename collision: %q", base)
		}
		seen[base] = true
	}
	return nil
}

// GenerateFixChain renders deterministic files without filesystem or transport effects.
func GenerateFixChain(source FixSource, options FixOptions) (CompiledUnit, error) {
	if source.Workflow.Environment.Type != backlog.EnvironmentGit {
		return CompiledUnit{}, fmt.Errorf("campaign fix requires a git environment")
	}
	if source.RunID == "" || source.Producer.Name == "" || source.Review.Name == "" {
		return CompiledUnit{}, fmt.Errorf("fix source run and task names are required")
	}
	if !compileRefPattern.MatchString(source.ReviewedCommit) {
		return CompiledUnit{}, fmt.Errorf("reviewed commit must be a full commit id")
	}
	lineage := options.Lineage
	if lineage.Schema == "" {
		lineage.Schema = FixLineageSchema
	}
	if lineage.Schema != FixLineageSchema || lineage.RoundLimit < 1 || lineage.RoundLimit > 8 || lineage.RoundsUsedBefore < 0 || lineage.RoundsDeclared < 1 || lineage.RoundsDeclared > 2 || lineage.RoundsUsedBefore+lineage.RoundsDeclared > lineage.RoundLimit {
		return CompiledUnit{}, fmt.Errorf("invalid fix lineage round limits")
	}
	lineage.Gate = options.Gate
	names := make([]string, 0, len(options.Context))
	for _, f := range options.Context {
		names = append(names, f.Path)
	}
	if err := ValidateFixContextNames(names); err != nil {
		return CompiledUnit{}, err
	}
	files := []CompiledFile{}
	seen := map[string]bool{}
	add := func(p string, b []byte) error {
		if p == "" || path.Clean(p) != p || strings.HasPrefix(p, "/") || strings.Contains(p, "\\") || p == ".." || strings.HasPrefix(p, "../") || seen[p] {
			return fmt.Errorf("fix file path collision or invalid path: %q", p)
		}
		seen[p] = true
		files = append(files, CompiledFile{Path: p, Content: append([]byte(nil), b...)})
		return nil
	}
	inputs := []string{"inputs/fix/rules.md", "inputs/fix/lineage.json"}
	for _, f := range options.Brief {
		if !strings.HasPrefix(f.Path, "inputs/fix/brief/") {
			return CompiledUnit{}, fmt.Errorf("brief path %q must be under inputs/fix/brief/", f.Path)
		}
		if err := add(f.Path, f.Content); err != nil {
			return CompiledUnit{}, err
		}
		inputs = append(inputs, f.Path)
	}
	if !seen["inputs/fix/brief/prompt.md"] {
		return CompiledUnit{}, fmt.Errorf("original brief prompt is required")
	}
	for _, f := range options.Context {
		p := "inputs/fix/context/" + path.Base(f.Path)
		if err := add(p, f.Content); err != nil {
			return CompiledUnit{}, err
		}
		inputs = append(inputs, p)
	}
	sort.Strings(inputs)
	producerRun := source.ProducerRunID
	if producerRun == "" {
		producerRun = source.RunID
	}
	producer := producerRun + "/" + source.Producer.Name
	review := source.RunID + "/" + source.Review.Name
	outputs := func(t domain.Task) []string {
		out := make([]string, 0, len(t.Outputs))
		for _, o := range t.Outputs {
			out = append(out, o.Name)
		}
		return out
	}
	routes := func(t domain.Task) []backlog.ManifestRoute {
		out := make([]backlog.ManifestRoute, 0, len(t.Routes))
		for _, r := range t.Routes {
			out = append(out, backlog.ManifestRoute{Host: r.WorkerID, Instance: r.ProviderInstanceID, Model: r.Model, QuotaPool: r.QuotaPoolID, Options: r.Options})
		}
		return out
	}
	fixTask := func(name string, needs []string, from map[string][]string) backlog.ManifestTask {
		return backlog.ManifestTask{PromptFile: "prompts/" + name + ".md", Needs: needs, InputsFrom: from, Outputs: []string{"continuation.md", "handoff.md", "verification.log"}, Commits: []backlog.ManifestCommit{{Name: "fix", Revision: "HEAD"}}, Verify: append(append([]string(nil), source.Producer.Verification...), "git diff --quiet && git diff --cached --quiet"), Routes: routes(source.Producer), Placement: backlog.ManifestPlacement{Hosts: append([]string(nil), source.Producer.Placement.Hosts...)}, Resources: backlog.ManifestResources{Preset: source.Producer.ResourcePreset}, MaxTurns: source.Producer.MaxTurns}
	}
	reviewTask := func(name string, needs []string, from map[string][]string) backlog.ManifestTask {
		return backlog.ManifestTask{PromptFile: "prompts/" + name + ".md", Needs: needs, InputsFrom: from, Outputs: []string{"continuation.md", "review.md", "verdict.json"}, ReviewOutput: &domain.ReviewOutput{Verdict: "verdict.json"}, Verify: []string{"head -1 review.md | grep -Eq '^VERDICT: (ACCEPT|CHANGES_REQUESTED)$'"}, Routes: routes(source.Review), MaxTurns: source.Review.MaxTurns}
	}
	tasks := map[string]backlog.ManifestTask{
		"fix1":    fixTask("fix1", []string{producer, review}, map[string][]string{producer: outputs(source.Producer), review: outputs(source.Review)}),
		"review2": reviewTask("review2", []string{"fix1"}, map[string][]string{"fix1": {"fix", "handoff.md", "verification.log"}}),
	}
	lastFix := "fix1"
	if lineage.RoundsDeclared == 2 {
		tasks["fix2"] = fixTask("fix2", []string{"fix1", "review2"}, map[string][]string{"fix1": {"fix", "handoff.md"}, "review2": {"review.md", "verdict.json"}})
		downstream := []string{"fix", "handoff.md", "verification.log"}
		if options.Gate != nil {
			downstream = append(downstream, "gate/log.txt")
		}
		tasks["review3"] = reviewTask("review3", []string{"fix2", "review2"}, map[string][]string{"fix2": downstream, "review2": {"review.md", "verdict.json"}})
		lastFix = "fix2"
	}
	last := tasks[lastFix]
	last.Gate = options.Gate
	tasks[lastFix] = last
	manifest := backlog.Manifest{Version: backlog.ManifestVersion, Name: options.Name, Class: source.Workflow.Class, Environment: backlog.ManifestEnvironment{Project: source.Workflow.Project, Type: source.Workflow.Environment.Type, Scope: source.Workflow.Environment.Scope, Ref: source.Workflow.Environment.Ref}, Inputs: inputs, Tasks: tasks}
	raw, err := yaml.Marshal(manifest)
	if err != nil {
		return CompiledUnit{}, err
	}
	if _, err = backlog.ParseManifest(raw); err != nil {
		return CompiledUnit{}, fmt.Errorf("generated fix workflow: %w", err)
	}
	if err = add("workflow.yaml", raw); err != nil {
		return CompiledUnit{}, err
	}
	raw, err = json.MarshalIndent(lineage, "", "  ")
	if err != nil {
		return CompiledUnit{}, err
	}
	if err = add("inputs/fix/lineage.json", append(raw, '\n')); err != nil {
		return CompiledUnit{}, err
	}
	render := func(p, t string, data any) error {
		var b bytes.Buffer
		if err := fixTemplates.ExecuteTemplate(&b, t, data); err != nil {
			return err
		}
		return add(p, append(bytes.TrimRight(b.Bytes(), "\n"), '\n'))
	}
	if err = render("inputs/fix/rules.md", "rules.md.tmpl", nil); err != nil {
		return CompiledUnit{}, err
	}
	taskNames := make([]string, 0, len(tasks))
	for n := range tasks {
		taskNames = append(taskNames, n)
	}
	sort.Strings(taskNames)
	for _, n := range taskNames {
		kind := "review.md.tmpl"
		if strings.HasPrefix(n, "fix") {
			kind = "fix.md.tmpl"
		}
		data := struct {
			Name, Commit, LastFix               string
			IsSecondFix, IsFinalReview, HasGate bool
		}{n, source.ReviewedCommit, lastFix, n == "fix2", n == "review3", options.Gate != nil}
		if err = render(tasks[n].PromptFile, kind, data); err != nil {
			return CompiledUnit{}, err
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return CompiledUnit{ID: options.Name, Files: files}, nil
}
