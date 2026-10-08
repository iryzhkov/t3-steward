package campaign

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

// CompileFormat is the plan format "campaign compile" reads, named by the
// plan's own compile: key so that a later format is refused by name rather
// than misread.
const CompileFormat = "v1"

// CompileTemplateImplementReview is the one template of the skeleton: an
// implement task that declares its commit, and a review task that consumes
// it.
const CompileTemplateImplementReview = "implement-review"

// Mount paths a compiled prompt names. They are fixed at authoring time: a
// campaign input is mounted at .t3/inputs/<declared path>, and a dependency's
// artifacts under .t3/dependencies/<producer task id>/, whose id is assigned
// at submission and so is listed rather than named.
const (
	compiledPlanInput      = "inputs/plan.md"
	compiledUnitInput      = "inputs/unit.md"
	compiledCommitName     = "implementation"
	compiledDependencyPath = ".t3/dependencies/"
)

//go:embed compile_templates/*.md.tmpl
var compileTemplateFiles embed.FS

var compileTemplates = template.Must(template.New("compile").Option("missingkey=error").
	ParseFS(compileTemplateFiles, "compile_templates/*.md.tmpl"))

// compileRefPattern is a full commit id. A branch or tag is refused because it
// moves, and every unit of a wave has to start from the same commit.
var compileRefPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// compileUnitIDPattern is a unit id. It becomes a directory name and the
// workflow name, so it has to satisfy both: lower-case letters, digits and
// single hyphens, starting with a letter.
var compileUnitIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)

// MaxCompileUnitIDLength bounds a unit id. The temporary siblings a unit is
// written through add about thirty bytes to it, and a name past the file
// system's limit would otherwise be refused only part-way through a wave.
const MaxCompileUnitIDLength = 64

// CompilePlanError is one refusal of a plan, at the plan line it concerns.
type CompilePlanError struct {
	Source string
	Line   int
	Reason string
}

func (e *CompilePlanError) Error() string {
	return fmt.Sprintf("%s:%d: %s", e.Source, e.Line, e.Reason)
}

// CompilePlan is a parsed compile/v1 plan with every unit rendered.
type CompilePlan struct {
	Project  string
	Ref      string
	Class    domain.TaskClass
	Template string
	Units    []CompiledUnit
}

// CompiledUnit is one unit's campaign directory, held in memory until it is
// written, so that a plan any unit of which is refused writes nothing.
type CompiledUnit struct {
	ID      string
	Title   string
	Section string
	// Line is the plan line that declares the unit.
	Line  int
	Files []CompiledFile
}

// CompiledFile is one file of a compiled unit, by its slash-separated path
// relative to the unit directory.
type CompiledFile struct {
	Path    string
	Content []byte
}

type compileFrontMatter struct {
	Compile  string            `yaml:"compile"`
	Project  string            `yaml:"project"`
	Ref      string            `yaml:"ref"`
	Class    string            `yaml:"class"`
	Template string            `yaml:"template"`
	Routes   compileRoutes     `yaml:"routes"`
	Verify   []string          `yaml:"verify"`
	Units    []compileUnitSpec `yaml:"units"`
}

type compileRoutes struct {
	Execute *compileRoute `yaml:"execute"`
	Review  *compileRoute `yaml:"review"`
}

type compileRoute struct {
	Instance  string `yaml:"instance"`
	Model     string `yaml:"model"`
	QuotaPool string `yaml:"quota_pool"`
	Effort    string `yaml:"effort"`
}

type compileUnitSpec struct {
	ID      string `yaml:"id"`
	Title   string `yaml:"title"`
	Section string `yaml:"section"`
}

// compileObjectNames names each front-matter shape as the author wrote it,
// in place of the Go type the decoder reports.
var compileObjectNames = strings.NewReplacer(
	"in type campaign.compileFrontMatter", "in the front matter",
	"in type campaign.compileRoutes", "in routes",
	"in type campaign.compileRoute", "in a route",
	"in type campaign.compileUnitSpec", "in a unit",
	"campaign.compileFrontMatter", "the front matter, which must be a mapping",
	"campaign.compileRoutes", "routes, which must be a mapping",
	"campaign.compileUnitSpec", "a unit",
	"campaign.compileRoute", "a route",
)

var yamlLinePattern = regexp.MustCompile(`line (\d+): (.*)$`)

// ParseCompilePlan reads a compile/v1 plan and renders every unit. It writes
// nothing; every refusal is a *CompilePlanError naming the plan line. source
// is how refusals name the plan, normally its path.
func ParseCompilePlan(source string, raw []byte) (CompilePlan, error) {
	refuse := func(line int, format string, args ...any) error {
		return &CompilePlanError{Source: source, Line: line, Reason: fmt.Sprintf(format, args...)}
	}
	lines := splitPlanLines(raw)
	if len(lines) == 0 || planLineText(raw, lines[0]) != "---" {
		return CompilePlan{}, refuse(1, "the plan has no YAML front matter: its first line must be --- (format compile: %s)", CompileFormat)
	}
	closing := -1
	for index := 1; index < len(lines); index++ {
		if planLineText(raw, lines[index]) == "---" {
			closing = index
			break
		}
	}
	if closing < 0 {
		return CompilePlan{}, refuse(1, "the front matter opened on line 1 has no closing --- line")
	}
	// One leading newline stands for the opening --- line, so the decoder's
	// line numbers are the plan's own.
	frontMatter := append([]byte("\n"), raw[lines[1].start:lines[closing].start]...)
	var header compileFrontMatter
	decoder := yaml.NewDecoder(bytes.NewReader(frontMatter))
	decoder.KnownFields(true)
	if err := decoder.Decode(&header); err != nil {
		if errors.Is(err, io.EOF) {
			return CompilePlan{}, refuse(1, "the front matter is empty; it must say compile: %s", CompileFormat)
		}
		return CompilePlan{}, compileDecodeError(source, err)
	}
	// The front matter is exactly one YAML document. Whatever follows an
	// explicit document end (... or a --- with content) would otherwise be
	// dropped without the strict decoding above ever seeing it.
	const secondDocument = "the front matter continues after its YAML document ends; it must be a single YAML document between the --- lines"
	var trailing yaml.Node
	switch err := decoder.Decode(&trailing); {
	case errors.Is(err, io.EOF):
	case err != nil:
		var refusal *CompilePlanError
		if errors.As(compileDecodeError(source, err), &refusal) {
			refusal.Reason = secondDocument + ": " + refusal.Reason
			return CompilePlan{}, refusal
		}
		return CompilePlan{}, compileDecodeError(source, err)
	default:
		// The document node starts at its --- line; an empty document's
		// content node sits wherever the parser read next.
		line := trailing.Line
		if line == 0 && len(trailing.Content) > 0 {
			line = trailing.Content[0].Line
		}
		return CompilePlan{}, refuse(line, "%s", secondDocument)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(frontMatter, &document); err != nil {
		return CompilePlan{}, compileDecodeError(source, err)
	}
	var root *yaml.Node
	if len(document.Content) == 1 && document.Content[0].Kind == yaml.MappingNode {
		root = document.Content[0]
	}
	// lineOf is the line of the key a path ends in, or 1, the opening ---, for
	// a key the front matter does not have.
	lineOf := func(node *yaml.Node, path ...string) int {
		for _, key := range path[:len(path)-1] {
			node = mappingValue(node, key)
		}
		if key := mappingKey(node, path[len(path)-1]); key != nil {
			return key.Line
		}
		return 1
	}

	switch {
	case strings.TrimSpace(header.Compile) == "":
		return CompilePlan{}, refuse(1, "the front matter does not say compile: %s, the only plan format this release reads", CompileFormat)
	case header.Compile != CompileFormat:
		return CompilePlan{}, refuse(lineOf(root, "compile"), "compile: %s is not a plan format this release reads; it reads compile: %s", header.Compile, CompileFormat)
	case strings.TrimSpace(header.Project) == "":
		return CompilePlan{}, refuse(lineOf(root, "project"), "project is required: the catalog project every unit runs in")
	case header.Ref == "":
		return CompilePlan{}, refuse(1, "ref is required: the full 40-character commit every unit is pinned to")
	case !compileRefPattern.MatchString(header.Ref):
		return CompilePlan{}, refuse(lineOf(root, "ref"), "ref %q is not a full 40-character commit id; a branch, tag or short id is refused so that every unit is pinned to one commit", header.Ref)
	}
	class := domain.TaskClass(header.Class)
	switch class {
	case "":
		class = domain.TaskClassSurplus
	case domain.TaskClassRequired, domain.TaskClassSurplus:
	default:
		return CompilePlan{}, refuse(lineOf(root, "class"), "class %q is neither required nor surplus", header.Class)
	}
	templateName := header.Template
	if templateName == "" {
		templateName = CompileTemplateImplementReview
	}
	if templateName != CompileTemplateImplementReview {
		return CompilePlan{}, refuse(lineOf(root, "template"), "template %q is not available; this release compiles %s only", header.Template, CompileTemplateImplementReview)
	}
	for _, role := range []struct {
		name  string
		route *compileRoute
	}{{"execute", header.Routes.Execute}, {"review", header.Routes.Review}} {
		if role.route == nil {
			return CompilePlan{}, refuse(lineOf(root, "routes"), "routes.%s is required: {instance, model, quota_pool, effort}", role.name)
		}
		line := lineOf(root, "routes", role.name)
		if strings.TrimSpace(role.route.Instance) == "" {
			return CompilePlan{}, refuse(line, "routes.%s needs an instance", role.name)
		}
		if strings.TrimSpace(role.route.Model) == "" {
			return CompilePlan{}, refuse(line, "routes.%s needs a model", role.name)
		}
	}
	verifyNode := mappingValue(root, "verify")
	for index, command := range header.Verify {
		if strings.TrimSpace(command) == "" {
			line := lineOf(root, "verify")
			if verifyNode != nil && index < len(verifyNode.Content) {
				line = verifyNode.Content[index].Line
			}
			return CompilePlan{}, refuse(line, "verify command %d is empty", index+1)
		}
	}
	if len(header.Units) == 0 {
		return CompilePlan{}, refuse(lineOf(root, "units"), "the plan declares no units")
	}

	body := raw[lines[closing].end:]
	headings := planHeadings(body, closing+2)
	plan := CompilePlan{Project: header.Project, Ref: header.Ref, Class: class, Template: templateName}
	unitsNode := mappingValue(root, "units")
	declared := map[string]int{}
	for index, spec := range header.Units {
		line := lineOf(root, "units")
		if unitsNode != nil && index < len(unitsNode.Content) {
			line = unitsNode.Content[index].Line
		}
		switch {
		case spec.ID == "":
			return CompilePlan{}, refuse(line, "unit %d has no id", index+1)
		case !compileUnitIDPattern.MatchString(spec.ID):
			return CompilePlan{}, refuse(line, "unit id %q is not lower-case letters, digits and single hyphens starting with a letter; it becomes a directory and workflow name", spec.ID)
		case len(spec.ID) > MaxCompileUnitIDLength:
			return CompilePlan{}, refuse(line, "unit id %q is %d bytes; a unit id is at most %d", spec.ID, len(spec.ID), MaxCompileUnitIDLength)
		case declared[spec.ID] != 0:
			return CompilePlan{}, refuse(line, "duplicate unit id %q; it is first declared on line %d", spec.ID, declared[spec.ID])
		case strings.TrimSpace(spec.Section) == "":
			return CompilePlan{}, refuse(line, "unit %q has no section: the heading whose section becomes its brief", spec.ID)
		}
		declared[spec.ID] = line
		var matches []planHeading
		for _, heading := range headings {
			if headingMatches(heading.text, spec.Section) {
				matches = append(matches, heading)
			}
		}
		switch len(matches) {
		case 0:
			return CompilePlan{}, refuse(line, "unit %q: section %q matches no heading in the plan body", spec.ID, spec.Section)
		case 1:
		default:
			at := make([]string, 0, len(matches))
			for _, heading := range matches {
				at = append(at, strconv.Itoa(heading.line))
			}
			return CompilePlan{}, refuse(line, "unit %q: section %q matches %d headings, on lines %s; name exactly one",
				spec.ID, spec.Section, len(matches), joinAnd(at))
		}
		unit, err := renderCompiledUnit(plan, header, spec, line, raw, body[matches[0].start:sectionEnd(body, headings, matches[0])])
		if err != nil {
			return CompilePlan{}, refuse(line, "unit %q: %v", spec.ID, err)
		}
		plan.Units = append(plan.Units, unit)
	}
	return plan, nil
}

// compileDecodeError turns a decoder refusal into a refusal at its line.
func compileDecodeError(source string, err error) error {
	message := err.Error()
	var typeErr *yaml.TypeError
	if errors.As(err, &typeErr) && len(typeErr.Errors) > 0 {
		message = typeErr.Errors[0]
	}
	message = compileObjectNames.Replace(message)
	if match := yamlLinePattern.FindStringSubmatch(message); match != nil {
		if line, convErr := strconv.Atoi(match[1]); convErr == nil {
			return &CompilePlanError{Source: source, Line: line, Reason: match[2]}
		}
	}
	return &CompilePlanError{Source: source, Line: 1, Reason: strings.TrimPrefix(message, "yaml: ")}
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			return node.Content[index+1]
		}
	}
	return nil
}

func mappingKey(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			return node.Content[index]
		}
	}
	return nil
}

func joinAnd(values []string) string {
	if len(values) < 2 {
		return strings.Join(values, "")
	}
	return strings.Join(values[:len(values)-1], ", ") + " and " + values[len(values)-1]
}

// planLine is one line of the plan by byte offsets; end includes the newline.
type planLine struct{ start, end int }

func splitPlanLines(raw []byte) []planLine {
	var lines []planLine
	for start := 0; start < len(raw); {
		end := bytes.IndexByte(raw[start:], '\n')
		if end < 0 {
			lines = append(lines, planLine{start, len(raw)})
			break
		}
		lines = append(lines, planLine{start, start + end + 1})
		start += end + 1
	}
	return lines
}

func planLineText(raw []byte, line planLine) string {
	return strings.TrimRight(string(raw[line.start:line.end]), "\r\n")
}

// planHeading is one ATX heading of the plan body.
type planHeading struct {
	level int
	text  string
	// start is the heading's byte offset in the body, line its plan line.
	start int
	line  int
}

// planHeadings lists the ATX headings of a Markdown body, skipping fenced code
// blocks, where a line that starts with # is not a heading. firstLine is the
// plan line of the body's first line.
func planHeadings(body []byte, firstLine int) []planHeading {
	var headings []planHeading
	fence := ""
	for index, line := range splitPlanLines(body) {
		text := planLineText(body, line)
		indented := strings.TrimLeft(text, " ")
		if len(text)-len(indented) > 3 {
			continue
		}
		if fence != "" {
			if strings.HasPrefix(indented, fence) && strings.Trim(indented, fence[:1]+" \t") == "" {
				fence = ""
			}
			continue
		}
		if marker := fenceMarker(indented); marker != "" {
			fence = marker
			continue
		}
		level := len(indented) - len(strings.TrimLeft(indented, "#"))
		if level < 1 || level > 6 {
			continue
		}
		rest := indented[level:]
		if rest != "" && rest[0] != ' ' && rest[0] != '\t' {
			continue
		}
		heading := strings.TrimSpace(rest)
		if trimmed := strings.TrimRight(heading, "#"); trimmed == "" || strings.HasSuffix(trimmed, " ") {
			heading = strings.TrimSpace(trimmed)
		}
		headings = append(headings, planHeading{level: level, text: heading, start: line.start, line: firstLine + index})
	}
	return headings
}

// fenceMarker returns the run of backticks or tildes that opens a fenced code
// block on this line, or "".
func fenceMarker(line string) string {
	for _, char := range []string{"`", "~"} {
		if strings.HasPrefix(line, char+char+char) {
			return line[:len(line)-len(strings.TrimLeft(line, char))]
		}
	}
	return ""
}

// headingMatches reports whether a heading names a section: its text is the
// section, or begins with it followed by a space or a colon, so that
// "C-1: compile skeleton" is section C-1 and "C-10" is not.
func headingMatches(text, section string) bool {
	if text == section {
		return true
	}
	rest, found := strings.CutPrefix(text, section)
	return found && rest != "" && (rest[0] == ' ' || rest[0] == '\t' || rest[0] == ':')
}

// sectionEnd is where a heading's section ends: at the next heading of the
// same or a higher level, or at the end of the body.
func sectionEnd(body []byte, headings []planHeading, heading planHeading) int {
	for _, next := range headings {
		if next.start > heading.start && next.level <= heading.level {
			return next.start
		}
	}
	return len(body)
}

// compilePromptData is what the prompt templates may name.
type compilePromptData struct {
	UnitID           string
	Title            string
	Section          string
	Project          string
	Ref              string
	PlanPath         string
	UnitPath         string
	DependenciesPath string
	CommitName       string
	Verify           []string
}

func renderCompiledUnit(plan CompilePlan, header compileFrontMatter, spec compileUnitSpec, line int, raw, section []byte) (CompiledUnit, error) {
	route := func(declared compileRoute) []backlog.ManifestRoute {
		converted := backlog.ManifestRoute{Instance: declared.Instance, Model: declared.Model, QuotaPool: declared.QuotaPool}
		if declared.Effort != "" {
			converted.Options = map[string]string{"effort": declared.Effort}
		}
		return []backlog.ManifestRoute{converted}
	}
	manifest := backlog.Manifest{
		Version: backlog.ManifestVersion,
		Name:    spec.ID,
		Class:   plan.Class,
		Environment: backlog.ManifestEnvironment{
			Project: plan.Project, Type: backlog.EnvironmentGit,
			Scope: backlog.EnvironmentScopeTask, Ref: plan.Ref,
		},
		Inputs: []string{compiledPlanInput, compiledUnitInput},
		Tasks: map[string]backlog.ManifestTask{
			"implement": {
				PromptFile: "prompts/implement.md",
				Outputs:    []string{"continuation.md", "handoff.md"},
				Commits:    []backlog.ManifestCommit{{Name: compiledCommitName, Revision: "HEAD"}},
				Verify:     append([]string(nil), header.Verify...),
				Routes:     route(*header.Routes.Execute),
			},
			"review": {
				PromptFile: "prompts/review.md",
				Needs:      backlog.ManifestNeeds{"implement"},
				InputsFrom: map[string][]string{"implement": {compiledCommitName, "handoff.md"}},
				Outputs:    []string{"continuation.md", "review.md"},
				// The coordinator records review.md's first line, which the
				// review prompt fixes, as the run's verdict.
				ReviewOutput: &domain.ReviewOutput{VerdictLine: "review.md"},
				Routes:       route(*header.Routes.Review),
			},
		},
	}
	workflow, err := yaml.Marshal(manifest)
	if err != nil {
		return CompiledUnit{}, err
	}
	// The manifest is checked here, before anything is written, so that a plan
	// whose routes or verify commands the workflow format refuses is refused
	// as a plan.
	if _, err := backlog.ParseManifest(workflow); err != nil {
		return CompiledUnit{}, fmt.Errorf("compiles to a workflow this release refuses: %w", err)
	}
	title := spec.Title
	if strings.TrimSpace(title) == "" {
		title = spec.Section
	}
	data := compilePromptData{
		UnitID: spec.ID, Title: title, Section: spec.Section,
		Project: plan.Project, Ref: plan.Ref,
		PlanPath:         ".t3/inputs/" + compiledPlanInput,
		UnitPath:         ".t3/inputs/" + compiledUnitInput,
		DependenciesPath: compiledDependencyPath,
		CommitName:       compiledCommitName,
		Verify:           header.Verify,
	}
	unit := CompiledUnit{ID: spec.ID, Title: title, Section: spec.Section, Line: line}
	unit.Files = append(unit.Files,
		CompiledFile{Path: ManifestFileName, Content: workflow},
		CompiledFile{Path: compiledPlanInput, Content: append([]byte(nil), raw...)},
		CompiledFile{Path: compiledUnitInput, Content: append([]byte(nil), section...)},
	)
	for _, name := range []string{"implement", "review"} {
		var prompt bytes.Buffer
		if err := compileTemplates.ExecuteTemplate(&prompt, name+".md.tmpl", data); err != nil {
			return CompiledUnit{}, err
		}
		unit.Files = append(unit.Files, CompiledFile{Path: "prompts/" + name + ".md", Content: prompt.Bytes()})
	}
	return unit, nil
}

// ErrCompileLeftover refuses to compile a unit beside the temporary siblings
// of an earlier compile of it that never finished.
var ErrCompileLeftover = errors.New("an earlier compile of this unit did not finish")

// CompileLeftovers lists the temporary siblings an interrupted compile of one
// unit left under out: a staging directory, or the retired copy of a unit a
// forced compile was replacing when it stopped. A retired copy may be the only
// copy of the previous unit, so nothing here deletes them; the caller refuses
// and names them.
func CompileLeftovers(out, id string) ([]string, error) {
	entries, err := os.ReadDir(out)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var leftovers []string
	for _, entry := range entries {
		rest, found := strings.CutPrefix(entry.Name(), ".compile-"+id+"-")
		if !found {
			continue
		}
		rest = strings.TrimPrefix(rest, "retired-")
		if rest != "" && strings.Trim(rest, "0123456789") == "" {
			leftovers = append(leftovers, filepath.Join(out, entry.Name()))
		}
	}
	return leftovers, nil
}

// CompileLeftoverError is the refusal of a unit with leftovers.
func CompileLeftoverError(id string, leftovers []string) error {
	return fmt.Errorf("%w: unit %s has %s. A -retired- directory holds the unit a forced compile was replacing; "+
		"move its %s back into place if it is wanted, then remove these directories and compile again",
		ErrCompileLeftover, id, strings.Join(leftovers, ", "), id)
}

// ErrCompiledUnitExists refuses to overwrite a unit directory without force.
var ErrCompiledUnitExists = errors.New("the unit directory already exists")

// WriteOptions controls WriteCompiledUnit.
type WriteOptions struct {
	// Force replaces an existing unit directory.
	Force bool
	// WriteFile writes one file; nil is os.WriteFile with mode 0644. It is a
	// seam so that a test can fail a write part-way through a unit.
	WriteFile func(path string, content []byte) error
}

// WriteCompiledUnit writes one unit under out/<id> and returns that
// directory. The unit is written into a temporary sibling first and renamed
// into place, so a failure part-way leaves no half-written unit, and a failed
// forced write leaves the directory it would have replaced as it was. A
// process killed part-way leaves its temporary siblings behind instead, and
// the next compile of that unit is refused until they are dealt with (see
// CompileLeftovers), so a retired copy is never silently orphaned.
func WriteCompiledUnit(out string, unit CompiledUnit, options WriteOptions) (string, error) {
	if !compileUnitIDPattern.MatchString(unit.ID) || len(unit.ID) > MaxCompileUnitIDLength {
		return "", fmt.Errorf("unit id %q is not a directory name", unit.ID)
	}
	target := filepath.Join(out, unit.ID)
	if leftovers, err := CompileLeftovers(out, unit.ID); err != nil {
		return target, err
	} else if len(leftovers) > 0 {
		return target, CompileLeftoverError(unit.ID, leftovers)
	}
	exists := false
	if _, err := os.Lstat(target); err == nil {
		if !options.Force {
			return target, fmt.Errorf("%s: %w; pass --force to replace it", target, ErrCompiledUnitExists)
		}
		exists = true
	} else if !os.IsNotExist(err) {
		return target, err
	}
	writeFile := options.WriteFile
	if writeFile == nil {
		writeFile = func(path string, content []byte) error { return os.WriteFile(path, content, 0o644) }
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return target, err
	}
	staging, err := os.MkdirTemp(out, ".compile-"+unit.ID+"-")
	if err != nil {
		return target, err
	}
	defer os.RemoveAll(staging)
	for _, file := range unit.Files {
		path := filepath.Join(staging, filepath.FromSlash(file.Path))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return target, err
		}
		if err := writeFile(path, file.Content); err != nil {
			return target, fmt.Errorf("write %s: %w", file.Path, err)
		}
	}
	// MkdirTemp creates the directory owner-only; a compiled unit is as
	// readable as the directories beside it.
	if err := os.Chmod(staging, 0o755); err != nil {
		return target, err
	}
	if !exists {
		return target, os.Rename(staging, target)
	}
	retired, err := os.MkdirTemp(out, ".compile-"+unit.ID+"-retired-")
	if err != nil {
		return target, err
	}
	old := filepath.Join(retired, unit.ID)
	if err := os.Rename(target, old); err != nil {
		os.RemoveAll(retired)
		return target, err
	}
	if err := os.Rename(staging, target); err != nil {
		if restoreErr := os.Rename(old, target); restoreErr != nil {
			// The previous unit is left where it is rather than deleted.
			return target, fmt.Errorf("%w; the previous unit could not be restored and is kept at %s: %v", err, old, restoreErr)
		}
		os.RemoveAll(retired)
		return target, err
	}
	// The new unit is in place; a retired copy that cannot be removed is
	// clutter, not a failed compile.
	os.RemoveAll(retired)
	return target, nil
}
