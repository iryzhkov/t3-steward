package backlog

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Defaults for a declared supervision block. They exist so a minimal block is
// still a complete configuration: every one of them is durable and bounded, and
// an author who wants a different bound writes it. The values are the ones the
// implementation plan used in its illustrative shape.
const (
	DefaultSupervisionMaxActivations        = 12
	DefaultSupervisionMaxTurnsPerActivation = 3
	DefaultSupervisionActivationDeadline    = 2 * time.Hour
)

// ManifestSupervision declares the optional campaign overseer. Its absence is
// the unsupervised case, which is exactly the behaviour of every campaign that
// exists today: no supervision record, no gates, no overseer session.
//
// The route is one route rather than a list. Alternatives would make the
// effective overseer route ambiguous, and "independently configured" is the
// requirement the plan states: a stronger model is an operator's choice, never a
// scheduling privilege.
type ManifestSupervision struct {
	Route      ManifestRoute                 `yaml:"route"`
	PromptFile string                        `yaml:"prompt_file"`
	Escalation ManifestSupervisionEscalation `yaml:"escalation"`
	// MaxActivations bounds the whole run and MaxTurnsPerActivation bounds one
	// activation, so a restart cannot spin.
	MaxActivations        int `yaml:"max_activations"`
	MaxTurnsPerActivation int `yaml:"max_turns_per_activation"`
	// ActivationDeadline and IdleEscalationAfter are duration strings, the way
	// every other steward duration is written. An absent idle escalation means
	// no idle escalation.
	ActivationDeadline  time.Duration `yaml:"activation_deadline"`
	IdleEscalationAfter time.Duration `yaml:"idle_escalation_after"`
}

// ManifestSupervisionEscalation reuses the existing notify-thread
// configuration. It adds no messaging integration and no recipient discovery.
type ManifestSupervisionEscalation struct {
	NotifyThread bool   `yaml:"notify_thread"`
	ThreadID     string `yaml:"thread_id"`
}

// ManifestGate is one declared review gate. It is a graph-level object rather
// than a task property: putting gate membership on a task would make every gate
// change a task-definition amendment.
//
// After names the observed producers whose results are reviewed. Before names
// the protected tasks the gate holds, which is empty exactly when Final is true.
type ManifestGate struct {
	After      []string `yaml:"after"`
	Before     []string `yaml:"before"`
	RubricFile string   `yaml:"rubric_file"`
	// Final marks the gate that guards run settlement instead of a downstream
	// task. It is modelled explicitly rather than as a dummy agent task.
	Final bool `yaml:"final"`
}

// GateNames lists the declared gates in one canonical order.
func (m Manifest) GateNames() []string {
	if len(m.Gates) == 0 {
		return nil
	}
	names := make([]string, 0, len(m.Gates))
	for name := range m.Gates {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// SupervisionConfig produces the frozen domain configuration from the declared
// block. The second result is false for an unsupervised manifest, which is the
// absence the domain reads as "every task is admitted".
//
// PromptArtifactID carries the bundle-relative prompt path. That is the prompt's
// identity everything before ingestion has: the coordinator replaces it with the
// retained artifact's ID when it stores the supervision record, exactly as it
// does for a task's prompt_file.
func (m Manifest) SupervisionConfig() (domain.SupervisionConfig, bool) {
	if m.Supervision == nil {
		return domain.SupervisionConfig{}, false
	}
	declared := m.Supervision
	config := domain.SupervisionConfig{
		Route: domain.ProviderRoute{
			WorkerID:           declared.Route.Host,
			ProviderInstanceID: declared.Route.Instance,
			Model:              declared.Route.Model,
			QuotaPoolID:        declared.Route.QuotaPool,
		},
		PromptArtifactID:      declared.PromptFile,
		MaxActivations:        declared.MaxActivations,
		MaxTurnsPerActivation: declared.MaxTurnsPerActivation,
		ActivationDeadline:    declared.ActivationDeadline,
		IdleEscalationAfter:   declared.IdleEscalationAfter,
		Escalation: domain.SupervisionEscalation{
			NotifyThread: declared.Escalation.NotifyThread,
			ThreadID:     declared.Escalation.ThreadID,
		},
	}
	if len(declared.Route.Options) > 0 {
		config.Route.Options = make(map[string]string, len(declared.Route.Options))
		for option, value := range declared.Route.Options {
			config.Route.Options[option] = value
		}
	}
	return config, true
}

// GateDefinitions produces the frozen domain gate definitions, ordered by gate
// name. The declared name is the manifest-level identity; the run-scoped gate ID
// is assigned at ingestion, so a definition built from a manifest alone names
// the gate it was written as.
func (m Manifest) GateDefinitions() []domain.GateDefinition {
	names := m.GateNames()
	if len(names) == 0 {
		return nil
	}
	definitions := make([]domain.GateDefinition, 0, len(names))
	for _, name := range names {
		gate := m.Gates[name]
		definitions = append(definitions, domain.GateDefinition{
			ID:               name,
			Name:             name,
			ObservedTaskIDs:  uniqueSorted(gate.After),
			ProtectedTaskIDs: uniqueSorted(gate.Before),
			RubricArtifactID: gate.RubricFile,
			Final:            gate.Final,
		})
	}
	return definitions
}

func applyManifestSupervisionDefaults(manifest *Manifest) {
	if manifest.Supervision == nil {
		return
	}
	if manifest.Supervision.MaxActivations == 0 {
		manifest.Supervision.MaxActivations = DefaultSupervisionMaxActivations
	}
	if manifest.Supervision.MaxTurnsPerActivation == 0 {
		manifest.Supervision.MaxTurnsPerActivation = DefaultSupervisionMaxTurnsPerActivation
	}
	if manifest.Supervision.ActivationDeadline == 0 {
		manifest.Supervision.ActivationDeadline = DefaultSupervisionActivationDeadline
	}
}

// validateManifestSupervision applies the eleven authoring rules of the
// supervision schema. It runs after the task graph has been validated and found
// acyclic, because gate scopes are resolved against that graph.
func validateManifestSupervision(manifest Manifest, taskNames []string) error {
	// Rule 10: a gate without supervision would be a permanently held campaign
	// that nothing can ever release.
	if len(manifest.Gates) > 0 && manifest.Supervision == nil {
		return errors.New("gates require supervision: a gate with no overseer can never be released")
	}
	if manifest.Supervision == nil {
		return nil
	}
	if err := validateSupervisionBlock(manifest); err != nil {
		return err
	}
	return validateManifestGates(manifest, taskNames)
}

func validateSupervisionBlock(manifest Manifest) error {
	declared := manifest.Supervision
	// Rule 1: exactly one route, checked by the route validator every other
	// route in the manifest goes through.
	if err := validateRoutes("supervision route", []ManifestRoute{declared.Route}); err != nil {
		return err
	}
	if err := validateSupervisionRoutePlacement(manifest); err != nil {
		return err
	}
	// Rule 2: the overseer prompt is a relative bundle path.
	if err := validateRelativePath(declared.PromptFile, false); err != nil {
		return fmt.Errorf("supervision prompt_file: %w", err)
	}
	// Rule 11: the declared budgets are positive and bounded. The bounds are the
	// frozen domain constants, so the manifest and the coordinator agree.
	config, _ := manifest.SupervisionConfig()
	if err := config.Validate(); err != nil {
		return err
	}
	return nil
}

// validateSupervisionRoutePlacement refuses an overseer route pinned to a host
// the workflow's placement excludes. Such a route can never be placed, and a
// campaign whose gates can never be decided is permanently held.
func validateSupervisionRoutePlacement(manifest Manifest) error {
	host := manifest.Supervision.Route.Host
	if host == "" || len(manifest.Placement.Hosts) == 0 {
		return nil
	}
	for _, eligible := range manifest.Placement.Hosts {
		if eligible == host {
			return nil
		}
	}
	return fmt.Errorf("supervision route host %q is not an eligible workflow host", host)
}

func validateManifestGates(manifest Manifest, taskNames []string) error {
	for _, name := range manifest.GateNames() {
		gate := manifest.Gates[name]
		prefix := "gate " + name
		// Rule 3: gate names share the manifest name space with tasks and with
		// the reserved sink, and must collide with neither.
		// The sink is checked before the name pattern: its name does not match the
		// pattern, and "invalid gate name" would be a true but useless answer to an
		// author who tried to gate the sink.
		if name == domain.SinkTaskName {
			return fmt.Errorf("%s uses the reserved sink name", prefix)
		}
		if !manifestNamePattern.MatchString(name) {
			return fmt.Errorf("invalid gate name %q", name)
		}
		if _, taken := manifest.Tasks[name]; taken {
			return fmt.Errorf("%s collides with a task of the same name", prefix)
		}
		if err := validateNonEmptyUnique(prefix+" after", gate.After); err != nil {
			return err
		}
		if err := validateNonEmptyUnique(prefix+" before", gate.Before); err != nil {
			return err
		}
		// Rule 4: every observed and protected name is a declared task of this
		// workflow. A gate does not reach into another run.
		for _, observed := range gate.After {
			if _, ok := manifest.Tasks[observed]; !ok {
				return fmt.Errorf("%s observes missing task %q", prefix, observed)
			}
		}
		for _, protected := range gate.Before {
			if _, ok := manifest.Tasks[protected]; !ok {
				return fmt.Errorf("%s protects missing task %q", prefix, protected)
			}
		}
		if gate.RubricFile != "" {
			// Rule 2 again: a rubric is a relative bundle path.
			if err := validateRelativePath(gate.RubricFile, false); err != nil {
				return fmt.Errorf("%s rubric_file: %w", prefix, err)
			}
		}
		// Rules 5, 6 and the observe-versus-protect overlap of rule 7 are the
		// frozen domain shape check, so the manifest and the coordinator refuse
		// the same definitions.
		definition := domain.GateDefinition{
			ID:               name,
			Name:             name,
			ObservedTaskIDs:  uniqueSorted(gate.After),
			ProtectedTaskIDs: uniqueSorted(gate.Before),
			RubricArtifactID: gate.RubricFile,
			Final:            gate.Final,
		}
		if err := definition.Validate(); err != nil {
			return fmt.Errorf("%s: %w", prefix, err)
		}
	}
	// Rule 7: the task-plus-gate graph is still acyclic.
	if err := validateGateAcyclic(manifest, taskNames); err != nil {
		return err
	}
	// Rule 8: a gate constrains dispatch and cannot substitute for a data or
	// dependency edge.
	return validateGateReachability(manifest)
}

// validateGateAcyclic walks the effective graph: the ordinary dependency edges
// plus, for every gate, an edge from each observed producer into the gate and
// from the gate to each protected task.
func validateGateAcyclic(manifest Manifest, taskNames []string) error {
	const gatePrefix = "gate:"
	successors := make(map[string][]string, len(taskNames)+len(manifest.Gates))
	for _, name := range taskNames {
		for _, dependency := range manifest.Tasks[name].Needs {
			if strings.Contains(dependency, "/") {
				continue
			}
			successors[dependency] = append(successors[dependency], name)
		}
	}
	gateNames := manifest.GateNames()
	for _, name := range gateNames {
		gate := manifest.Gates[name]
		node := gatePrefix + name
		for _, observed := range gate.After {
			successors[observed] = append(successors[observed], node)
		}
		successors[node] = append(successors[node], gate.Before...)
	}

	const (
		unseen = iota
		visiting
		visited
	)
	state := make(map[string]int, len(successors))
	var visit func(string) error
	visit = func(node string) error {
		switch state[node] {
		case visiting:
			return fmt.Errorf("gates introduce a dependency cycle at %q", strings.TrimPrefix(node, gatePrefix))
		case visited:
			return nil
		}
		state[node] = visiting
		for _, next := range successors[node] {
			if err := visit(next); err != nil {
				return err
			}
		}
		state[node] = visited
		return nil
	}
	for _, name := range taskNames {
		if err := visit(name); err != nil {
			return err
		}
	}
	for _, name := range gateNames {
		if err := visit(gatePrefix + name); err != nil {
			return err
		}
	}
	return nil
}

// validateGateReachability refuses a gate whose protected task does not already
// depend on every producer the gate observes. Without that rule a gate would be
// the only thing ordering two tasks, which is a dependency edge written in the
// wrong place: the protected task would receive no artifact, its inputs_from
// would be refused, and a removed gate would silently reorder the run.
func validateGateReachability(manifest Manifest) error {
	ancestors := manifestTaskAncestors(manifest.Tasks)
	for _, name := range manifest.GateNames() {
		gate := manifest.Gates[name]
		for _, protected := range gate.Before {
			for _, observed := range gate.After {
				if !ancestors[protected][observed] {
					return fmt.Errorf(
						"gate %s protects task %q which does not depend on observed task %q; a gate cannot replace a needs edge",
						name, protected, observed)
				}
			}
		}
	}
	return nil
}

// manifestTaskAncestors resolves, for every task, the set of tasks it
// transitively depends on through ordinary needs edges inside this workflow. It
// is called after validateAcyclic, so the recursion terminates.
func manifestTaskAncestors(tasks map[string]ManifestTask) map[string]map[string]bool {
	ancestors := make(map[string]map[string]bool, len(tasks))
	var resolve func(string) map[string]bool
	resolve = func(name string) map[string]bool {
		if known, ok := ancestors[name]; ok {
			return known
		}
		set := map[string]bool{}
		// Publish the set before recursing so a cycle that slipped past the
		// earlier check cannot recurse forever here.
		ancestors[name] = set
		for _, dependency := range tasks[name].Needs {
			if strings.Contains(dependency, "/") {
				continue
			}
			if _, ok := tasks[dependency]; !ok {
				continue
			}
			set[dependency] = true
			for inherited := range resolve(dependency) {
				set[inherited] = true
			}
		}
		return set
	}
	for name := range tasks {
		resolve(name)
	}
	return ancestors
}

// supervisionBundleFiles lists the bundle-relative files the supervision block
// and the gates reference, in one canonical order.
func supervisionBundleFiles(manifest Manifest) []string {
	var files []string
	if manifest.Supervision != nil && manifest.Supervision.PromptFile != "" {
		files = append(files, manifest.Supervision.PromptFile)
	}
	for _, name := range manifest.GateNames() {
		if rubric := manifest.Gates[name].RubricFile; rubric != "" {
			files = append(files, rubric)
		}
	}
	return files
}
