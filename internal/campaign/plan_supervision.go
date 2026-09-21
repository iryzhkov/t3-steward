package campaign

import (
	"sort"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Supervision is the declared overseer, projected statically.
//
// It reports the route the overseer will be dispatched on, the bounds the run
// will hold it to, and the events that are expected to wake it. It says nothing
// about whether that route will be healthy or whether its quota pool will have
// room: those are admission decisions taken after submission, exactly as for a
// worker route.
type Supervision struct {
	// Route is the overseer's own route. It is one route, never a list: an
	// alternative would make the effective overseer route ambiguous.
	Route      Route  `json:"route"`
	PromptFile string `json:"promptFile"`
	// DistinctFromTaskRoutes reports that no task in this workflow is routed to
	// the same provider instance and model. The plan requires the overseer to be
	// independently configured, and a reader should be able to see at a glance
	// whether it actually is.
	DistinctFromTaskRoutes bool                  `json:"distinctFromTaskRoutes"`
	MaxActivations         int                   `json:"maxActivations"`
	MaxTurnsPerActivation  int                   `json:"maxTurnsPerActivation"`
	ActivationDeadline     string                `json:"activationDeadline"`
	IdleEscalationAfter    string                `json:"idleEscalationAfter,omitempty"`
	Escalation             SupervisionEscalation `json:"escalation"`
	// Triggers are the events expected to wake the overseer, in a fixed order.
	// Normal brief quota or resource waiting is not among them: it is not a
	// review incident.
	Triggers []string `json:"triggers"`
}

// SupervisionEscalation is where an escalation is delivered. It reuses the
// existing notify-thread configuration only.
type SupervisionEscalation struct {
	NotifyThread bool   `json:"notifyThread,omitempty"`
	ThreadID     string `json:"threadId,omitempty"`
}

// Gate is one projected review boundary.
//
// Observes and Protects are what the manifest declared. ProtectedClosure is the
// answer to the question an operator actually asks: which tasks cannot be
// dispatched until this gate is accepted. That is the protected tasks together
// with their dependency descendants, because a descendant of a held task is held
// by ordinary dependency semantics whether or not the gate names it.
type Gate struct {
	Name     string   `json:"name"`
	Observes []string `json:"observes"`
	Protects []string `json:"protects,omitempty"`
	// ProtectedClosure is empty for a final-settlement gate: it protects no
	// downstream task and guards run settlement instead.
	ProtectedClosure []string `json:"protectedClosure,omitempty"`
	RubricFile       string   `json:"rubricFile,omitempty"`
	// Final marks the gate that guards run settlement. It is modelled
	// explicitly, never as a dummy agent task.
	Final bool `json:"final,omitempty"`
	// ReviewWave is the earliest wave by whose end every observed task may have
	// finished, which is the earliest wave in which this gate can become ready
	// for review. It is not a promise about when the review happens.
	ReviewWave int `json:"reviewWave"`
}

// projectSupervision builds the supervision and gate projection and reports,
// per task, which gates observe it and which gates hold it. It returns nil for
// an unsupervised manifest, which is every manifest that exists today.
func projectSupervision(
	manifest backlog.Manifest,
	tasks []Task,
	depth map[string]int,
	dependents map[string][]string,
) (*Supervision, []Gate, map[string][]string, map[string][]string) {
	config, supervised := manifest.SupervisionConfig()
	if !supervised {
		return nil, nil, nil, nil
	}

	gates := make([]Gate, 0, len(manifest.Gates))
	observedBy := map[string][]string{}
	heldBy := map[string][]string{}
	for _, definition := range manifest.GateDefinitions() {
		gate := Gate{
			Name:             definition.Name,
			Observes:         cloneStrings(definition.ObservedTaskIDs),
			Protects:         cloneStrings(definition.ProtectedTaskIDs),
			ProtectedClosure: dependencyClosure(definition.ProtectedTaskIDs, dependents),
			RubricFile:       definition.RubricArtifactID,
			Final:            definition.Final,
		}
		for _, observed := range gate.Observes {
			if depth[observed] > gate.ReviewWave {
				gate.ReviewWave = depth[observed]
			}
			observedBy[observed] = append(observedBy[observed], gate.Name)
		}
		for _, held := range gate.ProtectedClosure {
			heldBy[held] = append(heldBy[held], gate.Name)
		}
		gates = append(gates, gate)
	}
	sort.SliceStable(gates, func(i, j int) bool {
		if gates[i].ReviewWave != gates[j].ReviewWave {
			return gates[i].ReviewWave < gates[j].ReviewWave
		}
		return gates[i].Name < gates[j].Name
	})
	for name := range observedBy {
		sort.Strings(observedBy[name])
	}
	for name := range heldBy {
		sort.Strings(heldBy[name])
	}

	route := projectRoutes([]backlog.ManifestRoute{manifest.Supervision.Route})[0]
	supervision := &Supervision{
		Route:                  route,
		PromptFile:             config.PromptArtifactID,
		DistinctFromTaskRoutes: routeDistinctFromTasks(route, tasks),
		MaxActivations:         config.MaxActivations,
		MaxTurnsPerActivation:  config.MaxTurnsPerActivation,
		ActivationDeadline:     config.ActivationDeadline.String(),
		Escalation: SupervisionEscalation{
			NotifyThread: config.Escalation.NotifyThread,
			ThreadID:     config.Escalation.ThreadID,
		},
	}
	if config.IdleEscalationAfter > 0 {
		supervision.IdleEscalationAfter = config.IdleEscalationAfter.String()
	}
	supervision.Triggers = supervisionTriggers(gates, supervision)
	return supervision, gates, observedBy, heldBy
}

// dependencyClosure is the protected tasks plus every task reachable from them
// through ordinary dependency edges.
func dependencyClosure(roots []string, dependents map[string][]string) []string {
	if len(roots) == 0 {
		return nil
	}
	covered := make(map[string]bool, len(roots))
	var walk func(string)
	walk = func(name string) {
		if covered[name] {
			return
		}
		covered[name] = true
		for _, dependent := range dependents[name] {
			walk(dependent)
		}
	}
	for _, root := range roots {
		walk(root)
	}
	closure := make([]string, 0, len(covered))
	for name := range covered {
		closure = append(closure, name)
	}
	sort.Strings(closure)
	return closure
}

// routeDistinctFromTasks reports whether the overseer route names a provider
// instance and model that no task route names. Host and options are ignored on
// purpose: what the plan asks to be independent is the provider and the model.
func routeDistinctFromTasks(overseer Route, tasks []Task) bool {
	for _, task := range tasks {
		for _, route := range task.Routes {
			if route.Instance == overseer.Instance && route.Model == overseer.Model {
				return false
			}
		}
	}
	return true
}

// supervisionTriggers is the static list of events expected to wake the
// overseer. It is derived rather than declared, because the trigger set is a
// property of the scheduler, and the plan exists so an author can see it before
// the run starts.
func supervisionTriggers(gates []Gate, supervision *Supervision) []string {
	triggers := make([]string, 0, len(gates)+4)
	for _, gate := range gates {
		if gate.Final {
			triggers = append(triggers, "gate "+gate.Name+" becomes ready for review before run settlement")
			continue
		}
		triggers = append(triggers, "gate "+gate.Name+" becomes ready for review")
	}
	triggers = append(triggers,
		"a task fails, needs input or blocks in a way that requires judgement",
		"a capacity or route block persists past its configured threshold",
		"an operator requests reassessment",
		"a pending review is still undecided after "+supervision.ActivationDeadline,
	)
	return triggers
}

// overseerNodeName is the DOT node the overseer is drawn as. It is not a task
// and cannot be one: the supervision record lives beside the sink, outside the
// worker DAG, so it can observe a failure without waiting behind its own gate.
const overseerNodeName = "__overseer"

// sinkNodeName is the coordinator's terminal sink, named here so the supervision
// renderer can draw a final gate against it without importing the constant
// twice.
var sinkNodeName = domain.SinkTaskName
