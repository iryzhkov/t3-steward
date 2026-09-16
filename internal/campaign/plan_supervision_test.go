package campaign

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// supervisedYAML has a gate whose protected task has a descendant of its own, so
// the projection has something to close over, and a final-settlement gate that
// protects no task at all.
const supervisedYAML = `version: 2
name: supervised
environment:
  project: example-project
routes:
  - instance: workerInstance
    model: worker-model
    quota_pool: worker-pool
supervision:
  route:
    instance: overseerInstance
    model: overseer-model
    quota_pool: overseer-pool
  prompt_file: prompts/overseer.md
  max_activations: 6
  max_turns_per_activation: 2
  activation_deadline: 90m
  idle_escalation_after: 12h
  escalation:
    notify_thread: true
gates:
  review:
    after: [analyze]
    before: [apply]
    rubric_file: rubrics/review.md
  settle:
    after: [publish]
    final: true
tasks:
  analyze:
    prompt_file: prompts/analyze.md
    outputs: [analysis.md]
  apply:
    prompt_file: prompts/apply.md
    needs: [analyze]
    inputs_from:
      analyze: [analysis.md]
    outputs: [applied.md]
  publish:
    prompt_file: prompts/publish.md
    needs: [apply]
    inputs_from:
      apply: [applied.md]
`

func gateByName(t *testing.T, plan Plan, name string) Gate {
	t.Helper()
	for _, gate := range plan.Gates {
		if gate.Name == name {
			return gate
		}
	}
	t.Fatalf("gate %q is missing from the plan", name)
	return Gate{}
}

func TestProjectUnsupervisedPlanSaysNothingAboutSupervision(t *testing.T) {
	plan := project(t, joinYAML)
	if plan.Supervision != nil || len(plan.Gates) != 0 {
		t.Fatalf("an unsupervised manifest must project no overseer: %#v %#v", plan.Supervision, plan.Gates)
	}
	if plan.Totals.Gates != 0 || plan.Totals.HeldTasks != 0 {
		t.Fatalf("totals = %#v", plan.Totals)
	}
	for _, task := range plan.Tasks {
		if len(task.ObservedBy) > 0 || len(task.HeldBy) > 0 {
			t.Fatalf("task %s carries a review boundary it never declared", task.Name)
		}
	}
	encoded, err := RenderJSON(plan)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, absent := range []string{"\"supervision\"", "\"gates\": [", "observedBy", "heldBy"} {
		if strings.Contains(string(encoded), absent) {
			t.Fatalf("the unsupervised document must not carry %q:\n%s", absent, encoded)
		}
	}
}

func TestProjectSupervisionRouteAndBounds(t *testing.T) {
	plan := project(t, supervisedYAML)
	if plan.Supervision == nil {
		t.Fatal("a declared overseer must be projected")
	}
	supervision := *plan.Supervision
	wantRoute := Route{Instance: "overseerInstance", Model: "overseer-model", QuotaPool: "overseer-pool"}
	if !reflect.DeepEqual(supervision.Route, wantRoute) {
		t.Fatalf("overseer route = %#v", supervision.Route)
	}
	// The overseer route is not one of the task routes and the plan says so.
	if !supervision.DistinctFromTaskRoutes {
		t.Fatal("the overseer route differs from every task route and must be reported as distinct")
	}
	for _, task := range plan.Tasks {
		for _, route := range task.Routes {
			if route.Instance == supervision.Route.Instance {
				t.Fatalf("task %s inherited the overseer route", task.Name)
			}
		}
	}
	if supervision.PromptFile != "prompts/overseer.md" {
		t.Fatalf("overseer prompt = %q", supervision.PromptFile)
	}
	if supervision.MaxActivations != 6 || supervision.MaxTurnsPerActivation != 2 {
		t.Fatalf("bounds = %#v", supervision)
	}
	if supervision.ActivationDeadline != "1h30m0s" || supervision.IdleEscalationAfter != "12h0m0s" {
		t.Fatalf("durations = %q / %q", supervision.ActivationDeadline, supervision.IdleEscalationAfter)
	}
	if !supervision.Escalation.NotifyThread {
		t.Fatal("the escalation destination was declared and is not projected")
	}
}

func TestProjectSupervisionTriggersAreDerivedAndOrdered(t *testing.T) {
	plan := project(t, supervisedYAML)
	triggers := plan.Supervision.Triggers
	want := []string{
		"gate review becomes ready for review",
		"gate settle becomes ready for review before run settlement",
		"a task fails, needs input or blocks in a way that requires judgement",
		"a capacity or route block persists past its configured threshold",
		"an operator requests reassessment",
		"a pending review is still undecided after 1h30m0s",
		"the run requests final reporting at settlement",
	}
	if !reflect.DeepEqual(triggers, want) {
		t.Fatalf("triggers = %#v", triggers)
	}
	// Ordinary waiting is not a review incident, so it is not a trigger.
	for _, trigger := range triggers {
		if strings.Contains(trigger, "scheduler tick") || strings.Contains(trigger, "every successful task") {
			t.Fatalf("trigger %q would wake the overseer when no decision is needed", trigger)
		}
	}
}

func TestProjectGatesCloseOverDependencyDescendants(t *testing.T) {
	plan := project(t, supervisedYAML)

	// Gates are ordered by the earliest wave each can become ready in, then by
	// name, which is the order the waves themselves are printed in.
	if len(plan.Gates) != 2 || plan.Gates[0].Name != "review" || plan.Gates[1].Name != "settle" {
		t.Fatalf("gate order = %#v", plan.Gates)
	}
	review := gateByName(t, plan, "review")
	if !reflect.DeepEqual(review.Observes, []string{"analyze"}) || !reflect.DeepEqual(review.Protects, []string{"apply"}) {
		t.Fatalf("review scope = %#v", review)
	}
	// publish is not named by the gate. It is held all the same, because it
	// depends on a held task.
	if !reflect.DeepEqual(review.ProtectedClosure, []string{"apply", "publish"}) {
		t.Fatalf("review closure = %v", review.ProtectedClosure)
	}
	if review.RubricFile != "rubrics/review.md" || review.Final {
		t.Fatalf("review = %#v", review)
	}
	if review.ReviewWave != 0 {
		t.Fatalf("review is ready once wave 0 succeeds, not wave %d", review.ReviewWave)
	}

	settle := gateByName(t, plan, "settle")
	if !settle.Final || len(settle.Protects) != 0 || len(settle.ProtectedClosure) != 0 {
		t.Fatalf("a final-settlement gate holds no downstream task: %#v", settle)
	}
	if settle.ReviewWave != 2 {
		t.Fatalf("settle review wave = %d", settle.ReviewWave)
	}

	analyze := taskByName(t, plan, "analyze")
	if !reflect.DeepEqual(analyze.ObservedBy, []string{"review"}) || len(analyze.HeldBy) != 0 {
		t.Fatalf("analyze = %#v / %#v", analyze.ObservedBy, analyze.HeldBy)
	}
	apply := taskByName(t, plan, "apply")
	if !reflect.DeepEqual(apply.HeldBy, []string{"review"}) {
		t.Fatalf("apply held by %v", apply.HeldBy)
	}
	publish := taskByName(t, plan, "publish")
	if !reflect.DeepEqual(publish.HeldBy, []string{"review"}) {
		t.Fatalf("publish is a descendant of a held task and must be reported held: %v", publish.HeldBy)
	}
	if !reflect.DeepEqual(publish.ObservedBy, []string{"settle"}) {
		t.Fatalf("publish observed by %v", publish.ObservedBy)
	}
	if plan.Totals.Gates != 2 || plan.Totals.HeldTasks != 2 {
		t.Fatalf("totals = %#v", plan.Totals)
	}
}

func TestRenderTextReportsTheReviewBoundaries(t *testing.T) {
	text := RenderText(project(t, supervisedYAML))

	for _, want := range []string{
		"supervision  overseer route overseerInstance/overseer-model, pool overseer-pool",
		"its own route, not a worker route",
		"a provider instance and model no task in this workflow uses",
		"overseer prompt prompts/overseer.md",
		"bounds: at most 6 activations, 2 turns per activation, 1h30m0s of wall clock per activation",
		"idle escalation after 12h0m0s",
		"triggers     gate review becomes ready for review",
		"never on every scheduler tick",
		"gates (2 review boundaries)",
		"  review\n",
		"observes    analyze",
		"protects    apply",
		"holds       apply, publish (protected tasks and their dependency descendants)",
		"rubric      rubrics/review.md",
		"ready at    wave 0 at the earliest",
		"  settle [final]",
		"protects    run settlement; a final-settlement gate holds no downstream task",
		"held by     review -- no dispatch before every one of them is accepted",
		"reviewed by review",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text is missing %q:\n%s", want, text)
		}
	}
	// The static warning still closes the document: supervision does not make a
	// plan a promise about capacity.
	if !strings.HasSuffix(text, staticFooter+"\n") {
		t.Fatalf("text does not end with the static warning:\n%s", text)
	}
}

func TestRenderTextOfAnUnsupervisedPlanIsUnchanged(t *testing.T) {
	text := RenderText(project(t, joinYAML))
	for _, absent := range []string{"supervision", "gates (", "held by", "reviewed by"} {
		if strings.Contains(text, absent) {
			t.Fatalf("an unsupervised plan must not mention %q:\n%s", absent, text)
		}
	}
}

func TestRenderJSONCarriesTheSupervisionProjection(t *testing.T) {
	plan := project(t, supervisedYAML)
	encoded, err := RenderJSON(plan)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	var decoded Plan
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(decoded, plan) {
		t.Fatalf("supervised plan does not round trip:\n%#v\n%#v", decoded, plan)
	}
	reencoded, err := RenderJSON(decoded)
	if err != nil {
		t.Fatalf("re-render: %v", err)
	}
	if string(reencoded) != string(encoded) {
		t.Fatal("the supervised document is not byte-stable")
	}
	if decoded.SchemaVersion != PlanSchemaVersion || PlanSchemaVersion != 2 {
		t.Fatalf("supervision is a schema addition and raises the version: %d", decoded.SchemaVersion)
	}
	for _, want := range []string{
		"\"supervision\": {",
		"\"distinctFromTaskRoutes\": true",
		"\"protectedClosure\": [",
		"\"reviewWave\": 0",
		"\"final\": true",
		"\"heldBy\": [",
		"\"observedBy\": [",
		"\"heldTasks\": 2",
	} {
		if !strings.Contains(string(encoded), want) {
			t.Fatalf("document is missing %q:\n%s", want, encoded)
		}
	}
}

func TestRenderDOTDrawsGatesAsGatesAndTheOverseerApart(t *testing.T) {
	dot := RenderDOT(project(t, supervisedYAML))

	for _, want := range []string{
		"\"__overseer\" [shape=ellipse, peripheries=2, style=dashed, label=\"overseer: overseerInstance/overseer-model, pool overseer-pool\"];",
		"\"gate:review\" [shape=diamond, style=dashed, label=\"gate: review\"];",
		"\"__overseer\" -> \"gate:review\" [style=dotted, label=\"decides\"];",
		"\"analyze\" -> \"gate:review\" [style=dotted, label=\"reviewed\"];",
		"\"gate:review\" -> \"apply\" [style=dotted, label=\"releases\"];",
		"\"gate:settle\" [shape=diamond, style=dashed, label=\"final gate: settle\"];",
		"\"gate:settle\" -> \"__sink\" [style=dotted, label=\"settles\"];",
	} {
		if !strings.Contains(dot, want) {
			t.Fatalf("DOT is missing %q:\n%s", want, dot)
		}
	}
	// A gate is not a task and must not be drawn as one, or a reader counts
	// worker sessions that will never exist.
	if strings.Contains(dot, "\n  \"gate:review\";") || strings.Contains(dot, "\n  \"__overseer\";") {
		t.Fatalf("a gate or the overseer is drawn as a plain task node:\n%s", dot)
	}
	if strings.Count(dot, "{") != strings.Count(dot, "}") || !strings.HasSuffix(dot, "}\n") {
		t.Fatalf("DOT is not a closed graph:\n%s", dot)
	}
}

func TestRenderDOTOfAnUnsupervisedPlanDrawsNoGate(t *testing.T) {
	dot := RenderDOT(project(t, joinYAML))
	if strings.Contains(dot, "gate:") || strings.Contains(dot, overseerNodeName) {
		t.Fatalf("an unsupervised plan must draw no gate and no overseer:\n%s", dot)
	}
}
