package campaign

import (
	"fmt"
	"strconv"
	"strings"
)

// writeSupervision prints the overseer block of a supervised plan. It says
// outright that the overseer route is not a worker route: the whole point of the
// declaration is that the overseer is configured independently, and a reader
// scanning a route list would otherwise take it for one more task candidate.
func writeSupervision(out *strings.Builder, width int, plan Plan) {
	supervision := plan.Supervision
	if supervision == nil {
		return
	}
	distinct := "a provider instance and model no task in this workflow uses"
	if !supervision.DistinctFromTaskRoutes {
		distinct = "the same provider instance and model a task in this workflow uses"
	}
	header(out, width, "supervision", fmt.Sprintf(
		"overseer route %s -- its own route, not a worker route, on %s",
		describeRoute(supervision.Route), distinct))
	header(out, width, "", "overseer prompt "+supervision.PromptFile)
	header(out, width, "", fmt.Sprintf(
		"bounds: at most %s, %s per activation, %s of wall clock per activation",
		counted(supervision.MaxActivations, "activation", "activations"),
		counted(supervision.MaxTurnsPerActivation, "turn", "turns"),
		supervision.ActivationDeadline))
	if supervision.IdleEscalationAfter != "" {
		header(out, width, "", "idle escalation after "+supervision.IdleEscalationAfter)
	}
	if supervision.Escalation.NotifyThread {
		destination := "the configured notify thread"
		if supervision.Escalation.ThreadID != "" {
			destination = "thread " + supervision.Escalation.ThreadID
		}
		header(out, width, "", "escalations are delivered to "+destination)
	}
	for i, trigger := range supervision.Triggers {
		label := ""
		if i == 0 {
			label = "triggers"
		}
		header(out, width, label, trigger)
	}
	header(out, width, "", "the overseer is woken by the events above, never on every scheduler tick")
}

// writeGates prints the review boundaries. A gate is not a task and is never
// printed as one: it observes results and holds dispatch, and the tasks it holds
// include the dependency descendants of the tasks it names.
func writeGates(out *strings.Builder, plan Plan) {
	if len(plan.Gates) == 0 {
		return
	}
	const width = 11
	fmt.Fprintf(out, "\ngates (%s)\n", counted(len(plan.Gates), "review boundary", "review boundaries"))
	for _, gate := range plan.Gates {
		if gate.Final {
			fmt.Fprintf(out, "  %s [final]\n", gate.Name)
		} else {
			fmt.Fprintf(out, "  %s\n", gate.Name)
		}
		field(out, width, "observes", strings.Join(gate.Observes, ", "))
		if gate.Final {
			field(out, width, "protects", "run settlement; a final-settlement gate holds no downstream task")
		} else {
			field(out, width, "protects", strings.Join(gate.Protects, ", "))
			field(out, width, "holds", strings.Join(gate.ProtectedClosure, ", ")+
				" (protected tasks and their dependency descendants)")
		}
		if gate.RubricFile != "" {
			field(out, width, "rubric", gate.RubricFile)
		}
		field(out, width, "ready at", "wave "+strconv.Itoa(gate.ReviewWave)+
			" at the earliest, once every observed task has succeeded and its evidence is immutable")
	}
}

// writeSupervisionDOT draws the overseer and the gates. Gates are diamonds and
// the overseer is a doubled ellipse, both dashed: neither is a task this
// manifest declares, and drawing either as a task would invent worker work that
// does not exist.
func writeSupervisionDOT(out *strings.Builder, plan Plan) {
	if plan.Supervision == nil {
		return
	}
	fmt.Fprintf(out, "  %s [shape=ellipse, peripheries=2, style=dashed, label=%s];\n",
		quoteDOT(overseerNodeName),
		quoteDOT("overseer: "+describeRoute(plan.Supervision.Route)))
	for _, gate := range plan.Gates {
		label := "gate: " + gate.Name
		if gate.Final {
			label = "final gate: " + gate.Name
		}
		gateNode := "gate:" + gate.Name
		fmt.Fprintf(out, "  %s [shape=diamond, style=dashed, label=%s];\n",
			quoteDOT(gateNode), quoteDOT(label))
		fmt.Fprintf(out, "  %s -> %s [style=dotted, label=\"decides\"];\n",
			quoteDOT(overseerNodeName), quoteDOT(gateNode))
		for _, observed := range gate.Observes {
			fmt.Fprintf(out, "  %s -> %s [style=dotted, label=\"reviewed\"];\n",
				quoteDOT(observed), quoteDOT(gateNode))
		}
		if gate.Final {
			fmt.Fprintf(out, "  %s -> %s [style=dotted, label=\"settles\"];\n",
				quoteDOT(gateNode), quoteDOT(sinkNodeName))
			continue
		}
		for _, protected := range gate.Protects {
			fmt.Fprintf(out, "  %s -> %s [style=dotted, label=\"releases\"];\n",
				quoteDOT(gateNode), quoteDOT(protected))
		}
	}
}
