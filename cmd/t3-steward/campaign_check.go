package main

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// campaignCheckSchemaVersion versions the check document.
const campaignCheckSchemaVersion = 1

// campaignViabilityRequest projects a static plan into the requirements the
// coordinator is asked about. It sends requirements and never the bundle: the
// coordinator is asked whether the work could run, not asked to take delivery.
// The bundle size is passed as two numbers rather than as the bundle, so that
// the coordinator can build the same request during acceptance, where it has a
// manifest and no packed bundle at all.
func campaignViabilityRequest(plan campaign.Plan, bundleBytes int64, bundleFiles int, only string) (backlogadmin.ViabilityRequest, error) {
	request := backlogadmin.ViabilityRequest{
		SchemaVersion: campaignCheckSchemaVersion,
		BundleBytes:   bundleBytes,
		BundleFiles:   bundleFiles,
	}
	for _, task := range plan.Tasks {
		if only != "" && task.Name != only {
			continue
		}
		request.Tasks = append(request.Tasks, backlogadmin.ViabilityTask{
			Name:          task.Name,
			Project:       plan.Environment.Project,
			Type:          plan.Environment.Type,
			Ref:           plan.Environment.Ref,
			Class:         domain.TaskClass(task.Class),
			Hosts:         append([]string(nil), task.Placement.Hosts...),
			Capabilities:  append([]string(nil), task.Placement.Requires...),
			Resources:     campaignResourceDemand(task.Resources),
			Routes:        campaignProviderRoutes(task.Routes),
			Directories:   campaignDirectoryRequests(task.Directories),
			ResourceLocks: append([]string(nil), task.ResourceLocks...),
			NotBefore:     task.Timing.NotBefore,
			ExpiresAt:     task.Timing.ExpiresAt,
			Outputs:       len(task.Outputs),
		})
	}
	if plan.Supervision != nil {
		// A supervised campaign asks for one thing none of its tasks asks for: a
		// worker that hosts the overseer route and advertises the supervision
		// capability. It travels with the request so that a campaign whose gates
		// nobody could ever decide is refused here rather than accepted and held.
		request.Supervision = &backlogadmin.ViabilitySupervision{
			Route:              campaignProviderRoutes([]campaign.Route{plan.Supervision.Route})[0],
			RequiredCapability: workerproto.CapabilityCampaignSupervision,
		}
	}
	if len(request.Tasks) == 0 {
		if only != "" {
			return backlogadmin.ViabilityRequest{}, fmt.Errorf("campaign check: no task named %q in this campaign", only)
		}
		return backlogadmin.ViabilityRequest{}, fmt.Errorf("campaign check: the campaign declares no tasks")
	}
	return request, nil
}

func campaignResourceDemand(resources campaign.Resources) domain.ResourceDemand {
	demand := domain.ResourceDemand{
		MinCPUClass:       domain.CPUClass(resources.MinCPUClass),
		PreferredCPUClass: domain.CPUClass(resources.PreferredCPUClass),
	}
	if resources.CPUUnits != nil {
		demand.CPUUnits = *resources.CPUUnits
	}
	if resources.MemoryMB != nil {
		demand.MemoryMB = *resources.MemoryMB
	}
	if resources.ScratchMB != nil {
		demand.ScratchMB = *resources.ScratchMB
	}
	return demand
}

func campaignProviderRoutes(routes []campaign.Route) []domain.ProviderRoute {
	if len(routes) == 0 {
		return nil
	}
	result := make([]domain.ProviderRoute, 0, len(routes))
	for _, route := range routes {
		converted := domain.ProviderRoute{
			WorkerID: route.Host, ProviderInstanceID: route.Instance,
			Model: route.Model, QuotaPoolID: route.QuotaPool,
		}
		for _, option := range route.Options {
			if converted.Options == nil {
				converted.Options = make(map[string]string, len(route.Options))
			}
			converted.Options[option.Name] = option.Value
		}
		result = append(result, converted)
	}
	return result
}

func campaignDirectoryRequests(directories []campaign.Directory) []directoryresource.Request {
	if len(directories) == 0 {
		return nil
	}
	result := make([]directoryresource.Request, 0, len(directories))
	for _, directory := range directories {
		result = append(result, directoryresource.Request{
			WorkerID: directory.Worker, ResourceID: directory.Resource,
			Revision: directory.Revision, Access: directoryresource.Access(directory.Access),
		})
	}
	return result
}

// campaignCheck is the document check prints. It wraps the coordinator's matrix
// rather than restating it, so an agent parses one shape whether it read the
// matrix from check or from a refused submission.
type campaignCheck struct {
	SchemaVersion int                          `json:"schemaVersion"`
	Source        string                       `json:"source"`
	Name          string                       `json:"name"`
	Digest        string                       `json:"digest"`
	Matrix        backlogadmin.ViabilityMatrix `json:"matrix"`
}

// campaignSubmission is what submit prints as JSON. It carries the submission
// answer unchanged and adds the readiness outcome, because an agent that is
// about to end its turn needs to know whether anything can start.
// The submission answer is embedded rather than nested, so every field an
// existing reader parses stays exactly where it was and the readiness fields
// are additions.
type campaignSubmission struct {
	backlogadmin.LocalSubmissionResponse
	Outcome backlogadmin.ViabilityOutcome `json:"outcome,omitempty"`
	Matrix  *backlogadmin.ViabilityMatrix `json:"matrix,omitempty"`
	// Notify is present only when --notify-thread registered a wait.
	Notify *campaignNotification `json:"notify,omitempty"`
}

// campaignWaitingMatrix returns the matrix only when it explains a wait. A
// ready campaign does not need its own evidence repeated back.
func campaignWaitingMatrix(matrix backlogadmin.ViabilityMatrix) *backlogadmin.ViabilityMatrix {
	if matrix.Outcome != backlogadmin.ViabilityAcceptedWaiting {
		return nil
	}
	return &matrix
}

// submissionPrincipal names who ran the command. An unresolved transport leaves
// it empty, and the audit record then says so rather than inventing a name.
func (c campaignCLI) submissionPrincipal() string {
	if c.principal == "" {
		return "unknown"
	}
	return c.principal
}

func (c campaignCLI) runCheck(ctx context.Context, args []string) error {
	parsed, err := parseCampaignArgs("check", args, false, false)
	if err != nil {
		return err
	}
	bundle, plan, err := c.prepare(parsed.source)
	if err != nil {
		return err
	}
	matrix, err := c.checkViability(ctx, plan, bundle, parsed.task)
	if err != nil {
		return err
	}
	document := campaignCheck{
		SchemaVersion: campaignCheckSchemaVersion,
		Source:        parsed.source, Name: plan.Name, Digest: bundle.ContentDigest,
		Matrix: matrix,
	}
	if parsed.asJSON {
		if err := encodeCampaignJSON(c.stdout, document); err != nil {
			return err
		}
	} else if err := renderCampaignCheck(c.stdout, document); err != nil {
		return err
	}
	if matrix.Outcome == backlogadmin.ViabilityImpossible {
		// The document above is the whole --json answer; the refusal is the
		// exit code and the stderr line, never a second document.
		return afterDocument(campaignImpossible(matrix))
	}
	return nil
}

// checkViability asks the coordinator. It is a separate seam from the
// submission transport so that the campaign test suite can keep proving
// validate and plan never reach the coordinator while check always does.
func (c campaignCLI) checkViability(ctx context.Context, plan campaign.Plan, bundle campaign.Bundle, only string) (backlogadmin.ViabilityMatrix, error) {
	if c.viability == nil {
		return backlogadmin.ViabilityMatrix{}, fmt.Errorf("coordinator readiness transport is unavailable")
	}
	var bundleBytes int64
	var bundleFiles int
	if bundle.Campaign != nil {
		bundleBytes, bundleFiles = bundle.Campaign.TotalBytes(), len(bundle.Campaign.Files)
	}
	request, err := campaignViabilityRequest(plan, bundleBytes, bundleFiles, only)
	if err != nil {
		return backlogadmin.ViabilityMatrix{}, err
	}
	return c.viability(ctx, request)
}

// campaignImpossible turns a refused matrix into the one error an agent reads.
// It names the permanent reasons and nothing else, because a temporary reason
// in a refusal message reads as something the caller could wait out.
func campaignImpossible(matrix backlogadmin.ViabilityMatrix) error {
	reasons := matrix.PermanentReasons()
	lines := make([]string, 0, len(reasons))
	seen := make(map[string]bool, len(reasons))
	for _, reason := range reasons {
		line := reason.Code + ": " + reason.Detail
		if seen[line] {
			continue
		}
		seen[line] = true
		lines = append(lines, line)
	}
	sort.Strings(lines)
	return &backlogadmin.TransportError{
		Class:     backlogadmin.ClassRejected,
		Operation: "campaign check",
		Err: fmt.Errorf("this campaign can never run as written:\n  %s",
			strings.Join(lines, "\n  ")),
	}
}

// campaignSupervisionVerdict is the supervision sibling of campaignImpossible:
// it turns one refusal into the single error a caller branches on. The two live
// together because they answer the same question for a caller, "what is this
// refusal and can I do anything about it", and a second convention for that
// answer would be a second thing to learn.
//
// The supervision class is the distinction the caller reads, so it is named in
// the message and preserved verbatim. The exit code is the frozen transport
// numbering and is not extended: rejected on the merits is 8 for supervision
// exactly as it is for an impossible campaign, unauthorized scope is the
// authentication code 4, an unavailable coordinator is 5, and a request that
// was wrong in itself is the protocol code 7. stale-evidence and
// unmet-prerequisite therefore share exit 8; the class word tells them apart,
// in the message and in the --json error document built from it.
func campaignSupervisionVerdict(operation backlogadmin.SupervisionOperation, err error) error {
	if err == nil {
		return nil
	}
	class := backlogadmin.ClassifySupervisionError(err)
	if class == backlogadmin.SupervisionErrorMalformed {
		// A refusal that crossed a carrier arrives as prose with its sentinels
		// gone, so it would classify as malformed whatever it was. When it
		// already carries a transport class, that class is the honest answer and
		// inventing a supervision one over it would be worse than saying less.
		if carried := backlogadmin.ClassOf(err); carried != "" && carried != backlogadmin.ClassOK {
			return err
		}
	}
	return &backlogadmin.TransportError{
		Class:     campaignSupervisionTransportClass(class),
		Operation: "campaign supervision " + string(operation),
		Err:       fmt.Errorf("%s: %w", class, err),
	}
}

// campaignSupervisionTransportClass maps a supervision class onto the frozen
// transport class whose exit code it shares.
func campaignSupervisionTransportClass(class backlogadmin.SupervisionErrorClass) backlogadmin.TransportClass {
	switch class {
	case backlogadmin.SupervisionErrorUnauthorizedScope:
		return backlogadmin.ClassAuthentication
	case backlogadmin.SupervisionErrorUnavailable:
		return backlogadmin.ClassUnavailable
	case backlogadmin.SupervisionErrorMalformed:
		return backlogadmin.ClassProtocol
	default:
		// stale-evidence and unmet-prerequisite are both refusals on the merits.
		return backlogadmin.ClassRejected
	}
}

// campaignOutcomeHeadline is the first line of a check. accepted_waiting is
// spelled out, because the bare word next to "campaign X is" read as a failure
// to agents that had just been told a submission would succeed: it says what
// submit does with the campaign and names the obstruction it waits for.
func campaignOutcomeHeadline(name string, matrix backlogadmin.ViabilityMatrix) string {
	if matrix.Outcome != backlogadmin.ViabilityAcceptedWaiting {
		return fmt.Sprintf("campaign %s is %s", name, matrix.Outcome)
	}
	return fmt.Sprintf("campaign %s is accepted_waiting: submit accepts it and it waits, queued, for %s; nothing about it is permanently wrong",
		name, campaignWaitingFor(matrix))
}

// campaignWaitingFor names what an accepted_waiting campaign is waiting for:
// the distinct temporary reason codes anywhere in the matrix, which are the
// obstructions that clear on their own or by an operator's hand.
func campaignWaitingFor(matrix backlogadmin.ViabilityMatrix) string {
	seen := map[string]bool{}
	var codes []string
	add := func(reasons []backlogadmin.ViabilityReason) {
		for _, reason := range reasons {
			if !reason.Permanent && !seen[reason.Code] {
				seen[reason.Code] = true
				codes = append(codes, reason.Code)
			}
		}
	}
	add(matrix.Reasons)
	for _, task := range matrix.Tasks {
		add(task.Reasons)
		for _, candidate := range task.Candidates {
			add(candidate.Reasons)
		}
	}
	if len(codes) == 0 {
		return "a worker that can start it"
	}
	sort.Strings(codes)
	return strings.Join(codes, ", ") + " to clear"
}

// candidateNotEligible reports a worker that may not serve the task at all,
// as configured: every finding against it is worker-not-eligible, such as a
// worker outside the project's worker set or the task's placement hosts. It
// is not an obstruction of this campaign but a statement about the fleet, and
// printing it as "impossible" beside a ready worker read as a fault to fix.
func candidateNotEligible(candidate backlogadmin.ViabilityCandidate) bool {
	if candidate.Outcome != backlogadmin.ViabilityImpossible || len(candidate.Reasons) == 0 {
		return false
	}
	for _, reason := range candidate.Reasons {
		if reason.Code != backlogadmin.ReasonWorkerNotEligible {
			return false
		}
	}
	return true
}

// candidateNotes are the lines about what a candidate's answer did and did
// not look at, in the order they are printed.
func candidateNotes(candidate backlogadmin.ViabilityCandidate) []string {
	var notes []string
	// What was not checked is printed with what was. A reader of "ready"
	// has to be able to tell a passed check from a question nobody asked.
	if observation := candidate.Repository; observation != nil {
		if observation.Observed {
			notes = append(notes, "repository  observed: "+observation.Class)
		} else {
			notes = append(notes, "repository  not observed: "+observation.Unobserved)
		}
	}
	for _, note := range candidate.Unchecked {
		notes = append(notes, "unchecked   "+note)
	}
	return notes
}

// renderCampaignCheck prints the matrix for a reader. Two things are folded
// that the JSON document keeps in full. A note every listed worker of a task
// shares, such as a fresh workspace having no repository to reach, is printed
// once for the task rather than once per worker. Workers that are not eligible
// for the task at all are counted and named on one line after the eligible
// ones, rather than listed as impossible candidates.
func renderCampaignCheck(out interface{ Write([]byte) (int, error) }, document campaignCheck) error {
	var lines []string
	linef := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	linef("%s", campaignOutcomeHeadline(document.Name, document.Matrix))
	for _, reason := range document.Matrix.Reasons {
		linef("  %s  %s: %s", permanenceLabel(reason.Permanent), reason.Code, reason.Detail)
	}
	for _, task := range document.Matrix.Tasks {
		linef("  %s  %s", task.Task, task.Outcome)
		for _, reason := range task.Reasons {
			linef("    %s  %s: %s", permanenceLabel(reason.Permanent), reason.Code, reason.Detail)
		}
		var listed, excluded []backlogadmin.ViabilityCandidate
		for _, candidate := range task.Candidates {
			if candidateNotEligible(candidate) {
				excluded = append(excluded, candidate)
			} else {
				listed = append(listed, candidate)
			}
		}
		// A note is shared when every listed worker carries it; with one worker
		// there is nothing to fold.
		shared := map[string]bool{}
		var sharedOrder []string
		if len(listed) > 1 {
			for _, note := range candidateNotes(listed[0]) {
				all := true
				for _, other := range listed[1:] {
					if !slices.Contains(candidateNotes(other), note) {
						all = false
						break
					}
				}
				if all && !shared[note] {
					shared[note] = true
					sharedOrder = append(sharedOrder, note)
				}
			}
		}
		for _, note := range sharedOrder {
			linef("    every worker below: %s", note)
		}
		for _, candidate := range listed {
			linef("    %s  %s", candidate.Worker, candidate.Outcome)
			for _, note := range candidateNotes(candidate) {
				if !shared[note] {
					linef("      %s", note)
				}
			}
			for _, reason := range candidate.Reasons {
				if reason.Desired != "" || reason.Observed != "" {
					linef("      %s  %s: %s (desired %s, observed %s, expected revision %d)",
						permanenceLabel(reason.Permanent), reason.Code, reason.Detail,
						reason.Desired, reason.Observed, reason.Revision)
				} else {
					linef("      %s  %s: %s", permanenceLabel(reason.Permanent), reason.Code, reason.Detail)
				}
			}
		}
		if len(excluded) > 0 {
			workers := "workers"
			if len(excluded) == 1 {
				workers = "worker"
			}
			details := make([]string, 0, len(excluded))
			for _, candidate := range excluded {
				findings := make([]string, 0, len(candidate.Reasons))
				for _, reason := range candidate.Reasons {
					findings = append(findings, reason.Detail)
				}
				details = append(details, candidate.Worker+" ("+strings.Join(findings, "; ")+")")
			}
			linef("    not eligible, omitted: %d %s: %s", len(excluded), workers, strings.Join(details, ", "))
		}
	}
	_, err := fmt.Fprint(out, strings.Join(lines, "\n")+"\n")
	return err
}

func permanenceLabel(permanent bool) string {
	if permanent {
		return "permanent"
	}
	return "temporary"
}
