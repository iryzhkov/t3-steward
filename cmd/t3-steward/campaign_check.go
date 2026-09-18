package main

import (
	"context"
	"fmt"
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

func renderCampaignCheck(out interface{ Write([]byte) (int, error) }, document campaignCheck) error {
	if _, err := fmt.Fprintf(out, "campaign %s is %s\n", document.Name, document.Matrix.Outcome); err != nil {
		return err
	}
	for _, reason := range document.Matrix.Reasons {
		if _, err := fmt.Fprintf(out, "  %s  %s: %s\n",
			permanenceLabel(reason.Permanent), reason.Code, reason.Detail); err != nil {
			return err
		}
	}
	for _, task := range document.Matrix.Tasks {
		if _, err := fmt.Fprintf(out, "  %s  %s\n", task.Task, task.Outcome); err != nil {
			return err
		}
		for _, reason := range task.Reasons {
			if _, err := fmt.Fprintf(out, "    %s  %s: %s\n",
				permanenceLabel(reason.Permanent), reason.Code, reason.Detail); err != nil {
				return err
			}
		}
		for _, candidate := range task.Candidates {
			if _, err := fmt.Fprintf(out, "    %s  %s\n", candidate.Worker, candidate.Outcome); err != nil {
				return err
			}
			// What was not checked is printed with what was. A reader of "ready"
			// has to be able to tell a passed check from a question nobody asked.
			if observation := candidate.Repository; observation != nil {
				line := "      repository  observed: " + observation.Class + "\n"
				if !observation.Observed {
					line = "      repository  not observed: " + observation.Unobserved + "\n"
				}
				if _, err := fmt.Fprint(out, line); err != nil {
					return err
				}
			}
			for _, note := range candidate.Unchecked {
				if _, err := fmt.Fprintf(out, "      unchecked   %s\n", note); err != nil {
					return err
				}
			}
			for _, reason := range candidate.Reasons {
				line := fmt.Sprintf("      %s  %s: %s\n",
					permanenceLabel(reason.Permanent), reason.Code, reason.Detail)
				if reason.Desired != "" || reason.Observed != "" {
					line = fmt.Sprintf("      %s  %s: %s (desired %s, observed %s, expected revision %d)\n",
						permanenceLabel(reason.Permanent), reason.Code, reason.Detail,
						reason.Desired, reason.Observed, reason.Revision)
				}
				if _, err := fmt.Fprint(out, line); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func permanenceLabel(permanent bool) string {
	if permanent {
		return "permanent"
	}
	return "temporary"
}
