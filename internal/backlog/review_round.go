package backlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ReviewCollector consumes only coordinator-retained artifacts from the final
// attempt. It never executes models, chooses routes, or accepts workspace files.
type ReviewCollector struct {
	Store   *sqlite.Store
	Open    func(context.Context, string) (domain.Artifact, io.ReadCloser, error)
	Results string
}

func (c ReviewCollector) Tick(ctx context.Context, now time.Time) error {
	rounds, err := c.Store.ListUncollectedReviewRounds(ctx)
	if err != nil {
		return err
	}
	if len(rounds) == 0 {
		return nil
	}
	records, err := c.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return err
	}
	for _, round := range rounds {
		for _, member := range round.Reviewers {
			if member.State != "pending" {
				continue
			}
			var latest *domain.Attempt
			for i := range records.Attempts {
				a := &records.Attempts[i]
				if a.WorkflowRunID == round.WorkflowRunID && a.TaskID == member.TaskID && (latest == nil || a.Number > latest.Number) {
					latest = a
				}
			}
			result := review.Result{}
			if latest != nil && latest.Progress.Terminal() {
				if latest.CompletedAt != nil && !round.Deadline.IsZero() && latest.CompletedAt.After(round.Deadline) {
					result = review.Result{State: "timed-out", Failure: "review completed after the round deadline"}
				} else if latest.Progress != domain.ProgressSucceeded {
					result = review.Result{State: "failed", Failure: "review task " + string(latest.Progress) + ": " + latest.Failure}
				} else {
					result.State = "succeeded"
					for _, artifact := range records.Artifacts {
						if artifact.WorkflowRunID != round.WorkflowRunID || artifact.TaskID != member.TaskID || artifact.AttemptID != latest.ID || artifact.Kind != domain.ArtifactOutput || (artifact.Name != "review.md" && artifact.Name != "verdict.json") {
							continue
						}
						if artifact.Size > review.MaxDocumentBytes {
							result.State = "failed"
							result.Failure = "review document exceeds 1 MiB"
							break
						}
						if c.Open == nil {
							return errors.New("review artifact opener is unavailable")
						}
						_, reader, err := c.Open(ctx, artifact.ID)
						if err != nil {
							return fmt.Errorf("open retained review artifact: %w", err)
						}
						raw, readErr := io.ReadAll(io.LimitReader(reader, review.MaxDocumentBytes+1))
						closeErr := reader.Close()
						if readErr != nil {
							return readErr
						}
						if closeErr != nil {
							return closeErr
						}
						if len(raw) > review.MaxDocumentBytes {
							result.State = "failed"
							result.Failure = "review document exceeds 1 MiB"
							break
						}
						if artifact.Name == "review.md" {
							result.ReviewMD = string(raw)
						} else {
							result.VerdictJSON = raw
						}
					}
				}
			} else if !round.Deadline.IsZero() && !now.Before(round.Deadline) {
				result = review.Result{State: "timed-out", Failure: "review was not collected before the round deadline"}
			} else {
				continue
			}
			round, err = c.Store.RecordReviewResult(ctx, round.ID, member.ID, round.Revision, result)
			if errors.Is(err, sqlite.ErrReviewRoundConflict) {
				break
			}
			if err != nil {
				return err
			}
		}
		if round.Terminal() && round.ReplyText == "" {
			reply, err := review.WriteOutput(c.Results, round)
			if err != nil {
				return err
			}
			if err := c.Store.PublishReviewReply(ctx, round.ID, round.Revision, review.ShortReply(reply)); err != nil && !errors.Is(err, sqlite.ErrReviewRoundConflict) {
				return err
			}
		}
	}
	return nil
}

// ValidateReviewManifest keeps the graph shape and round requirements together.
func ValidateReviewManifest(m Manifest) error {
	if m.Review == nil {
		return nil
	}
	if m.Environment.Scope != EnvironmentScopeTask {
		return errors.New("review environment.scope must be task to isolate reviewer outputs")
	}
	r := *m.Review
	if r.TemplateVersion != review.TemplateVersion || r.Deadline.IsZero() || m.Supervision != nil || len(m.Gates) > 0 {
		return errors.New("review requires versioned instructions and a deadline; supervision gates are not review rounds")
	}
	if r.ID != "pending" || r.WorkflowRunID != "" || r.Revision != 0 || r.ReplyText != "" {
		return errors.New("review submission cannot contain durable round state")
	}
	if err := r.Initialize(time.Now()); err != nil {
		return err
	}
	if len(r.Reviewers) != len(m.Tasks) {
		return errors.New("every review task must belong to the round")
	}
	swarm := map[string]bool{}
	for _, v := range r.Reviewers {
		if strings.HasPrefix(v.Role, "swarm:") {
			swarm[v.ID] = true
		}
	}
	for _, v := range r.Reviewers {
		task, ok := m.Tasks[v.ID]
		if !ok {
			return fmt.Errorf("reviewer %s has no task", v.ID)
		}
		instance, model, _ := strings.Cut(v.Route, "/")
		if v.TaskID != "" || len(task.Routes) != 1 || task.Routes[0].Instance != instance || task.Routes[0].Model != model {
			return errors.New("review task route must match the round")
		}
		if task.Deadline == nil || !task.Deadline.Equal(r.Deadline) {
			return errors.New("review task must carry the round deadline")
		}
		if len(task.Outputs) != 2 || task.Outputs[0] != "review.md" || task.Outputs[1] != "verdict.json" || len(task.Commits) > 0 {
			return errors.New("review tasks must declare review.md and verdict.json only")
		}
		if v.Role == "judge" {
			if len(task.Needs) != len(swarm) || len(task.InputsFrom) != len(swarm) {
				return errors.New("judge must depend on every swarm lens only")
			}
			seen := map[string]bool{}
			for _, id := range task.Needs {
				if seen[id] {
					return errors.New("judge must depend on every swarm lens exactly once")
				}
				seen[id] = true
				if !swarm[id] {
					return errors.New("judge cannot see independent output")
				}
			}
			for id, paths := range task.InputsFrom {
				if !swarm[id] || len(paths) != 1 || paths[0] != "verdict.json" {
					return errors.New("judge reads only swarm verdicts")
				}
			}
		} else if len(task.Needs) > 0 || len(task.InputsFrom) > 0 {
			return errors.New("reviewer isolation forbids dependencies")
		}
	}
	if err := validateReviewCandidateManifest(m); err != nil {
		return err
	}
	return review.ValidateSelection(r, 0)
}

// reviewJudgeInputs adds coordinator observations to the existing context input.
// It uses the established context transport, so older workers need no new wire fields.
func reviewJudgeInputs(task domain.Task, tasks []domain.Task, artifacts map[string]domain.Artifact, runID string, observed time.Time) *domain.ProjectContext {
	if !task.ReviewJudge {
		return task.Context
	}
	available := map[string]bool{}
	for _, a := range artifacts {
		if a.WorkflowRunID == runID && a.Kind == domain.ArtifactOutput && a.Name == "verdict.json" {
			available[a.TaskID] = true
		}
	}
	var missing []string
	for _, name := range task.Needs {
		for _, producer := range tasks {
			if producer.Name == name && !available[producer.ID] {
				missing = append(missing, name)
			}
		}
	}
	sort.Strings(missing)
	var index domain.ProjectContext
	if task.Context != nil {
		index = *task.Context
		index.CheckpointDelta = append([]string(nil), index.CheckpointDelta...)
	} else {
		index = domain.ProjectContext{
			Version: domain.ProjectContextVersion, Revision: "review-availability", Status: domain.ProjectContextPinned,
			Objective: "Judge the available swarm verdicts against the original evidence",
			Authority: []string{"Coordinator observations of swarm artifact availability; no approval authority"},
			Budget:    "Round deadline", Outputs: []string{"review.md", "verdict.json"},
			Setup:     []string{"Read original pinned inputs and available swarm verdicts"},
			Checks:    []string{"Verify every finding against the original evidence"},
			Freshness: domain.ProjectContextFreshness{ObservedAt: observed},
		}
	}
	observation := "Missing swarm lenses: none"
	if len(missing) > 0 {
		observation = "Missing swarm lenses: " + strings.Join(missing, ", ")
	}
	index.CheckpointDelta = append(index.CheckpointDelta, observation)
	return &index
}
