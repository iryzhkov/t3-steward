package backlog

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ManifestFixLoop declares a finite sequence of implementation and fresh review tasks.
type ManifestFixLoop struct {
	Implement string `yaml:"implement"`
	Review    string `yaml:"review"`
	MaxRounds int    `yaml:"max_rounds"`
}

// MarshalYAML preserves the bounded authored form when a parsed manifest is
// saved again. Generated nodes are reconstructed on parsing, rather than
// colliding with their own templates or accepting authored internal metadata.
func (manifest Manifest) MarshalYAML() (any, error) {
	type plain Manifest
	result := plain(manifest)
	result.Tasks = maps.Clone(manifest.Tasks)
	for name, task := range result.Tasks {
		if task.FixLoop != nil && task.FixLoop.Round > 1 {
			delete(result.Tasks, name)
		}
	}
	return result, nil
}

// MaxFixLoopRounds bounds manifest expansion before any task is admitted.
const MaxFixLoopRounds = 20

func validateNeedsVerdict(name string, task ManifestTask, tasks map[string]ManifestTask) error {
	producers := make([]string, 0, len(task.NeedsVerdict))
	for producer := range task.NeedsVerdict {
		producers = append(producers, producer)
	}
	sort.Strings(producers)
	for _, producer := range producers {
		verdict := task.NeedsVerdict[producer]
		if verdict != "accept" && verdict != "changes-requested" {
			return fmt.Errorf("task %s needs_verdict %s must be accept or changes-requested", name, producer)
		}
		if !slices.Contains(task.Needs, producer) {
			return fmt.Errorf("task %s needs_verdict %s is not a direct dependency", name, producer)
		}
		source, ok := tasks[producer]
		if !ok || strings.Contains(producer, "/") || source.ReviewOutput == nil {
			return fmt.Errorf("task %s needs_verdict %s requires a local review_output producer", name, producer)
		}
	}
	return nil
}

func fixLoopRoundName(base string, round int) string {
	if round == 1 {
		return base
	}
	return fmt.Sprintf("%s-round-%d", base, round)
}

func cloneFixLoopTemplate(task ManifestTask) ManifestTask {
	task.Needs = slices.Clone(task.Needs)
	task.Outputs = slices.Clone(task.Outputs)
	task.Commits = slices.Clone(task.Commits)
	task.Verify = slices.Clone(task.Verify)
	task.ResourceLocks = slices.Clone(task.ResourceLocks)
	task.Options = cloneRoleOptions(task.Options)
	task.Routes = cloneRoutes(task.Routes)
	task.NeedsVerdict = maps.Clone(task.NeedsVerdict)
	task.InputsFrom = maps.Clone(task.InputsFrom)
	for name, values := range task.InputsFrom {
		task.InputsFrom[name] = slices.Clone(values)
	}
	task.ReviewOutput = domain.CloneReviewOutput(task.ReviewOutput)
	task.ReviewRequirements = cloneManifestReview(task.ReviewRequirements)
	return task
}

func addLoopInput(task *ManifestTask, producer string, outputs []string) {
	if !slices.Contains(task.Needs, producer) {
		task.Needs = append(task.Needs, producer)
	}
	if task.InputsFrom == nil {
		task.InputsFrom = map[string][]string{}
	}
	for _, output := range outputs {
		if !slices.Contains(task.InputsFrom[producer], output) {
			task.InputsFrom[producer] = append(task.InputsFrom[producer], output)
		}
	}
}

func remapLoopProducer(task *ManifestTask, original, replacement string) {
	for i, producer := range task.Needs {
		if producer == original {
			task.Needs[i] = replacement
		}
	}
	if inputs, ok := task.InputsFrom[original]; ok {
		delete(task.InputsFrom, original)
		task.InputsFrom[replacement] = inputs
	}
	if verdict, ok := task.NeedsVerdict[original]; ok {
		delete(task.NeedsVerdict, original)
		task.NeedsVerdict[replacement] = verdict
	}
}

func expandManifestFixLoops(manifest *Manifest) error {
	names := make([]string, 0, len(manifest.FixLoops))
	for name := range manifest.FixLoops {
		names = append(names, name)
	}
	sort.Strings(names)
	// Templates must come from the authored graph, never from rounds generated
	// while expanding an earlier loop.
	authoredTasks := maps.Clone(manifest.Tasks)
	used := map[string]bool{}
	for _, name := range names {
		loop := manifest.FixLoops[name]
		fail := func(reason string) error { return fmt.Errorf("fix_loop %s: %s", name, reason) }
		if !manifestNamePattern.MatchString(name) {
			return fail("invalid loop name")
		}
		if loop.MaxRounds < 1 || loop.MaxRounds > MaxFixLoopRounds {
			return fail("max_rounds must be between 1 and 20")
		}
		if manifest.Environment.Type != EnvironmentGit || manifest.Environment.Scope != EnvironmentScopeTask {
			return fail("requires git task workspaces so every round is fresh")
		}
		implement, exists := authoredTasks[loop.Implement]
		if !exists {
			return fail("implement task is missing")
		}
		reviewer, exists := authoredTasks[loop.Review]
		if !exists || loop.Implement == loop.Review {
			return fail("review must name a distinct existing task")
		}
		if used[loop.Implement] || used[loop.Review] {
			return fail("task templates cannot belong to multiple loops")
		}
		used[loop.Implement], used[loop.Review] = true, true
		if len(implement.Commits) == 0 {
			return fail("implement must declare a commit")
		}
		if reviewer.ReviewOutput == nil || len(reviewer.Outputs) == 0 {
			return fail("review must declare review_output and outputs")
		}
		if !slices.Contains(reviewer.Outputs, "review.md") {
			return fail("review must retain review.md for the next implementation round")
		}
		if len(reviewer.Commits) > 0 {
			return fail("review cannot declare commits; each review must be independent of implementation")
		}
		if implement.ReviewOutput != nil {
			return fail("implement cannot be a review_output task")
		}
		if slices.Contains(implement.Needs, loop.Review) {
			return fail("implement cannot depend on its review")
		}
		if !slices.Contains(reviewer.Needs, loop.Implement) {
			return fail("review must need implement")
		}
		if _, conditioned := reviewer.NeedsVerdict[loop.Implement]; conditioned {
			return fail("review cannot condition its implementation dependency")
		}
		commits := make([]string, 0, len(implement.Commits))
		for _, commit := range implement.Commits {
			commits = append(commits, commit.Name)
		}
		// Every review receives the commit it judges, including the first round.
		reviewer = cloneFixLoopTemplate(reviewer)
		addLoopInput(&reviewer, loop.Implement, commits)
		for round := 1; round <= loop.MaxRounds; round++ {
			implementationName := fixLoopRoundName(loop.Implement, round)
			reviewName := fixLoopRoundName(loop.Review, round)
			if round > 1 {
				if _, exists := manifest.Tasks[implementationName]; exists {
					return fail("generated task name collides: " + implementationName)
				}
				if _, exists := manifest.Tasks[reviewName]; exists {
					return fail("generated task name collides: " + reviewName)
				}
			}
			nextImplement := cloneFixLoopTemplate(implement)
			nextReview := cloneFixLoopTemplate(reviewer)
			nextImplement.FixLoop = &domain.FixLoopTask{Name: name, Round: round, MaxRounds: loop.MaxRounds, Kind: "implement"}
			nextReview.FixLoop = &domain.FixLoopTask{Name: name, Round: round, MaxRounds: loop.MaxRounds, Kind: "review"}
			if round > 1 {
				previousImplement := fixLoopRoundName(loop.Implement, round-1)
				previousReview := fixLoopRoundName(loop.Review, round-1)
				addLoopInput(&nextImplement, previousImplement, commits)
				addLoopInput(&nextImplement, previousReview, reviewer.Outputs)
				if nextImplement.NeedsVerdict == nil {
					nextImplement.NeedsVerdict = map[string]string{}
				}
				nextImplement.NeedsVerdict[previousReview] = "changes-requested"
				remapLoopProducer(&nextReview, loop.Implement, implementationName)
			}
			manifest.Tasks[implementationName] = nextImplement
			manifest.Tasks[reviewName] = nextReview
		}
	}
	return nil
}
