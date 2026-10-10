package wait

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// ChecksCompleted is the state of a checks wait: every check of a commit or of
// a pull request's head has finished. It is met when none of them failed and
// failed when one did, and in both cases the reason is one line counting the
// conclusions. Unlike checks-passed it does not settle on the first failure:
// the caller asked for the whole picture of one commit, so it waits until no
// check is still running.
const ChecksCompleted = "checks-completed"

// GitHubCommitSHA is the abbreviated or full hexadecimal commit id a checks
// wait accepts. GitHub resolves an abbreviation of at least seven digits.
var GitHubCommitSHA = regexp.MustCompile(`^[0-9a-fA-F]{7,40}$`)

// gitHubCommitChecksQuery reads the check runs and commit statuses of one
// commit in one request: GitHub's own rollup, the same one a pull request
// shows, with up to the first hundred contexts named.
const gitHubCommitChecksQuery = `query($owner: String!, $name: String!, $expression: String!) {
  repository(owner: $owner, name: $name) {
    object(expression: $expression) {
      ... on Commit {
        oid
        url
        statusCheckRollup {
          state
          contexts(first: 100) {
            totalCount
            nodes {
              __typename
              ... on CheckRun { name status conclusion }
              ... on StatusContext { context state }
            }
          }
        }
      }
    }
  }
}`

// commitChecksArgs are the fixed gh arguments that read a commit's checks.
// Every variable is passed with -f, as a string: -F would turn an all-digit
// abbreviated commit id into a number.
func commitChecksArgs(t GitHubTarget) []string {
	owner, name, _ := strings.Cut(t.Repo, "/")
	return []string{"api", "graphql",
		"-f", "query=" + gitHubCommitChecksQuery,
		"-f", "owner=" + owner, "-f", "name=" + name, "-f", "expression=" + t.ID}
}

// validateCommitTarget checks what a commit target needs beyond a kind and a
// state: a commit id and the repository it is in.
func validateCommitTarget(t GitHubTarget) error {
	if !GitHubCommitSHA.MatchString(t.ID) {
		return fmt.Errorf("--github-checks commit %q is not a commit id of 7 to 40 hexadecimal digits", t.ID)
	}
	if t.Repo == "" {
		return fmt.Errorf("--github-checks commit %s needs its repository: owner/name@%s", t.ID, t.ID)
	}
	return nil
}

// gitHubCheck is one check run or commit status as gh reports it, in either
// the GraphQL shape or gh pr view's statusCheckRollup, which share it.
type gitHubCheck struct {
	Typename   string `json:"__typename"`
	Name       string `json:"name"`
	Context    string `json:"context"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	State      string `json:"state"`
}

// label is the check's name, made safe for one line of a wake message.
func (c gitHubCheck) label() string {
	name := c.Name
	if name == "" {
		name = c.Context
	}
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == ',' || r == '(' || r == ')' {
			return ' '
		}
		return r
	}, name)
	name = strings.Join(strings.Fields(name), " ")
	if name == "" {
		name = "unnamed"
	}
	return summaryText(name, 40)
}

// outcome is the check's conclusion in lower case, or pending while it is
// still running. A check run that is not completed is pending whatever its
// conclusion field says, and a commit status has only a state.
func (c gitHubCheck) outcome() string {
	if c.Status != "" && !strings.EqualFold(c.Status, "COMPLETED") {
		return "pending"
	}
	conclusion := c.Conclusion
	if conclusion == "" {
		conclusion = c.State
	}
	switch strings.ToUpper(conclusion) {
	case "", "PENDING", "EXPECTED", "QUEUED", "IN_PROGRESS", "WAITING", "REQUESTED":
		return "pending"
	}
	return strings.ToLower(conclusion)
}

// checkPassed are the conclusions that do not fail a commit.
var checkPassed = map[string]bool{"success": true, "skipped": true, "neutral": true}

// ChecksSummary is the outcome of a set of checks: whether every one finished,
// whether any failed, and the one line that says so.
type ChecksSummary struct {
	Total   int
	Pending int
	Failed  int
	// Counts are the checks per lower-case conclusion, pending included.
	Counts map[string]int
	// Line is the one-line summary: "checks passed: ...", "checks failed:
	// ..." or "checks pending: ...".
	Line string
	// Conclusion is success, the first failing conclusion, or empty while a
	// check is pending.
	Conclusion string
}

// summarizeChecks counts the checks and writes the line. total is how many
// checks GitHub says there are, which can exceed the ones read; rollup is
// GitHub's own aggregate state, which then decides for the checks not read.
func summarizeChecks(checks []gitHubCheck, total int, rollup string) ChecksSummary {
	s := ChecksSummary{Counts: map[string]int{}}
	names := map[string][]string{}
	firstFailure := ""
	for _, check := range checks {
		outcome := check.outcome()
		s.Counts[outcome]++
		switch {
		case outcome == "pending":
			s.Pending++
			names[outcome] = append(names[outcome], check.label())
		case !checkPassed[outcome]:
			s.Failed++
			names[outcome] = append(names[outcome], check.label())
			if firstFailure == "" {
				firstFailure = outcome
			}
		}
	}
	s.Total = len(checks)
	unread := 0
	if total > len(checks) {
		unread = total - len(checks)
		s.Total = total
		// The checks not read are judged by GitHub's rollup of all of them.
		switch strings.ToUpper(rollup) {
		case "SUCCESS":
		case "FAILURE", "ERROR":
			if s.Failed == 0 {
				s.Failed++
				firstFailure = strings.ToLower(rollup)
			}
		default:
			if s.Pending == 0 {
				s.Pending++
			}
		}
	}
	verdict := "passed"
	switch {
	case s.Pending > 0 || s.Total == 0:
		verdict = "pending"
	case s.Failed > 0:
		verdict, s.Conclusion = "failed", firstFailure
	default:
		s.Conclusion = "success"
	}
	if s.Total == 0 {
		s.Line = "checks pending: no checks reported yet"
		return s
	}
	// Failures first, then pending, then the passing conclusions, each in a
	// fixed order so the same checks always read the same.
	outcomes := make([]string, 0, len(s.Counts))
	for outcome := range s.Counts {
		outcomes = append(outcomes, outcome)
	}
	rank := func(o string) int {
		switch {
		case o == "pending":
			return 1
		case checkPassed[o]:
			return 2
		}
		return 0
	}
	sort.Slice(outcomes, func(i, j int) bool {
		if rank(outcomes[i]) != rank(outcomes[j]) {
			return rank(outcomes[i]) < rank(outcomes[j])
		}
		return outcomes[i] < outcomes[j]
	})
	parts := make([]string, 0, len(outcomes)+1)
	for _, outcome := range outcomes {
		part := strconv.Itoa(s.Counts[outcome]) + " " + outcome
		if listed := names[outcome]; len(listed) > 0 {
			shown := listed
			if len(shown) > 3 {
				shown = shown[:3]
			}
			part += " (" + strings.Join(shown, ", ")
			if more := len(listed) - len(shown); more > 0 {
				part += fmt.Sprintf(", +%d more", more)
			}
			part += ")"
		}
		parts = append(parts, part)
	}
	if unread > 0 {
		parts = append(parts, fmt.Sprintf("%d not listed, rollup %s", unread, strings.ToLower(rollup)))
	}
	s.Line = fmt.Sprintf("checks %s: %d checks, %s", verdict, s.Total, strings.Join(parts, ", "))
	return s
}

// checksReading turns a summary into the reading of a checks-completed wait.
func checksReading(reading GitHubReading, s ChecksSummary) GitHubReading {
	reading.Reason = s.Line
	counts := make([]string, 0, len(s.Counts))
	for outcome, n := range s.Counts {
		counts = append(counts, outcome+"="+strconv.Itoa(n))
	}
	sort.Strings(counts)
	if len(counts) > 0 {
		reading.Fields["checks"] = strings.Join(counts, ",")
	}
	switch {
	case s.Pending > 0 || s.Total == 0:
		reading.Status = StatusWaiting
	case s.Failed > 0:
		reading.Status = StatusFailed
		reading.Fields["conclusion"] = s.Conclusion
	default:
		reading.Status = StatusMet
		reading.Fields["conclusion"] = s.Conclusion
	}
	return reading
}

// evaluateCommitChecks reads gh api graphql's answer for a commit target.
func evaluateCommitChecks(target GitHubTarget, reading GitHubReading, output []byte) (GitHubReading, error) {
	var answer struct {
		Data struct {
			Repository *struct {
				Object *struct {
					OID               string `json:"oid"`
					URL               string `json:"url"`
					StatusCheckRollup *struct {
						State    string `json:"state"`
						Contexts struct {
							TotalCount int           `json:"totalCount"`
							Nodes      []gitHubCheck `json:"nodes"`
						} `json:"contexts"`
					} `json:"statusCheckRollup"`
				} `json:"object"`
			} `json:"repository"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(output, &answer); err != nil {
		return reading, fmt.Errorf("gh api graphql returned something other than the requested JSON: %w", err)
	}
	if len(answer.Errors) > 0 {
		return reading, errors.New("gh api graphql: " + summaryText(answer.Errors[0].Message, 400))
	}
	// The wording matches gitHubGonePattern on purpose: a repository or a
	// commit that does not exist will not appear later, so the wait gives up
	// at once instead of after three identical answers.
	if answer.Data.Repository == nil {
		return reading, fmt.Errorf("repository %s not found", target.Repo)
	}
	object := answer.Data.Repository.Object
	if object == nil || object.OID == "" {
		return reading, fmt.Errorf("commit %s not found in %s", target.ID, target.Repo)
	}
	reading.Fields["url"] = object.URL
	reading.Fields["head"] = object.OID
	if object.StatusCheckRollup == nil {
		return checksReading(reading, summarizeChecks(nil, 0, "")), nil
	}
	rollup := object.StatusCheckRollup
	return checksReading(reading, summarizeChecks(rollup.Contexts.Nodes, rollup.Contexts.TotalCount, rollup.State)), nil
}
