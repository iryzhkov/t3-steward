package wait

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Unlike gh run/pr view, these fixed API requests cannot perform hidden
// workflow lookups or rollup pagination inside one counted runner invocation.
const annotationRecheckQuery = `query($owner:String!,$name:String!,$number:Int!){
 repository(owner:$owner,name:$name){pullRequest(number:$number){
  headRefOid state commits(last:1){nodes{commit{statusCheckRollup{
   contexts(first:100){nodes{__typename
    ... on CheckRun{name status conclusion startedAt completedAt detailsUrl}
    ... on StatusContext{context state targetUrl}
   } pageInfo{hasNextPage}}
  }}}}
 }}
}`

func (c *annotationCollection) consistent(t GitHubTarget, snapshot annotationSnapshot) error {
	var args []string
	if t.Kind == "run" {
		args = []string{"api", "--method", "GET", "--hostname", "github.com", fmt.Sprintf("repos/%s/actions/runs/%d?exclude_pull_requests=true", c.repo, snapshot.ID)}
	} else {
		names := strings.Split(c.repo, "/")
		args = []string{"api", "--method", "POST", "--hostname", "github.com", "graphql", "-f", "query=" + annotationRecheckQuery,
			"-F", "owner=" + names[0], "-F", "name=" + names[1], "-F", "number=" + t.ID}
	}
	output, err := c.fetch(args, true)
	if err != nil {
		return err
	}
	if !json.Valid([]byte(output)) {
		return errors.New("malformed-recheck")
	}
	if t.Kind == "run" {
		var run struct {
			ID         int64  `json:"id"`
			Attempt    int    `json:"run_attempt"`
			Head       string `json:"head_sha"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
			URL        string `json:"html_url"`
		}
		if json.Unmarshal([]byte(output), &run) != nil {
			return errors.New("malformed-recheck")
		}
		if run.ID != snapshot.ID || run.Attempt != snapshot.Attempt || run.Head != snapshot.Head ||
			run.Status != snapshot.Status || run.Conclusion != snapshot.Conclusion || run.URL != snapshot.URL {
			return errors.New("snapshot-changed")
		}
		return nil
	}
	var response struct {
		Errors json.RawMessage `json:"errors"`
		Data   struct {
			Repository *struct {
				PR *struct {
					Head    string `json:"headRefOid"`
					State   string `json:"state"`
					Commits struct {
						Nodes []struct {
							Commit struct {
								Rollup *struct {
									Contexts struct {
										Nodes    []annotationRollup `json:"nodes"`
										PageInfo struct {
											HasNext *bool `json:"hasNextPage"`
										} `json:"pageInfo"`
									} `json:"contexts"`
								} `json:"statusCheckRollup"`
							} `json:"commit"`
						} `json:"nodes"`
					} `json:"commits"`
				} `json:"pullRequest"`
			} `json:"repository"`
		} `json:"data"`
	}
	if json.Unmarshal([]byte(output), &response) != nil || (len(response.Errors) > 0 && string(response.Errors) != "null" && string(response.Errors) != "[]") ||
		response.Data.Repository == nil || response.Data.Repository.PR == nil {
		return errors.New("malformed-recheck")
	}
	pr := response.Data.Repository.PR
	if pr.Head != snapshot.PRHead || pr.State != snapshot.State {
		return errors.New("snapshot-changed")
	}
	if len(pr.Commits.Nodes) != 1 || pr.Commits.Nodes[0].Commit.Rollup == nil {
		return errors.New("malformed-recheck")
	}
	contexts := pr.Commits.Nodes[0].Commit.Rollup.Contexts
	if contexts.PageInfo.HasNext == nil || contexts.Nodes == nil || len(contexts.Nodes) > 100 {
		return errors.New("malformed-recheck")
	}
	if *contexts.PageInfo.HasNext {
		return errors.New("recheck-page-cap")
	}
	if !reflect.DeepEqual(contexts.Nodes, snapshot.Checks) {
		return errors.New("snapshot-changed")
	}
	// The constructed API request is anchored in the already validated repository
	// and PR number; no URL returned by the API is followed.
	if _, err := strconv.ParseInt(t.ID, 10, 64); err != nil {
		return errors.New("target-mismatch")
	}
	return nil
}
