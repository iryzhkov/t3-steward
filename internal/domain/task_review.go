package domain

// TaskReviewRequirements is the versioned immutable declaration compiled at submission.
// Criteria binds retained input custody; no worker or caller classification is stored here.
type TaskReviewRequirements struct {
	Version             int                `json:"version"`
	Risk                string             `json:"risk"`
	RequiredReviewers   int                `json:"requiredReviewers"`
	MinProviderFamilies int                `json:"minProviderFamilies"`
	RoundLimit          int                `json:"roundLimit"`
	Members             []TaskReviewMember `json:"members"`
	Criteria            ReviewCriteria     `json:"criteria"`
}
type TaskReviewMember struct {
	ID       string `json:"id"`
	Role     string `json:"role"`
	Route    string `json:"route"`
	Required bool   `json:"required"`
}
type ReviewCriteria struct {
	ArtifactID string `json:"artifactId"`
	RunID      string `json:"runId"`
	WorkflowID string `json:"workflowId"`
	TaskID     string `json:"taskId"`
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
}

func CloneTaskReview(r *TaskReviewRequirements) *TaskReviewRequirements {
	if r == nil {
		return nil
	}
	copied := *r
	copied.Members = append([]TaskReviewMember(nil), r.Members...)
	return &copied
}
