package domain

// ReprioritizeReceipt records the transaction's declared-task selection.
// Lists are always present, including when empty.
type ReprioritizeReceipt struct {
	Importance int                      `json:"importance"`
	Changed    []ReprioritizeTaskRecord `json:"changed"`
	Skipped    []ReprioritizeTaskRecord `json:"skipped"`
}
type ReprioritizeTaskRecord struct {
	TaskID   string `json:"taskId"`
	TaskName string `json:"taskName"`
	Reason   string `json:"reason,omitempty"`
}
