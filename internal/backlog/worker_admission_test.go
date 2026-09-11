package backlog

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkerAdmissionPolicyFailsClosedForOffersAndNewWorkCommands(t *testing.T) {
	policy := WorkerAdmissionPolicyFromQuotaReport(QuotaBridgeReport{Derived: []QuotaPoolAdmissionSnapshot{
		{QuotaPoolID: "open", Admission: domain.AdmissionOpen},
		{QuotaPoolID: "closed", Admission: domain.AdmissionClosed},
		{QuotaPoolID: "constrained", Admission: domain.AdmissionConstrained},
	}})
	assignments := []domain.Assignment{
		{ID: "assignment-open", AttemptID: "attempt-open", Route: domain.ProviderRoute{QuotaPoolID: "open"}},
		{ID: "assignment-closed", AttemptID: "attempt-closed", Route: domain.ProviderRoute{QuotaPoolID: "closed"}},
		{ID: "assignment-missing", AttemptID: "attempt-missing", Route: domain.ProviderRoute{QuotaPoolID: "missing"}},
	}
	allowedOffers, withheldOffers := policy.filterOffers(assignments, nil)
	if len(allowedOffers) != 1 || allowedOffers[0].ID != "assignment-open" || len(withheldOffers) != 2 {
		t.Fatalf("offers allowed=%+v withheld=%+v", allowedOffers, withheldOffers)
	}
	commands := []domain.WorkerCommand{
		{ID: "prepare-open", Kind: domain.WorkerCommandPrepare, AssignmentID: "assignment-open"},
		{ID: "dispatch-closed", Kind: domain.WorkerCommandDispatch, AssignmentID: "assignment-closed"},
		{ID: "prepare-missing", Kind: domain.WorkerCommandPrepare, AssignmentID: "assignment-missing"},
		{ID: "collect-closed", Kind: domain.WorkerCommandCollect, AssignmentID: "assignment-closed"},
		{ID: "stop-closed", Kind: domain.WorkerCommandStop, AssignmentID: "assignment-closed"},
	}
	allowedCommands, withheldCommands := policy.filterCommands(assignments, nil, commands)
	if got := admissionCommandIDs(allowedCommands); !equalStrings(got, []string{"collect-closed", "prepare-open", "stop-closed"}) {
		t.Fatalf("allowed commands = %v", got)
	}
	if got := admissionCommandIDs(withheldCommands); !equalStrings(got, []string{"dispatch-closed", "prepare-missing"}) {
		t.Fatalf("withheld commands = %v", got)
	}
	attempts := []domain.Attempt{{ID: "attempt-closed", AdminForceStart: true}}
	allowedOffers, withheldOffers = policy.filterOffers(assignments, attempts)
	if len(allowedOffers) != 2 || allowedOffers[1].ID != "assignment-closed" || len(withheldOffers) != 1 {
		t.Fatalf("forced offers allowed=%+v withheld=%+v", allowedOffers, withheldOffers)
	}
	allowedCommands, withheldCommands = policy.filterCommands(assignments, attempts, commands)
	if got := admissionCommandIDs(allowedCommands); !equalStrings(got, []string{"collect-closed", "dispatch-closed", "prepare-open", "stop-closed"}) {
		t.Fatalf("forced allowed commands = %v", got)
	}
	if got := admissionCommandIDs(withheldCommands); !equalStrings(got, []string{"prepare-missing"}) {
		t.Fatalf("forced withheld commands = %v", got)
	}
}
func admissionCommandIDs(commands []domain.WorkerCommand) []string {
	result := make([]string, len(commands))
	for index := range commands {
		result[index] = commands[index].ID
	}
	return result
}
