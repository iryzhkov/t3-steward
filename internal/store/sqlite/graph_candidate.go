package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func validateGraphCandidateTx(ctx context.Context, tx *sql.Tx, c GraphCommit, run domain.WorkflowRun, templates []domain.Task) error {
	records, err := nodeRecordsTx(ctx, tx)
	if err != nil {
		return err
	}
	request := c.Request
	if strings.Contains(request.Source, "/") {
		ref, err := domain.ParseNodeRef(request.Source)
		if err != nil {
			return err
		}
		obs, err := resolveNodeRecords(ref, records)
		if err != nil {
			return err
		}
		request.Source = obs.Target.String()
	}
	tasks, err := domain.AmendTasks(request, run, templates, "task:graph:"+request.ID, "input:graph:"+request.ID)
	if err != nil {
		return err
	}
	for i := range tasks {
		for j, ref := range tasks[i].ExternalNeeds {
			obs, err := resolveNodeRecords(ref, records)
			if err != nil {
				return err
			}
			tasks[i].ExternalNeeds[j] = obs.Target
		}
	}
	if !reflect.DeepEqual(tasks, c.Tasks) {
		return errors.New("prepared graph does not match amendment intent")
	}
	if request.Operation == "task-add" {
		if len(c.Inputs) != 1 || c.Inputs[0].ID != "input:graph:"+request.ID {
			return errors.New("task add requires its single prepared prompt")
		}
	} else if len(c.Inputs) != 0 {
		return errors.New("unexpected amendment input metadata")
	}
	return nil
}
