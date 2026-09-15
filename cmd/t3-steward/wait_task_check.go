package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func saveRegisteredTaskCheck(ctx context.Context, store *sqlite.Store, local wait.Wait) error {
	checks, err := store.ListWaits(ctx, "")
	if err != nil {
		return err
	}
	for _, check := range checks {
		if check.TaskWaitID == local.TaskWaitID {
			return nil
		}
	}
	return store.InsertTaskWaitCheck(ctx, local)
}
