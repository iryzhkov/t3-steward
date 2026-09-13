package main

import (
	"context"
	"os/exec"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

func observeHostInventory(control *t3control.Control, dataDir string) func(context.Context, config.BacklogV2, domain.WorkerInventory) (domain.WorkerInventory, error) {
	return func(ctx context.Context, settings config.BacklogV2, wanted domain.WorkerInventory) (domain.WorkerInventory, error) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		projects, projectErr := control.ListProjects(ctx)
		probe := workerruntime.HostInventoryProbe{
			DataDir: dataDir,
			Capability: func(ctx context.Context, name string) bool {
				switch name {
				case "git":
					_, err := exec.LookPath("git")
					return err == nil
				case "huyang":
					return exec.CommandContext(ctx, "systemctl", "--user", "is-active", "--quiet", "huyang.service").Run() == nil
				default:
					return false
				}
			},
			ProjectAvailable: func(_ context.Context, name string) (bool, error) {
				if projectErr != nil {
					return false, projectErr
				}
				binding, ok := settings.Projects[name]
				if !ok {
					return false, nil
				}
				for _, project := range projects {
					if project.ID == binding.T3Project || project.Title == binding.T3Project {
						return true, nil
					}
				}
				return false, nil
			},
		}
		return probe.Observe(ctx, wanted)
	}
}
