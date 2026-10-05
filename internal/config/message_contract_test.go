package config

import (
	"strings"
	"testing"
)

func TestQuotaMessageContractExactMigration(t *testing.T) {
	oldDrain := `T3 steward quota drain: checkpoint and pause. "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Do not start new work or subagents. Ask active subagents to checkpoint and return partial results promptly. Preserve completed work and write a brief checkpoint covering current state, partial results, and remaining work. Then end your turn. T3 steward will interrupt any still-running turn in {{.GracePeriod}}.`
	oldResume := `T3 steward quota recovery: quota is available again. Resume the unfinished user task from the latest checkpoint. First inspect current user instructions, repository state, and saved partial results. Verify whether previous subagents are still running before relying on them. Continue only unfinished work within the current authorized scope.`
	for _, custom := range []bool{false, true} {
		for _, suffix := range []string{"", "\n"} {
			c := Default()
			c.Messages.Drain = oldDrain + suffix
			c.Resume.Prompt = oldResume + suffix
			if custom {
				c.Messages.Drain += " custom"
				c.Resume.Prompt += " custom"
			}
			drain, resume := c.Messages.Drain, c.Resume.Prompt
			c.migrateQuotaDefaults()
			if custom {
				if c.Messages.Drain != drain || c.Resume.Prompt != resume {
					t.Fatal("custom bytes changed")
				}
			} else {
				if c.Messages.Drain != DefaultDrainMessage || c.Resume.Prompt != DefaultResumePrompt {
					t.Fatal("prior shipped default did not migrate")
				}
				if strings.Contains(c.Messages.Drain, "will interrupt any") || strings.Contains(c.Resume.Prompt, "quota is available again") {
					t.Error("shipped migration still misleading")
				}
			}
		}
	}
}
