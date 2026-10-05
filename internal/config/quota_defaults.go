package config

import (
	"strings"
	"time"
)

// legacyWarnMessage and the exact 95/97/99 secondary override were emitted by
// Sample before Codex began reporting weekly quota as primary.
const legacyWarnMessage = `Provider quota warning from the T3 quota watchdog: "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Do not start new subagents. Ask active subagents to checkpoint and return their results, consolidate the current work, then stop at a clean point.`

const legacyDrainMessage = `Provider quota is nearly exhausted (T3 quota watchdog): "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Stop spawning subagents now. Cancel or finish active subagents, collect their results, write a short checkpoint of the current state and remaining work, then stop. The session will be interrupted in {{.GracePeriod}} if it is still running.`
const legacyResumePrompt = `The provider quota has recovered (T3 quota watchdog). Resume the interrupted task from the latest checkpoint. First inspect the current thread, repository state, and any partial results. Do not assume previous subagents are still running. Continue only the unfinished work, and create new subagents only when needed.`

const previousDefaultDrainMessage = `T3 steward quota drain: checkpoint and pause. "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Do not start new work or subagents. Ask active subagents to checkpoint and return partial results promptly. Preserve completed work and write a brief checkpoint covering current state, partial results, and remaining work. Then end your turn. T3 steward will interrupt any still-running turn in {{.GracePeriod}}.`
const previousDefaultResumePrompt = `T3 steward quota recovery: quota is available again. Resume the unfinished user task from the latest checkpoint. First inspect current user instructions, repository state, and saved partial results. Verify whether previous subagents are still running before relying on them. Continue only unfinished work within the current authorized scope.`

// migrateQuotaDefaults upgrades only the old shipped defaults in memory.
// Keep customized messages, selectors, thresholds and grace periods intact;
// never rewrite the operator's file or touch worker/coordinator settings.
func (c *Config) migrateQuotaDefaults() {
	if strings.TrimSpace(c.Messages.Warn) == legacyWarnMessage {
		c.Messages.Warn = DefaultWarnMessage
	}
	if strings.TrimSpace(c.Messages.Drain) == legacyDrainMessage || c.Messages.Drain == previousDefaultDrainMessage || c.Messages.Drain == previousDefaultDrainMessage+"\n" {
		c.Messages.Drain = DefaultDrainMessage
	}
	if strings.TrimSpace(c.Resume.Prompt) == legacyResumePrompt || c.Resume.Prompt == previousDefaultResumePrompt || c.Resume.Prompt == previousDefaultResumePrompt+"\n" {
		c.Resume.Prompt = DefaultResumePrompt
	}
	for i := range c.Overrides {
		o := &c.Overrides[i]
		if o.Match.Provider == "codex" && o.Match.Window == "secondary" &&
			o.Match.LimitName == "" && o.Match.MinWindowDuration == 0 &&
			o.WarnPercent != nil && *o.WarnPercent == 95 &&
			o.DrainPercent != nil && *o.DrainPercent == 97 &&
			o.StopPercent != nil && *o.StopPercent == 99 && o.GracePeriod == nil {
			o.Match.Window = ""
			o.Match.MinWindowDuration = Duration(7 * 24 * time.Hour)
		}
	}
}
