package campaign

import (
	"strings"
	"testing"
)

// A task that declares only a preset reads its expanded sizes back in the
// plan, so an author sees what the task will reserve before submitting it.
func TestResourcePresetsExpandSizes(t *testing.T) {
	plan := project(t, `version: 2
name: sizing
class: required
environment:
  project: example-project
routes:
  - instance: codex
    model: gpt
tasks:
  build:
    prompt_file: prompts/build.md
    resources: {preset: build}
  light:
    prompt_file: prompts/light.md
    resources: {preset: light, memory_mb: 2048}
`)
	build := taskByName(t, plan, "build")
	if build.Resources.CPUUnits == nil || *build.Resources.CPUUnits != 4 || build.Resources.MemoryMB == nil ||
		*build.Resources.MemoryMB != 6000 || build.Resources.ScratchMB == nil || *build.Resources.ScratchMB != 8192 {
		t.Fatalf("build resources = %+v", build.Resources)
	}
	light := taskByName(t, plan, "light")
	if light.Resources.CPUUnits == nil || *light.Resources.CPUUnits != .5 || light.Resources.MemoryMB == nil || *light.Resources.MemoryMB != 2048 {
		t.Fatalf("light resources = %+v, want the explicit memory to win", light.Resources)
	}
	text := RenderText(plan)
	for _, want := range []string{"preset build, min cpu medium, prefer cpu high, cpu units 4, memory 6000 MB, scratch 8192 MB", "cpu units 0.5, memory 2048 MB, scratch 512 MB"} {
		if !strings.Contains(text, want) {
			t.Fatalf("plan text does not show %q:\n%s", want, text)
		}
	}
}
