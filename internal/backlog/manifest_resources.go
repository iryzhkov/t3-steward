package backlog

import "fmt"

// CPUClass is the operator-assigned performance class of a worker's CPU, used
// as a placement constraint floor and as a soft preference. It is an ordered
// value: low < medium < high.
//
// CPUClass is deliberately not domain.TaskClass and does not belong in the
// domain package yet. domain.TaskClass is the required-versus-surplus
// admission classification of work; CPUClass describes hardware performance.
// The two axes share the English word "class" and nothing else, so keeping
// CPUClass local to the manifest package until a consumer needs it elsewhere
// prevents the two from being confused at a call site or merged by accident.
type CPUClass string

const (
	CPUClassLow    CPUClass = "low"
	CPUClassMedium CPUClass = "medium"
	CPUClassHigh   CPUClass = "high"
)

// Resource presets expand to a class floor and preference. An explicitly
// declared field always wins over what a preset would have expanded to.
const (
	ResourcePresetBuild = "build"
	ResourcePresetLight = "light"
)

// rank orders the classes. An unknown or unset class ranks below every valid
// class, so comparisons on unvalidated input never claim a bad value is high.
func (c CPUClass) rank() int {
	switch c {
	case CPUClassLow:
		return 1
	case CPUClassMedium:
		return 2
	case CPUClassHigh:
		return 3
	default:
		return 0
	}
}

// Valid reports whether the class is one of the three declared values.
func (c CPUClass) Valid() bool { return c.rank() != 0 }

// Compare returns a negative number when c is the lower class, zero when the
// two are equal, and a positive number when c is the higher class.
func (c CPUClass) Compare(other CPUClass) int { return c.rank() - other.rank() }

// ManifestResources declares the resource demand of a workflow or of one task.
// Every field is optional. Numeric fields are pointers so that an explicit
// zero, which is always a mistake, is distinguishable from an absent field and
// can be rejected by name.
type ManifestResources struct {
	Preset            string   `yaml:"preset"`
	MinCPUClass       CPUClass `yaml:"min_cpu_class"`
	PreferredCPUClass CPUClass `yaml:"preferred_cpu_class"`
	CPUUnits          *float64 `yaml:"cpu_units"`
	MemoryMB          *int     `yaml:"memory_mb"`
	ScratchMB         *int     `yaml:"scratch_mb"`
}

// expandResourcePreset fills the fields a preset implies, leaving every field
// the author declared explicitly untouched. Precedence is therefore: an
// explicit field beats the preset, and the preset beats nothing at all. An
// unknown preset name expands nothing; validateResources rejects it by name.
func expandResourcePreset(resources *ManifestResources) {
	switch resources.Preset {
	case ResourcePresetBuild:
		if resources.MinCPUClass == "" {
			resources.MinCPUClass = CPUClassMedium
		}
		if resources.PreferredCPUClass == "" {
			resources.PreferredCPUClass = CPUClassHigh
		}
	case ResourcePresetLight:
		if resources.MinCPUClass == "" {
			resources.MinCPUClass = CPUClassLow
		}
	}
}

// effectiveResources merges a workflow-level declaration into a task-level one,
// field by field, with the task winning wherever it declared something.
//
// This is an override rather than the intersection effectivePlacement performs
// on hosts. Placement can intersect because a host list is a set of eligible
// hosts and both sides must be satisfied at once, so narrowing is meaningful.
// A resource scalar has no intersection: two different numbers cannot both
// describe one reservation, and silently taking the larger would let a
// workflow-level default inflate every task that tried to ask for less. The
// precedent for that choice is already in this file's neighbour: task routes
// replace inherited workflow routes instead of merging with them.
func effectiveResources(workflow, task ManifestResources) ManifestResources {
	result := workflow
	if task.Preset != "" {
		result.Preset = task.Preset
	}
	if task.MinCPUClass != "" {
		result.MinCPUClass = task.MinCPUClass
	}
	if task.PreferredCPUClass != "" {
		result.PreferredCPUClass = task.PreferredCPUClass
	}
	if task.CPUUnits != nil {
		value := *task.CPUUnits
		result.CPUUnits = &value
	}
	if task.MemoryMB != nil {
		value := *task.MemoryMB
		result.MemoryMB = &value
	}
	if task.ScratchMB != nil {
		value := *task.ScratchMB
		result.ScratchMB = &value
	}
	return result
}

// validateResources rejects a resource declaration, naming the offending field
// and value so that an author of a hundred-task batch can find it.
func validateResources(label string, resources ManifestResources) error {
	switch resources.Preset {
	case "", ResourcePresetBuild, ResourcePresetLight:
	default:
		return fmt.Errorf("%s preset %q is not one of build, light", label, resources.Preset)
	}
	if resources.MinCPUClass != "" && !resources.MinCPUClass.Valid() {
		return fmt.Errorf("%s min_cpu_class %q is not one of low, medium, high", label, resources.MinCPUClass)
	}
	if resources.PreferredCPUClass != "" && !resources.PreferredCPUClass.Valid() {
		return fmt.Errorf("%s preferred_cpu_class %q is not one of low, medium, high", label, resources.PreferredCPUClass)
	}
	if resources.MinCPUClass != "" && resources.PreferredCPUClass != "" &&
		resources.PreferredCPUClass.Compare(resources.MinCPUClass) < 0 {
		return fmt.Errorf("%s preferred_cpu_class %q is lower than min_cpu_class %q",
			label, resources.PreferredCPUClass, resources.MinCPUClass)
	}
	if resources.CPUUnits != nil && *resources.CPUUnits <= 0 {
		return fmt.Errorf("%s cpu_units must be positive, got %v", label, *resources.CPUUnits)
	}
	if resources.MemoryMB != nil && *resources.MemoryMB <= 0 {
		return fmt.Errorf("%s memory_mb must be positive, got %d", label, *resources.MemoryMB)
	}
	if resources.ScratchMB != nil && *resources.ScratchMB <= 0 {
		return fmt.Errorf("%s scratch_mb must be positive, got %d", label, *resources.ScratchMB)
	}
	return nil
}
