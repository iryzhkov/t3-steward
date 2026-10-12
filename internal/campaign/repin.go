package campaign

import (
	"bytes"
	"fmt"
	"regexp"

	"gopkg.in/yaml.v3"
)

// RepinnedUnit is an in-memory recompilation; publication belongs to the caller.
type RepinnedUnit struct {
	Plan           []byte
	Unit           CompiledUnit
	OldRef, NewRef string
}

// RepinCompileUnit replaces full pin mentions in plan text, never in extra inputs.
func RepinCompileUnit(raw []byte, id, target string, options CompileOptions) (RepinnedUnit, error) {
	if !compileRefPattern.MatchString(target) {
		return RepinnedUnit{}, fmt.Errorf("invalid target SHA %q", target)
	}
	if err := supportedRepinRef(raw); err != nil {
		return RepinnedUnit{}, err
	}
	original, err := ParseCompilePlanWith("plan.md", raw, options)
	if err != nil {
		return RepinnedUnit{}, err
	}
	updated := bytes.ReplaceAll(raw, []byte(original.Ref), []byte(target))
	plan, err := ParseCompilePlanWith("plan.md", updated, options)
	if err != nil {
		return RepinnedUnit{}, err
	}
	if plan.Ref != target {
		return RepinnedUnit{}, fmt.Errorf("repin decoded ref differs from target")
	}
	for _, unit := range plan.Units {
		if unit.ID == id {
			return RepinnedUnit{updated, unit, original.Ref, plan.Ref}, nil
		}
	}
	return RepinnedUnit{}, fmt.Errorf("plan declares no unit %q", id)
}

// supportedRepinRef is the single fail-closed syntax guard, including same-pin calls.
// It inspects only the root and ref; unrelated YAML fields retain compiler semantics.
func supportedRepinRef(raw []byte) error {
	unsupported := fmt.Errorf("unsupported ref syntax")
	lines := splitPlanLines(raw)
	if len(lines) < 3 || planLineText(raw, lines[0]) != "---" {
		return unsupported
	}
	end := 1
	for end < len(lines) && planLineText(raw, lines[end]) != "---" {
		end++
	}
	if end == len(lines) {
		return unsupported
	}
	var doc yaml.Node
	if yaml.Unmarshal(append([]byte("\n"), raw[lines[1].start:lines[end].start]...), &doc) != nil || len(doc.Content) != 1 {
		return unsupported
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode || root.Style != 0 || root.Anchor != "" {
		return unsupported
	}
	var ref *yaml.Node
	for i := 0; i < len(root.Content); i += 2 {
		key, value := root.Content[i], root.Content[i+1]
		if key.Value == "<<" {
			return unsupported
		}
		if key.Value == "ref" {
			if ref != nil || key.Anchor != "" {
				return unsupported
			}
			ref = value
		}
	}
	if ref == nil || ref.Kind != yaml.ScalarNode || ref.Anchor != "" ||
		(ref.Style != 0 && ref.Style != yaml.SingleQuotedStyle && ref.Style != yaml.DoubleQuotedStyle) {
		return unsupported
	}
	if ref.Line < 1 || ref.Line > end {
		return unsupported
	}
	// Match the physical scalar, refusing tags, escapes, aliases and multiline forms.
	pin := regexp.QuoteMeta(ref.Value)
	line := planLineText(raw, lines[ref.Line-1])
	if !regexp.MustCompile(`^ref:[ \t]+(?:` + pin + `|'` + pin + `'|"` + pin + `")[ \t]*(?:#.*)?$`).MatchString(line) {
		return unsupported
	}
	return nil
}
