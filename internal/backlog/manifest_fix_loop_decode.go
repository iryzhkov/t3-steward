package backlog

import (
	"fmt"
	"gopkg.in/yaml.v3"
)

// UnmarshalYAML refuses scalar coercion for loop declarations. In particular,
// yaml.v3 normally truncates floats when decoding an int.
func (loop *ManifestFixLoop) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("fix loop must be a mapping")
	}
	for i := 0; i < len(value.Content); i += 2 {
		key, child := value.Content[i], value.Content[i+1]
		// Resolve scalar anchors without permitting alias cycles.
		seen := map[*yaml.Node]bool{}
		for child.Kind == yaml.AliasNode {
			if seen[child] || child.Alias == nil {
				return fmt.Errorf("fix loop %s has invalid alias", key.Value)
			}
			seen[child] = true
			child = child.Alias
		}
		switch key.Value {
		case "implement", "review":
			if child.Kind != yaml.ScalarNode || child.Tag != "!!str" {
				return fmt.Errorf("fix loop %s must be a task name string", key.Value)
			}
		case "max_rounds":
			if child.Kind != yaml.ScalarNode || child.Tag != "!!int" {
				return fmt.Errorf("fix loop max_rounds must be an integer")
			}
		default:
			// Preserve the standard strict decoder's field/line diagnostic contract.
			return &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: field %s not found in type backlog.ManifestFixLoop", key.Line, key.Value)}}
		}
	}
	type plain ManifestFixLoop
	var decoded plain
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*loop = ManifestFixLoop(decoded)
	return nil
}
