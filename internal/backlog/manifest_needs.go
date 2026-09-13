package backlog

import (
	"fmt"
	"gopkg.in/yaml.v3"
)

type ManifestNeeds []string

func (n *ManifestNeeds) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		if value.Tag != "!!str" {
			return fmt.Errorf("needs must be a node reference or string list")
		}
		*n = []string{value.Value}
		return nil
	case yaml.SequenceNode:
		var values []string
		for _, child := range value.Content {
			if child.Kind != yaml.ScalarNode || child.Tag != "!!str" {
				return fmt.Errorf("needs entries must be strings")
			}
			values = append(values, child.Value)
		}
		*n = values
		return nil
	default:
		return fmt.Errorf("needs must be a node reference or string list")
	}
}
