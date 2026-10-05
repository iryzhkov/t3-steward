package backlog

import (
	"fmt"
	"gopkg.in/yaml.v3"
)

// Bound the authored member graph before marshaling or decoding it. Aliases are
// followed with an active path, rather than an unbounded recursive merge walk.
func validateReviewMemberNodes(root *yaml.Node) error {
	active := make(map[*yaml.Node]bool)
	visits := 0
	var walk func(*yaml.Node, int) error
	walk = func(n *yaml.Node, depth int) error {
		visits++
		if n == nil || depth > 64 || visits > 4096 {
			return fmt.Errorf("review member YAML exceeds node/depth limits")
		}
		if active[n] {
			return fmt.Errorf("review member YAML contains a cycle")
		}
		active[n] = true
		defer delete(active, n)
		if n.Kind == yaml.AliasNode {
			return walk(n.Alias, depth+1)
		}
		for _, child := range n.Content {
			if err := walk(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root, 0)
}

type reviewNodeLookup struct{ visits int }

func (l *reviewNodeLookup) resolve(n *yaml.Node) (*yaml.Node, error) {
	for depth := 0; ; depth++ {
		l.visits++
		if n == nil || depth > 64 || l.visits > 32768 {
			return nil, fmt.Errorf("review member YAML lookup exceeds limits")
		}
		if n.Kind != yaml.AliasNode {
			return n, nil
		}
		n = n.Alias
	}
}

// yaml.v3 gives explicit keys priority over merges, and the first mapping in
// a merge sequence priority over later mappings. Presence is separate from
// value, since an effective null execution is still a declaration.
func (l *reviewNodeLookup) field(n *yaml.Node, name string) (*yaml.Node, bool, error) {
	n, err := l.resolve(n)
	if err != nil {
		return nil, false, err
	}
	if n.Kind != yaml.MappingNode || len(n.Content)%2 != 0 {
		return nil, false, fmt.Errorf("review member YAML requires a mapping")
	}
	var merge *yaml.Node
	for i := 0; i < len(n.Content); i += 2 {
		key := n.Content[i]
		if key.Kind != yaml.ScalarNode {
			return nil, false, fmt.Errorf("review member YAML requires scalar keys")
		}
		if key.Tag == "!!merge" {
			merge = n.Content[i+1]
		} else if key.Value == name {
			return n.Content[i+1], true, nil
		}
	}
	if merge == nil {
		return nil, false, nil
	}
	merge, err = l.resolve(merge)
	if err != nil {
		return nil, false, err
	}
	if merge.Kind == yaml.SequenceNode {
		for _, item := range merge.Content {
			value, found, err := l.field(item, name)
			if err != nil || found {
				return value, found, err
			}
		}
		return nil, false, nil
	}
	return l.field(merge, name)
}

func (l *reviewNodeLookup) integer(n *yaml.Node, name string) error {
	value, found, err := l.field(n, name)
	if err != nil || !found {
		return err
	}
	value, err = l.resolve(value)
	if err != nil {
		return err
	}
	// Decode only after proving this is an integer scalar. yaml.v3 otherwise
	// permits a float to be truncated when the destination is int or *int.
	if value.Kind != yaml.ScalarNode || value.Tag != "!!int" || len(value.Value) > 128 {
		return fmt.Errorf("review execution %s must be a YAML integer scalar", name)
	}
	var integer int
	if err := value.Decode(&integer); err != nil {
		return fmt.Errorf("review execution %s integer: %w", name, err)
	}
	return nil
}

func reviewExecutionNodePresence(node *yaml.Node) (bool, error) {
	if err := validateReviewMemberNodes(node); err != nil {
		return false, err
	}
	lookup := reviewNodeLookup{}
	execution, found, err := lookup.field(node, "execution")
	if err != nil || !found {
		return found, err
	}
	execution, err = lookup.resolve(execution)
	if err != nil {
		return found, err
	}
	if execution.Kind == yaml.ScalarNode && execution.Tag == "!!null" {
		return true, nil
	}
	if err := lookup.integer(execution, "max_turns"); err != nil {
		return true, err
	}
	resources, present, err := lookup.field(execution, "resources")
	if err != nil || !present {
		return true, err
	}
	resources, err = lookup.resolve(resources)
	if err != nil {
		return true, err
	}
	if resources.Kind == yaml.ScalarNode && resources.Tag == "!!null" {
		return true, nil
	}
	for _, name := range []string{"memory_mb", "scratch_mb"} {
		if err := lookup.integer(resources, name); err != nil {
			return true, err
		}
	}
	return true, nil
}
