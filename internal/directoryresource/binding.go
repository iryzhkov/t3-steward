package directoryresource

import "fmt"

// ValidateBinding requires an explicit resolved access mode and complete inode
// evidence. Only worker inspection plus operator registration establishes trust.
func ValidateBinding(binding Binding) error {
	if binding.Access != ReadOnly && binding.Access != ReadWrite {
		return fmt.Errorf("directory binding access is unresolved")
	}
	_, err := Bind(binding.Identity, binding.Access)
	if err != nil {
		return err
	}
	for _, ancestor := range binding.Identity.Ancestors {
		if ancestor.Inode == 0 {
			return fmt.Errorf("directory ancestor identity is incomplete")
		}
	}
	return nil
}

func CloneBindings(bindings []Binding) []Binding {
	if bindings == nil {
		return nil
	}
	out := append([]Binding(nil), bindings...)
	for i := range out {
		out[i].Identity.Ancestors = append([]Object(nil), out[i].Identity.Ancestors...)
	}
	return out
}
