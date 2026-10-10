//go:build !linux

package config

// Staged validation fails closed without descriptor-relative ownership,
// hardlink and change-time evidence. Runtime loaders remain unchanged.
type stagedInput struct{ raw []byte }

func observeStagedInput(string, int64, bool) (*stagedInput, error) { return nil, ErrStagedConfig }
func (*stagedInput) check() error                                  { return ErrStagedConfig }
