package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

// Gate declarations must fit the coordinator's structured evidence budget even
// after JSON escaping, timings, toolchain metadata and a command failure.
const (
	MaxGateCommands      = 64
	MaxGateCommandBytes  = 4096
	MaxGateCommandsBytes = 16 << 10
)

func (g TaskGate) Validate() error {
	if g.Timeout <= 0 || g.Timeout > 6*time.Hour || len(g.Commands) == 0 || len(g.Commands) > MaxGateCommands {
		return errors.New("gate requires 1..64 commands and timeout in (0, 6h]")
	}
	total := 0
	seen := map[string]bool{}
	for _, command := range g.Commands {
		if strings.TrimSpace(command) == "" || strings.ContainsRune(command, 0) || !utf8.ValidString(command) || len(command) > MaxGateCommandBytes || seen[command] {
			return errors.New("gate command must be unique, valid UTF-8, nonempty, NUL-free and at most 4096 bytes")
		}
		seen[command] = true
		total += len(command)
		if total > MaxGateCommandsBytes {
			return errors.New("gate commands exceed 16 KiB")
		}
	}
	return nil
}
