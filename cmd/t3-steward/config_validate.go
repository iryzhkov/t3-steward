package main

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/config"
)

// This argv-only exemption includes refused calls to the documented pure verb.
// It grants no exemption to other config verbs or ordinary runtime commands.
func isConfigValidateInvocation(args []string) bool {
	return len(args) >= 2 && args[0] == "config" && args[1] == "validate"
}

func cmdConfig(args []string) error {
	// A file value spelled "help" is data, never a help request.
	help := len(args) > 0 && isHelp(args[0]) ||
		len(args) > 1 && args[0] == "validate" && isHelp(args[1])
	if help {
		if answered, err := admitHelp(os.Stdout, []string{"config"}, args); answered || err != nil {
			if err != nil {
				return config.ErrStagedConfig
			}
			return nil
		}
	}
	if len(args) == 0 || args[0] != "validate" {
		return config.ErrStagedConfig
	}
	return cmdConfigValidate(args[1:])
}

func cmdConfigValidate(args []string) error {
	fs := flag.NewFlagSet("config validate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	file := fs.String("file", "", "private staged configuration file")
	asJSON := fs.Bool("json", false, "print validation JSON")
	// Only the documented adapter flags are admitted. Reject duplicates,
	// operands, global flags and bool=false rather than changing the contract.
	seenFile, seenJSON := false, false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--json":
			if seenJSON {
				return config.ErrStagedConfig
			}
			seenJSON = true
		case args[i] == "--file":
			if seenFile || i+1 >= len(args) {
				return config.ErrStagedConfig
			}
			seenFile = true
			i++
		case strings.HasPrefix(args[i], "--file="):
			if seenFile {
				return config.ErrStagedConfig
			}
			seenFile = true
		default:
			return config.ErrStagedConfig
		}
	}
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !seenFile || !seenJSON || !*asJSON {
		return config.ErrStagedConfig
	}
	digest, err := config.ValidateStagedFile(*file)
	if err != nil {
		return config.ErrStagedConfig
	}
	result := struct {
		SchemaVersion int    `json:"schema_version"`
		Kind          string `json:"kind"`
		Valid         bool   `json:"valid"`
		FileSHA256    string `json:"file_sha256"`
	}{1, "config-validation", true, digest}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return afterDocument(config.ErrStagedConfig)
	}
	return nil
}

func configHelpPages() []helpPage {
	return []helpPage{
		{Path: "config", Purpose: "Validate staged configuration without runtime effects.", Usage: []string{"t3-steward config validate --file PATH --json"}, Exits: localExits(), JSONNote: "Choose validate for the required JSON validation result.", Parsers: []parserSite{{Func: "cmdConfig"}}},
		{Path: "config validate", Purpose: "Validate exact private staged bytes and applicable original-HOME owned projections.", Usage: []string{"t3-steward config validate --file PATH --json"},
			Flags: []helpFlag{{Name: "--file", Value: "PATH", Required: true, Text: "Existing nonempty private 0600 configuration, at most 4 MiB."}, {Name: "--json", Required: true, Text: "Print one bounded validation JSON object."}},
			Exits: localExits(), JSONKeys: []string{"schema_version", "kind", "valid", "file_sha256"},
			Notes:   "Effective configuration includes applicable current-HOME coordinator fleet/client projections with runtime precedence. No environment overrides, credential resolution, runtime setup or instrumentation writes. Unsafe or changed inputs refuse with sanitized errors. Staged validation requires Linux ownership and change-time evidence.",
			Parsers: []parserSite{{Func: "cmdConfigValidate"}}},
	}
}
