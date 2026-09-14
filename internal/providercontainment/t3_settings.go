package providercontainment

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// prepareT3Settings writes only execution-local settings before the server starts.
// Existing settings, including symlinks, are recovery evidence and never replaced.
// No host provider configuration or credentials are copied into the execution.
func prepareT3Settings(root string, spec T3Spec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	if err := privateDirectory(root); err != nil {
		return err
	}
	providers := map[string]any{}
	for _, name := range []string{"codex", "claudeAgent", "cursor", "grok", "opencode"} {
		providers[name] = map[string]any{"enabled": false}
	}
	settings := map[string]any{
		"enableProviderUpdateChecks": false,
		"enableAgentBrowserAccess":   false,
		"providers":                  providers,
	}
	if spec.OpenCodeBinary != "" {
		providers["opencode"] = map[string]any{
			"enabled": true, "binaryPath": spec.OpenCodeBinary,
			"serverUrl": "", "serverPassword": "",
			"customModels": []string{spec.OpenCodeModel},
		}
		settings["textGenerationModelSelection"] = map[string]any{
			"instanceId": "opencode", "model": spec.OpenCodeModel,
		}
	}
	data, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(root, ".settings-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	// Link is an atomic, no-replace publication, including for dangling symlinks.
	if err := os.Link(file.Name(), filepath.Join(root, "settings.json")); err != nil {
		return err
	}
	return syncDirectory(root)
}
