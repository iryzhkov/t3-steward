package t3api

import (
	"errors"
	"slices"
	"strings"
)

// BuildModelSelection translates authorized route options for a fresh thread.
// Only the audited standard Codex instance uses reasoningEffort; custom
// instances and other providers retain their original option IDs and values.
func BuildModelSelection(instanceID, model string, routeOptions map[string]string) (map[string]any, error) {
	options := make(map[string]string, len(routeOptions))
	for id, value := range routeOptions {
		options[id] = value
	}
	if instanceID == "codex" {
		generic, hasGeneric := options["effort"]
		canonical, hasCanonical := options["reasoningEffort"]
		if (hasGeneric && strings.TrimSpace(generic) == "") || (hasCanonical && strings.TrimSpace(canonical) == "") {
			return nil, errors.New("standard Codex effort must not be blank")
		}
		if hasGeneric && hasCanonical && generic != canonical {
			return nil, errors.New("standard Codex effort and reasoningEffort conflict")
		}
		if hasGeneric {
			options["reasoningEffort"] = generic
			delete(options, "effort")
		}
	}
	selection := map[string]any{"instanceId": instanceID, "model": model}
	if len(options) != 0 {
		keys := make([]string, 0, len(options))
		for id := range options {
			keys = append(keys, id)
		}
		slices.Sort(keys)
		values := make([]any, 0, len(keys))
		for _, id := range keys {
			values = append(values, map[string]any{"id": id, "value": options[id]})
		}
		selection["options"] = values
	}
	return selection, nil
}
