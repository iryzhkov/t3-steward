package backlog

import (
	"reflect"
	"strings"
)

// suggestManifestField returns the known field of the object type typeName,
// found anywhere in the shape of into, that is closest to the unknown field,
// when it is close enough to be a typo: one edit for a name of up to five
// characters, two for a longer one. It returns "" when there is no such field,
// so a field from a newer release keeps the newer-release advice.
func suggestManifestField(into any, typeName, field string) string {
	known := map[string][]string{}
	collectYAMLFields(reflect.TypeOf(into), known, map[reflect.Type]bool{})
	limit := 1
	if len(field) > 5 {
		limit = 2
	}
	best, bestDistance := "", limit+1
	for _, candidate := range known[typeName] {
		if distance := EditDistance(strings.ToLower(field), candidate); distance < bestDistance {
			best, bestDistance = candidate, distance
		}
	}
	return best
}

// collectYAMLFields records, for every struct type reachable from t, the yaml
// names of its fields under the type name the yaml decoder reports, such as
// "backlog.ManifestTask". Inline fields contribute to their parent.
func collectYAMLFields(t reflect.Type, known map[string][]string, seen map[reflect.Type]bool) {
	for t != nil && (t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Map) {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct || seen[t] {
		return
	}
	seen[t] = true
	names := yamlFieldNames(t, known, seen)
	known[t.String()] = append(known[t.String()], names...)
}

func yamlFieldNames(t reflect.Type, known map[string][]string, seen map[reflect.Type]bool) []string {
	var names []string
	for index := range t.NumField() {
		field := t.Field(index)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("yaml")
		name, options, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		if strings.Contains(options, "inline") {
			inner := field.Type
			for inner.Kind() == reflect.Pointer {
				inner = inner.Elem()
			}
			if inner.Kind() == reflect.Struct {
				names = append(names, yamlFieldNames(inner, known, seen)...)
			}
			continue
		}
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		names = append(names, name)
		collectYAMLFields(field.Type, known, seen)
	}
	return names
}

// EditDistance is the Levenshtein distance between a and b, counted in bytes.
// The command line uses it for its own did-you-mean suggestions.
func EditDistance(a, b string) int {
	previous := make([]int, len(b)+1)
	current := make([]int, len(b)+1)
	for j := range previous {
		previous[j] = j
	}
	for i := 1; i <= len(a); i++ {
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			current[j] = min(previous[j]+1, current[j-1]+1, previous[j-1]+cost)
		}
		previous, current = current, previous
	}
	return previous[len(b)]
}
