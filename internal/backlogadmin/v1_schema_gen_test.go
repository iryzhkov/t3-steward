package backlogadmin

import (
	"encoding"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// jsonKeySchema lists every JSON key path a strict decoder of t accepts, in
// the format of v1_response_schema.txt: one path per line, "." separated
// from the root, "*" for the keys of a map, and a trailing "!" on a value
// the decoder takes whole (a custom unmarshaler, an interface, raw JSON, or
// a type that contains itself), whose inside is not checked.
func jsonKeySchema(t reflect.Type) []string {
	paths := map[string]bool{}
	var walk func(t reflect.Type, path string, stack map[reflect.Type]bool)
	unmarshaler := reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	textUnmarshaler := reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	walk = func(t reflect.Type, path string, stack map[reflect.Type]bool) {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if reflect.PointerTo(t).Implements(unmarshaler) || reflect.PointerTo(t).Implements(textUnmarshaler) {
			paths[path+"!"] = true
			return
		}
		switch t.Kind() {
		case reflect.Interface:
			paths[path+"!"] = true
		case reflect.Slice, reflect.Array:
			if t.Elem().Kind() == reflect.Uint8 {
				return
			}
			walk(t.Elem(), path, stack)
		case reflect.Map:
			paths[path+".*"] = true
			walk(t.Elem(), path+".*", stack)
		case reflect.Struct:
			if stack[t] {
				paths[path+"!"] = true
				return
			}
			stack[t] = true
			defer delete(stack, t)
			for index := 0; index < t.NumField(); index++ {
				field := t.Field(index)
				tag := field.Tag.Get("json")
				if tag == "-" {
					continue
				}
				name, _, _ := strings.Cut(tag, ",")
				if field.Anonymous && name == "" {
					embedded := field.Type
					for embedded.Kind() == reflect.Pointer {
						embedded = embedded.Elem()
					}
					if embedded.Kind() == reflect.Struct {
						walk(embedded, path, stack)
						continue
					}
				}
				if !field.IsExported() {
					continue
				}
				if name == "" {
					name = field.Name
				}
				paths[path+"."+name] = true
				walk(field.Type, path+"."+name, stack)
			}
		}
	}
	walk(t, "", map[reflect.Type]bool{})
	result := make([]string, 0, len(paths))
	for path := range paths {
		result = append(result, path)
	}
	sort.Strings(result)
	return result
}

// v1SchemaTypes are the answers whose v1 shape is frozen, by schema file.
var v1SchemaTypes = map[string]reflect.Type{
	"v1_response_schema.txt":         reflect.TypeOf(Response{}),
	"v1_graph_amendment_schema.txt":  reflect.TypeOf(domain.GraphAmendmentResult{}),
	"v1_unknown_recovery_schema.txt": reflect.TypeOf(domain.UnknownAssignmentRecoveryDecision{}),
}

// Writes the schemas of this tree's answers into the directory
// T3_WRITE_V1_SCHEMA names. Run on the release whose v1 shape is frozen, it
// produced the v1_*_schema.txt files.
func TestWriteV1ResponseSchema(t *testing.T) {
	dir := os.Getenv("T3_WRITE_V1_SCHEMA")
	if dir == "" {
		t.Skip("T3_WRITE_V1_SCHEMA is not set")
	}
	for name, answer := range v1SchemaTypes {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(strings.Join(jsonKeySchema(answer), "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
