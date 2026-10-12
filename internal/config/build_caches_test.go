package config

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestWorkerCacheValidation(t *testing.T) {
	valid := func() V2Cache {
		return V2Cache{Name: "go", Toolchain: V2CacheToolchain{Command: "go", VersionArgs: []string{"env", "GOVERSION"}}, Env: map[string]string{"GOCACHE": "go-build", "GOMODCACHE": "mod"}, MaxMB: 100}
	}
	cases := map[string]func(*V2Cache){
		"name":              func(c *V2Cache) { c.Name = "../Go" },
		"empty command":     func(c *V2Cache) { c.Toolchain.Command = "" },
		"relative command":  func(c *V2Cache) { c.Toolchain.Command = "./go" },
		"command arguments": func(c *V2Cache) { c.Toolchain.Command = "go env" },
		"empty env":         func(c *V2Cache) { c.Env = nil },
		"variable":          func(c *V2Cache) { c.Env = map[string]string{"1VAR": "mod"} },
		"empty path":        func(c *V2Cache) { c.Env["GOCACHE"] = "" },
		"absolute":          func(c *V2Cache) { c.Env["GOCACHE"] = "/tmp/cache" },
		"unclean":           func(c *V2Cache) { c.Env["GOCACHE"] = "a/./b" },
		"parent":            func(c *V2Cache) { c.Env["GOCACHE"] = "../cache" },
		"dot":               func(c *V2Cache) { c.Env["GOCACHE"] = "." },
		"equal":             func(c *V2Cache) { c.Env["GOCACHE"] = "mod" },
		"nested":            func(c *V2Cache) { c.Env["GOCACHE"] = "mod/build" },
		"zero":              func(c *V2Cache) { c.MaxMB = 0 },
		"negative":          func(c *V2Cache) { c.MaxMB = -1 },
	}
	for _, name := range domain.TaskWaitEnvironmentNames() {
		cases[name] = func(c *V2Cache) { c.Env = map[string]string{name: "identity"} }
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := valid()
			mutate(&c)
			cfg := validBacklogV2Config(t)
			w := cfg.BacklogV2.Workers["normandy"]
			w.Caches = []V2Cache{c}
			cfg.BacklogV2.Workers["normandy"] = w
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "worker \"normandy\" cache") {
				t.Fatalf("refusal = %v", err)
			}
		})
	}
	for _, duplicateName := range []bool{true, false} {
		a, b := valid(), valid()
		if !duplicateName {
			b.Name = "other"
		}
		if err := ValidateWorkerCaches("worker", []V2Cache{a, b}); err == nil {
			t.Fatal("duplicate accepted")
		}
	}
	c := valid()
	if err := ValidateWorkerCaches("worker", []V2Cache{c}); err != nil {
		t.Fatal(err)
	}
	c.Toolchain.Command = "/usr/bin/go"
	c.Toolchain.VersionArgs = nil
	if err := ValidateWorkerCaches("worker", []V2Cache{c}); err != nil {
		t.Fatal(err)
	}
}
