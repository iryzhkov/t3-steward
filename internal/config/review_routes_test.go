package config

import "testing"

func TestReviewRouteMetadataValidation(t *testing.T) {
	for _, test := range []struct {
		route, family, tier string
		valid               bool
	}{
		{"alias/full", "openai", "executor", true},
		{"another/full", "claude", "critical", true},
		{"a/cheap", "claude", "economy", true},
		{"a/full", "", "executor", false},
		{"a/full", "claude", "premium", false},
		{"full", "openai", "executor", false},
	} {
		c := Default()
		c.BacklogV2.ReviewRoutes = map[string]ReviewRouteMetadata{test.route: {ProviderFamily: test.family, Tier: test.tier}}
		err := c.validateBacklogV2()
		if (err == nil) != test.valid {
			t.Fatalf("%+v: %v", test, err)
		}
	}
	c := Default()
	if err := c.validateBacklogV2(); err != nil {
		t.Fatalf("old config: %v", err)
	}
}
