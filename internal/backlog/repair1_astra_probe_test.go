package backlog

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"strings"
	"testing"
)

func TestRepair1AstraAliasExpansionAndStrictMerges(t *testing.T) {
	for _, depth := range []int{3, 13} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			nested := "&a0 {required: true}"
			for i := 1; i <= depth; i++ {
				nested = fmt.Sprintf("&a%d {<<: [%s, *a%d]}", i, nested, i-1)
			}
			raw := strings.Replace(declaredManifestYAML(), "required: true", "<<: "+nested, 1)
			_, err := ParseManifest([]byte(raw))
			if depth == 3 && err != nil {
				t.Fatal("small acyclic shared merge refused", err)
			}
			if depth == 13 && (err == nil || !strings.Contains(err.Error(), "limits")) {
				t.Fatal("expanded alias visit budget not enforced", err)
			}
			t.Logf("depth=%d err=%v", depth, err)
		})
	}
	for name, fields := range map[string]string{
		"duplicate-in-merge":           "<<: {required: true, required: false}",
		"unknown-after-first-sequence": "<<: [{required: true}, {unknown: true}]",
		"duplicate-merge-key":          "<<: {required: true}, <<: {required: false}",
		"mutual-effective-cycle":       "<<: &a {<<: &b {<<: *a}}",
	} {
		t.Run(name, func(t *testing.T) {
			raw := strings.Replace(declaredManifestYAML(), "required: true", fields, 1)
			if _, err := ParseManifest([]byte(raw)); err == nil {
				t.Fatal("unsafe merge accepted")
			} else {
				t.Log(err)
			}
		})
	}
}

func TestRepair1AstraExactNodeBudgets(t *testing.T) {
	for _, count := range []int{4095, 4096} {
		n := &yaml.Node{Kind: yaml.SequenceNode}
		leaf := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "x"}
		for i := 0; i < count; i++ {
			n.Content = append(n.Content, leaf)
		}
		err := validateReviewMemberNodes(n)
		if (err == nil) != (count == 4095) {
			t.Fatalf("count=%d err=%v", count, err)
		}
	}
	leaf := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: "9"}
	for _, depth := range []int{64, 65} {
		n := leaf
		for i := 0; i < depth; i++ {
			n = &yaml.Node{Kind: yaml.AliasNode, Alias: n}
		}
		err := validateReviewMemberNodes(n)
		if (err == nil) != (depth == 64) {
			t.Fatalf("depth=%d err=%v", depth, err)
		}
	}
	lookup := reviewNodeLookup{visits: 32767}
	if _, err := lookup.resolve(leaf); err != nil {
		t.Fatal(err)
	}
	if _, err := lookup.resolve(leaf); err == nil {
		t.Fatal("lookup budget overflow accepted")
	}
	t.Log("4096 visits, depth64, and 32768 lookup visits exact inclusive boundaries confirmed")
}
