package domain

import "testing"

func TestAskRelayHelpers(t *testing.T) {
	if AskRelayThreadID("tw-a") != AskRelayThreadID("tw-a") || AskRelayThreadID("tw-a") == AskRelayThreadID("tw-b") ||
		len(AskRelayThreadID("tw-a")) != 36 {
		t.Fatal("relay thread IDs are not stable per ask")
	}
	ask := AskRequest{Question: "Pick", Options: []string{"alpha", "beta", "gamma, delta"}, Multi: true}
	for _, c := range []struct {
		values  []string
		options string
		free    string
	}{
		{[]string{"beta"}, "beta", ""},
		{[]string{"beta, alpha"}, "alpha|beta", ""},
		{[]string{"gamma, delta"}, "gamma, delta", ""},
		{[]string{"alpha", "something else"}, "alpha", "something else"},
		{[]string{"neither"}, "", "neither"},
	} {
		options, free := ask.SplitAnswer(c.values)
		joined := ""
		for i, option := range options {
			if i > 0 {
				joined += "|"
			}
			joined += option
		}
		if joined != c.options || free != c.free {
			t.Errorf("SplitAnswer(%q) = %q, %q", c.values, joined, free)
		}
	}
	card := UserInputQuestion{Question: "Pick", Options: []UserInputOption{{Label: "gamma, delta"}, {Label: "alpha"}, {Label: "beta"}}}
	if !card.MatchesAsk(ask) {
		t.Fatal("a reordered card of the same options did not match")
	}
	card.Options = card.Options[:2]
	if card.MatchesAsk(ask) {
		t.Fatal("a card missing an option matched")
	}
}
