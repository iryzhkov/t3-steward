package main

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// A misspelt family is refused with the family it was probably meant to be,
// and before its flags are parsed: "campagin submit dir --idempotency-key K"
// used to be refused for its first flag, which said nothing about the word
// that was wrong.
func TestMisspeltTopLevelCommandSuggestsTheFamily(t *testing.T) {
	for _, args := range [][]string{
		{"campagin", "list"},
		{"campagin", "submit", "dir", "--idempotency-key", "k"},
	} {
		err := dispatch(args)
		if !errors.Is(err, errUnknownCommand) || !strings.Contains(err.Error(), `did you mean "campaign"?`) {
			t.Fatalf("%v: error = %v", args, err)
		}
	}
	if err := dispatch([]string{"zzzzzzzz"}); !errors.Is(err, errUnknownCommand) || strings.Contains(err.Error(), "did you mean") {
		t.Fatalf("a word near nothing: error = %v", err)
	}
}

// Every word dispatch routes is one of the two lists the unknown-command check
// and its suggestion read, so a verb added to dispatch without being listed is
// a failure here rather than a verb refused as unknown.
func TestEveryDispatchedWordIsKnownToTheUnknownCommandCheck(t *testing.T) {
	for word := range verbsRoutedIn(t, packageSource(t), "dispatch") {
		// Flags, and the name of the --config flag dispatch compares against,
		// are not command words.
		if strings.HasPrefix(word, "-") || word == "config" {
			continue
		}
		if !slices.Contains(dispatchedVerbs, word) && !slices.Contains(topLevelFamilies, word) {
			t.Errorf("dispatch routes %q, which neither dispatchedVerbs nor topLevelFamilies lists", word)
		}
	}
}
