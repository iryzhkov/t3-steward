//go:build race

package backlog_test

// raceEnabled reports whether the tests were built with the race detector.
// Under it every write through the pure-Go SQLite driver is several times
// slower, so tests that write thousands of rows to cross a bound cross a
// lowered bound instead; the plain run still proves the production bound.
const raceEnabled = true
