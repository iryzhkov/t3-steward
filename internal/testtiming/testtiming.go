// Package testtiming holds the two forms a test may use to bound how long
// code takes, so that a test's verdict does not depend on how busy its host
// is.
//
// A test that guards against a complexity regression measures growth with
// CheckLinear: it times the operation at two input sizes back to back in the
// same process and bounds the ratio. The time is the CPU time the test
// process consumed, not wall-clock time, so other processes competing for
// the CPU do not lengthen one sample more than the other. A test that guards against a hang keeps an absolute bound, written
// as Bound(d) so that it is scaled by RaceFactor under the race detector;
// such a bound must stay well below the time the hang it detects would take.
package testtiming

import (
	"fmt"
	"runtime"
	"time"

	"golang.org/x/sys/unix"
)

// Bound scales an absolute wall-clock bound by RaceFactor.
func Bound(d time.Duration) time.Duration {
	return d * RaceFactor
}

// GrowthFactor is how many times larger the large input of a growth
// measurement is than the small one.
const GrowthFactor = 16

// LinearLimit bounds the growth ratio of a linear operation. A linear
// operation grows by about GrowthFactor (16) and a quadratic one by about
// GrowthFactor squared (256); the limit sits at their geometric mean, so
// either may be measured four times off before it is misjudged. Linear code
// that is heavy on maps or allocation grows somewhat faster than its input
// once the large input leaves the CPU caches, so a narrower factor left too
// little room on a loaded host.
const LinearLimit = 64.0

// growthRounds is how many times each size is timed; the fastest sample of
// each size is the one compared, since interference only ever adds time.
const growthRounds = 5

// minimumSample is the shortest sample timed: an operation faster than that
// is repeated within a sample, so that clock resolution and the cost of
// reading the clock do not dominate it.
const minimumSample = 5 * time.Millisecond

// Growth reports how the running time of an operation grows when its input
// grows from n to GrowthFactor*n. prepare builds the input of a size outside
// the timed region and returns the operation to time, which must be
// repeatable. Each size is timed growthRounds times, alternately, each sample
// repeating the operation until it lasts minimumSample, and the result is the
// fastest time per operation at the large size divided by the fastest at the
// small size. The input size is not enlarged to lengthen a sample, so the
// caller's n keeps the large input small enough to measure quickly.
func Growth(n int, prepare func(n int) func()) float64 {
	smallOperation, largeOperation := prepare(n), prepare(GrowthFactor*n)
	smallRepeats, largeRepeats := repeatsFor(smallOperation), repeatsFor(largeOperation)
	small, large := time.Duration(1<<63-1), time.Duration(1<<63-1)
	for range growthRounds {
		small = min(small, sample(smallOperation, smallRepeats)/time.Duration(smallRepeats))
		large = min(large, sample(largeOperation, largeRepeats)/time.Duration(largeRepeats))
	}
	return float64(large) / float64(max(small, time.Nanosecond))
}

// CheckLinear returns an error when an operation's running time grows faster
// than linearly in its input, as measured by Growth.
func CheckLinear(n int, prepare func(n int) func()) error {
	if ratio := Growth(n, prepare); ratio > LinearLimit {
		return fmt.Errorf("growing the input %dx from %d took %.1fx as long, want at most %.0fx for linear growth", GrowthFactor, n, ratio, LinearLimit)
	}
	return nil
}

// repeatsFor is how many times operation must run for a sample to last
// minimumSample.
func repeatsFor(operation func()) int {
	repeats := 1
	for repeats < 1<<20 && sample(operation, repeats) < minimumSample {
		repeats *= 2
	}
	return repeats
}

// sample is the CPU time the process spends running operation repeats
// times. A wall-clock sample was not enough: on a saturated CPU a short
// sample of the small input could finish within one scheduler time slice
// while every long sample of the large input was shared with the competing
// processes, so the ratio grew with load (68x for linear code with two busy
// loops on one CPU under the race detector). CPU time charges the test only
// for the time it ran.
func sample(operation func(), repeats int) time.Duration {
	runtime.GC()
	started := processCPUTime()
	for range repeats {
		operation()
	}
	return processCPUTime() - started
}

func processCPUTime() time.Duration {
	var now unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &now); err != nil {
		panic(fmt.Sprintf("testtiming: reading the process CPU clock: %v", err))
	}
	return time.Duration(now.Nano())
}
