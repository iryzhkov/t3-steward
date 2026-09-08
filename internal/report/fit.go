package report

import (
	"fmt"
	"math"
	"os"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// Weights are quota percent per million tokens of each type.
type Weights struct {
	Input      float64 `json:"input"`
	CacheWrite float64 `json:"cacheWrite"`
	CacheRead  float64 `json:"cacheRead"`
	Output     float64 `json:"output"`
}

// DefaultWeights follow list-price ratios (input 1, cache write 1.25, cache
// read 0.1, output 5). Only the ratios matter for splitting a rise between
// threads; the absolute scale is arbitrary.
var DefaultWeights = Weights{Input: 1, CacheWrite: 1.25, CacheRead: 0.1, Output: 5}

// Cost of a sample under the weights, in quota percent.
func (w Weights) Cost(u domain.UsageSample) float64 {
	return (w.Input*float64(u.InputTokens) + w.CacheWrite*float64(u.CacheWriteTokens) +
		w.CacheRead*float64(u.CacheReadTokens) + w.Output*float64(u.OutputTokens)) / 1e6
}

// Fit describes how the weights were obtained.
type Fit struct {
	Weights Weights `json:"weights"`
	// Intervals is the number of reading-to-reading intervals with token
	// data that entered the fit.
	Intervals int `json:"intervals"`
	// R2 is the coefficient of determination of rise versus predicted
	// cost; 1 is a perfect model, 0 no better than the mean.
	R2 float64 `json:"r2"`
	// Fitted is false when the data was too thin and DefaultWeights were
	// used for attribution instead.
	Fitted bool   `json:"fitted"`
	Note   string `json:"note,omitempty"`
	// Volume is the total tokens of each type (millions) in the fit;
	// weights for types with almost no volume are not identifiable.
	Volume [4]float64 `json:"volume"`
}

// interval is the token total between two consecutive readings.
type interval struct {
	rise   float64
	tokens [4]float64 // millions of input, cache write, cache read, output
	// bin groups intervals for the fit. Readings are rounded to whole
	// percent, so single intervals carry almost no signal; hour-sized bins
	// do.
	bin string
}

// fitWeights solves a non-negative least squares problem
// rise ≈ w·tokens over the intervals by projected coordinate descent. Four
// unknowns and at most a few thousand rows: a few hundred sweeps suffice.
func fitWeights(rows []interval) Fit {
	const minRows = 30
	binned := map[string]*interval{}
	var order []string
	for _, r := range rows {
		if r.bin == "" {
			r.bin = fmt.Sprint(len(order))
		}
		b, ok := binned[r.bin]
		if !ok {
			b = &interval{bin: r.bin}
			binned[r.bin] = b
			order = append(order, r.bin)
		}
		b.rise += r.rise
		for i := range b.tokens {
			b.tokens[i] += r.tokens[i]
		}
	}
	var usable []interval
	for _, key := range order {
		r := *binned[key]
		if r.tokens[0]+r.tokens[1]+r.tokens[2]+r.tokens[3] > 0 && r.rise >= 0 {
			usable = append(usable, r)
		}
	}
	if len(usable) < minRows {
		return Fit{Weights: DefaultWeights, Intervals: len(usable), Fitted: false,
			Note: "too few intervals with token data; list-price ratios used"}
	}
	// Normal equations A = XᵀX, b = Xᵀy.
	var A [4][4]float64
	var b [4]float64
	for _, r := range usable {
		for i := 0; i < 4; i++ {
			b[i] += r.tokens[i] * r.rise
			for j := 0; j < 4; j++ {
				A[i][j] += r.tokens[i] * r.tokens[j]
			}
		}
	}
	var w [4]float64
	for sweep := 0; sweep < 5000; sweep++ {
		maxDelta := 0.0
		for i := 0; i < 4; i++ {
			if A[i][i] == 0 {
				continue
			}
			g := b[i]
			for j := 0; j < 4; j++ {
				if j != i {
					g -= A[i][j] * w[j]
				}
			}
			next := g / A[i][i]
			if next < 0 {
				next = 0
			}
			if d := math.Abs(next - w[i]); d > maxDelta {
				maxDelta = d
			}
			w[i] = next
		}
		if maxDelta < 1e-9 {
			break
		}
	}
	if os.Getenv("TQW_DEBUG_FIT") != "" {
		for _, r := range usable {
			fmt.Fprintf(os.Stderr, "%.0f\t%.3f\t%.3f\t%.3f\t%.3f\t%s\n", r.rise, r.tokens[0], r.tokens[1], r.tokens[2], r.tokens[3], r.bin)
		}
		fmt.Fprintf(os.Stderr, "w=%v\n", w)
	}
	fit := Fit{Weights: Weights{Input: w[0], CacheWrite: w[1], CacheRead: w[2], Output: w[3]}, Intervals: len(usable), Fitted: true}
	for _, r := range usable {
		for i := range r.tokens {
			fit.Volume[i] += r.tokens[i]
		}
	}
	// R² against the mean.
	mean := 0.0
	for _, r := range usable {
		mean += r.rise
	}
	mean /= float64(len(usable))
	var ssRes, ssTot float64
	for _, r := range usable {
		pred := 0.0
		for i := 0; i < 4; i++ {
			pred += w[i] * r.tokens[i]
		}
		ssRes += (r.rise - pred) * (r.rise - pred)
		ssTot += (r.rise - mean) * (r.rise - mean)
	}
	if ssTot > 0 {
		fit.R2 = 1 - ssRes/ssTot
	}
	// The four token types are strongly collinear (every call carries all
	// of them in similar proportions), so the free fit tends to load one
	// type and zero the others. Compare it with a one-parameter model,
	// list-price ratios times a scale, and prefer that unless the free
	// fit is clearly better.
	scale, scaledR2 := fitScale(usable)
	if os.Getenv("TQW_DEBUG_FIT") != "" {
		fmt.Fprintf(os.Stderr, "free R2=%.3f scaled R2=%.3f scale=%.3f\n", fit.R2, scaledR2, scale)
	}
	if scaledR2 >= fit.R2-0.05 && scale > 0 {
		fit.Weights = Weights{
			Input: DefaultWeights.Input * scale, CacheWrite: DefaultWeights.CacheWrite * scale,
			CacheRead: DefaultWeights.CacheRead * scale, Output: DefaultWeights.Output * scale,
		}
		fit.R2 = scaledR2
		fit.Note = "list-price ratios, scaled to the observed consumption"
	}
	if fit.R2 < 0.2 || (fit.Weights.Input+fit.Weights.CacheWrite+fit.Weights.CacheRead+fit.Weights.Output) == 0 {
		fit.Note = "fit explains too little variance; list-price ratios used for attribution"
		fit.Weights = DefaultWeights
		fit.Fitted = false
	}
	return fit
}

// fitScale fits rise ≈ scale · defaultCost(tokens) and returns the scale
// and its R².
func fitScale(rows []interval) (float64, float64) {
	var xy, xx, sum float64
	for _, r := range rows {
		x := DefaultWeights.Input*r.tokens[0] + DefaultWeights.CacheWrite*r.tokens[1] +
			DefaultWeights.CacheRead*r.tokens[2] + DefaultWeights.Output*r.tokens[3]
		xy += x * r.rise
		xx += x * x
		sum += r.rise
	}
	if xx == 0 || len(rows) == 0 {
		return 0, 0
	}
	scale := xy / xx
	if scale < 0 {
		scale = 0
	}
	mean := sum / float64(len(rows))
	var ssRes, ssTot float64
	for _, r := range rows {
		x := DefaultWeights.Input*r.tokens[0] + DefaultWeights.CacheWrite*r.tokens[1] +
			DefaultWeights.CacheRead*r.tokens[2] + DefaultWeights.Output*r.tokens[3]
		ssRes += (r.rise - scale*x) * (r.rise - scale*x)
		ssTot += (r.rise - mean) * (r.rise - mean)
	}
	if ssTot == 0 {
		return scale, 0
	}
	return scale, 1 - ssRes/ssTot
}
