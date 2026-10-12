package campaign

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type CompileSizeContributor struct {
	Section, Text string
	Line          int
	Raw           int64
}

// CompileSizeEstimate is advisory when Available is false; diagnostics invalidate it.
type CompileSizeEstimate struct {
	Available     bool
	Raw, Estimate int64
	Multiplier    float64
	SampleCount   int
	Label         string
	Contributors  []CompileSizeContributor
	Diagnostics   []string
}

// SizeCalibration is immutable to callers. Its zero value selects the fallback.
type SizeCalibration struct {
	ratio   *big.Rat
	samples int
}
type sizeSample struct {
	Raw       int64  `json:"raw"`
	Observed  int64  `json:"observed"`
	Base      string `json:"base"`
	Head      string `json:"head"`
	AcceptRef string `json:"accept_ref"`
	GateRef   string `json:"gate_ref"`
}

var sizeCommit = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
var sizeBudget = regexp.MustCompile(`\[changed-lines: ([0-9]+)\]$`)
var sizeBullet = regexp.MustCompile(`^(?:[-+*]|[0-9]+[.)])[ \t]+(.*)$`)

// ParseSizeCalibration validates operator assertions, without verifying their references.
func ParseSizeCalibration(raw []byte) (SizeCalibration, error) {
	var document struct {
		Samples []sizeSample `json:"samples"`
	}
	if len(raw) > 1<<20 {
		return SizeCalibration{}, fmt.Errorf("calibration exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return SizeCalibration{}, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return SizeCalibration{}, fmt.Errorf("calibration has trailing JSON")
	}
	if document.Samples == nil || len(document.Samples) > 1000 {
		return SizeCalibration{}, fmt.Errorf("calibration requires samples array of at most 1000 entries")
	}
	ratios := make([]*big.Rat, 0, len(document.Samples))
	for i, sample := range document.Samples {
		if sample.Raw <= 0 || sample.Observed <= 0 || !sizeCommit.MatchString(sample.Base) || !sizeCommit.MatchString(sample.Head) || strings.TrimSpace(sample.AcceptRef) == "" || strings.TrimSpace(sample.GateRef) == "" {
			return SizeCalibration{}, fmt.Errorf("calibration sample %d: positive counts and full commits, ACCEPT and gate references required", i+1)
		}
		ratios = append(ratios, new(big.Rat).SetFrac64(sample.Observed, sample.Raw))
	}
	if len(ratios) == 0 {
		return SizeCalibration{}, nil
	}
	sort.Slice(ratios, func(i, j int) bool { return ratios[i].Cmp(ratios[j]) < 0 })
	ratio := ratios[(9*len(ratios)+9)/10-1]
	if ratio.Cmp(big.NewRat(1, 1)) < 0 {
		ratio = big.NewRat(1, 1)
	}
	return SizeCalibration{ratio, len(ratios)}, nil
}

// EstimateCompileSize accepts only an extracted unit section, never a whole plan.
func EstimateCompileSize(section []byte, calibration SizeCalibration) CompileSizeEstimate {
	result := CompileSizeEstimate{Multiplier: 1.25, Label: "uncalibrated"}
	ratio := big.NewRat(5, 4)
	if calibration.ratio != nil {
		ratio = calibration.ratio
		result.Multiplier, _ = ratio.Float64()
		result.SampleCount, result.Label = calibration.samples, "calibrated"
	}
	// Blank fences in a copy so the compiler's ATX offsets and local lines stay intact.
	visible := append([]byte(nil), section...)
	fence := ""
	lines := splitPlanLines(section)
	for _, line := range lines {
		text := planLineText(section, line)
		indented := strings.TrimLeft(text, " ")
		fenced := fence != ""
		if len(text)-len(indented) <= 3 {
			if fence != "" {
				if strings.HasPrefix(indented, fence) && strings.Trim(indented, fence[:1]+" \t") == "" {
					fence = ""
				}
			} else if marker := fenceMarker(indented); marker != "" {
				fence, fenced = marker, true
			}
		}
		if fenced {
			for i := line.start; i < line.end; i++ {
				if visible[i] != '\n' {
					visible[i] = ' '
				}
			}
		}
	}
	if !bytes.Contains(visible, []byte("[changed-lines")) {
		return result
	}
	result.Available = true
	headings := planHeadings(visible, 1)
	seenSections, seenText := map[string]bool{}, map[string]bool{}
	for _, name := range []string{"Code map", "Tests to add"} {
		for _, heading := range headings {
			if !headingMatches(heading.text, name) {
				continue
			}
			seenSections[name] = true
			end := sectionEnd(visible, headings, heading)
			for index := 0; index < len(lines); index++ {
				line := lines[index]
				if line.start <= heading.start || line.start >= end {
					continue
				}
				text := strings.TrimSpace(planLineText(visible, line))
				bullet := sizeBullet.FindStringSubmatch(text)
				if bullet == nil {
					continue
				}
				firstLine := index + 1
				diagnostic := func(message string) {
					result.Diagnostics = append(result.Diagnostics, fmt.Sprintf("%s line %d: %s", name, firstLine, message))
				}
				contributionText := bullet[1]
				for index+1 < len(lines) && lines[index+1].start < end {
					next := planLineText(visible, lines[index+1])
					if strings.TrimSpace(next) == "" || len(next)-len(strings.TrimLeft(next, " \t")) <= len(planLineText(visible, line))-len(strings.TrimLeft(planLineText(visible, line), " \t")) || sizeBullet.MatchString(strings.TrimSpace(next)) {
						break
					}
					index++
					contributionText += " " + strings.TrimSpace(next)
				}
				annotation := sizeBudget.FindStringSubmatch(contributionText)
				if annotation == nil {
					diagnostic("missing or invalid [changed-lines: N] suffix")
					continue
				}
				budget, err := strconv.ParseInt(annotation[1], 10, 64)
				if err != nil || budget <= 0 {
					diagnostic("budget must be a positive int64")
					continue
				}
				contribution := strings.TrimSpace(strings.TrimSuffix(contributionText, annotation[0]))
				if contribution == "" || strings.Contains(contribution, "[changed-lines") {
					diagnostic("invalid contribution or annotation")
					continue
				}
				if seenText[contribution] {
					diagnostic("duplicate contribution")
					continue
				}
				seenText[contribution] = true
				if budget > math.MaxInt64-result.Raw {
					diagnostic("raw sum overflow")
					continue
				}
				result.Raw += budget
				result.Contributors = append(result.Contributors, CompileSizeContributor{name, contribution, firstLine, budget})
			}
		}
		if !seenSections[name] {
			result.Diagnostics = append(result.Diagnostics, fmt.Sprintf("%s line 1: missing budget section", name))
		}
	}
	// Malformed markers outside contribution bullets must also opt in and fail.
	for index, line := range lines {
		text := strings.TrimSpace(planLineText(visible, line))
		if strings.Contains(text, "[changed-lines") && sizeBudget.FindStringSubmatch(text) == nil {
			result.Diagnostics = append(result.Diagnostics, fmt.Sprintf("unit line %d: invalid changed-lines marker", index+1))
		}
	}
	sort.SliceStable(result.Contributors, func(i, j int) bool {
		return result.Contributors[i].Raw > result.Contributors[j].Raw || result.Contributors[i].Raw == result.Contributors[j].Raw && result.Contributors[i].Line < result.Contributors[j].Line
	})
	if len(result.Diagnostics) > 0 {
		return result
	}
	scaled := new(big.Rat).Mul(new(big.Rat).SetInt64(result.Raw), ratio)
	numerator := new(big.Int).Sub(new(big.Int).Add(scaled.Num(), scaled.Denom()), big.NewInt(1))
	ceiling := new(big.Int).Quo(numerator, scaled.Denom())
	if !ceiling.IsInt64() {
		result.Diagnostics = append(result.Diagnostics, "unit line 1: calibrated ceil overflow")
		return result
	}
	result.Estimate = ceiling.Int64()
	return result
}
