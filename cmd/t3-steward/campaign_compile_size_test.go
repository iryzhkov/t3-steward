package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func compileSizeFixture(t *testing.T, first, second int) string {
	t.Helper()
	path, raw := compileFixturePlan(t)
	for index, budget := range []int{first, second} {
		if budget == 0 {
			continue
		}
		anchor := []string{"\n## B-1\n", "\n## C-10 later\n"}[index]
		section := fmt.Sprintf("\n### Code map\n- owner [changed-lines: %d]\n- tie [changed-lines: 1]\n### Tests to add\n- tests [changed-lines: 1]\n", budget-2)
		raw = bytes.Replace(raw, []byte(anchor), []byte(section+anchor), 1)
	}
	compileSizeOK(t, os.WriteFile(path, raw, 0600))
	return path
}
func TestCampaignCompileSizeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		raw, threshold, estimate int
		check, refuses           bool
	}{
		{239, 300, 299, true, false}, {240, 300, 300, true, false},
		{241, 300, 302, true, true}, {241, 302, 302, true, false},
		{241, 300, 302, false, false},
	} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			path := compileSizeFixture(t, 10, tc.raw)
			out := filepath.Join(t.TempDir(), "wave")
			calibration := filepath.Join(t.TempDir(), "size.json")
			compileSizeOK(t, os.WriteFile(calibration, []byte(`{"samples":[{"raw":4,"observed":5,"base":"55465b6cce6588504ba7750e06b0e1cc2de5a714","head":"55465b6cce6588504ba7750e06b0e1cc2de5a714","accept_ref":"ACCEPT","gate_ref":"passing gate"}]}`), 0600))
			var stdout bytes.Buffer
			cli, requests := campaignCheckCLI(t, &stdout, campaignReadyMatrix)
			before := len(*requests)
			args := []string{"compile", path, "--out", out, "--json", "--size-calibration", calibration, "--max-changed-lines", fmt.Sprint(tc.threshold)}
			if tc.check {
				args = append(args, "--check")
			}
			err := cli.run(context.Background(), args)
			if tc.refuses {
				compileSizeAssert(t, !(err == nil || exitCodeFor(err) != 1), fmt.Sprintf("err %v", err))
				for _, want := range []string{"b1 estimate 302", "threshold 300", "owner", "prerequisite parts"} {
					compileSizeAssert(t, strings.Contains(err.Error(), want), err)
				}
				compileSizeAssert(t, !(strings.Index(err.Error(), "owner") > strings.Index(err.Error(), "tie") || strings.Index(err.Error(), "tie") > strings.Index(err.Error(), "tests (1)")), err)
				_, statErr := os.Stat(out)
				compileSizeAssert(t, os.IsNotExist(statErr), "refusal created output")
				compileSizeAssert(t, len(*requests) == before, "readiness before refusal")
				return
			}
			compileSizeAssert(t, err == nil, err)
			var report campaignCompileReport
			decoder := json.NewDecoder(&stdout)
			compileSizeOK(t, decoder.Decode(&report))
			compileSizeAssert(t, decoder.Decode(new(any)) == io.EOF, "extra JSON")
			compileSizeAssert(t, !(len(report.Units) != 2 || report.Units[0].ID != "c1" || report.Units[1].ID != "b1"), fmt.Sprintf("%+v", report))
			size := report.Units[1].Size
			compileSizeAssert(t, !(size == nil || size.Raw != int64(tc.raw) || size.Estimate != int64(tc.estimate) || size.Threshold != int64(tc.threshold) || size.Multiplier != 1.25 || size.SampleCount != 1 || size.Label != "calibrated" || len(size.Contributions) != 3 || report.Units[0].Size.Raw != 10), fmt.Sprintf("%+v", size))
		})
	}
}
func TestCampaignCompileSizeRefusalPreservesTree(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, force := range []bool{false, true} {
			path := compileSizeFixture(t, 10, 241)
			out := filepath.Join(t.TempDir(), "wave")
			sentinel := filepath.Join(out, "b1", "keep")
			if existing {
				compileSizeOK(t, os.MkdirAll(filepath.Dir(sentinel), 0755))
				compileSizeOK(t, os.WriteFile(sentinel, []byte("untouched"), 0600))
			}
			var stdout bytes.Buffer
			cli, requests := campaignCheckCLI(t, &stdout, campaignReadyMatrix)
			before := len(*requests)
			args := []string{"compile", path, "--out", out, "--check"}
			if force {
				args = append(args, "--force")
			}
			err := cli.run(context.Background(), args)
			compileSizeAssert(t, err != nil && strings.Contains(err.Error(), "estimate 302"), err)
			compileSizeAssert(t, len(*requests) == before, "readiness request")
			if existing {
				assertDirectoryEntries(t, out, "b1")
				assertDirectoryEntries(t, filepath.Dir(sentinel), "keep")
				got, err := os.ReadFile(sentinel)
				compileSizeAssert(t, err == nil && string(got) == "untouched", err)
			} else {
				assertDirectoryEntries(t, out)
			}
			compileSizeOK(t, cli.run(context.Background(), []string{"compile", path, "--out", out, "--check", "--unit", "c1"}))
			compileSizeAssert(t, len(*requests) == before+1, "selection readiness")
		}
	}
}
func TestCampaignCompileSizeCalibrationAndInvalidInput(t *testing.T) {
	sample := `{"samples":[{"raw":2,"observed":3,"base":"55465b6cce6588504ba7750e06b0e1cc2de5a714","head":"55465b6cce6588504ba7750e06b0e1cc2de5a714","accept_ref":"independent ACCEPT","gate_ref":"passing gate"}]}`
	for _, data := range []string{sample, `{"samples":[]}`, "{", `{"samples":[{"raw":2,"observed":3}]}`, strings.Repeat(" ", (1<<20)+1), "missing", "directory"} {
		path := compileSizeFixture(t, 10, 201)
		calibration := filepath.Join(t.TempDir(), "calibration.json")
		if data == "directory" {
			compileSizeOK(t, os.Mkdir(calibration, 0700))
		} else if data != "missing" {
			compileSizeOK(t, os.WriteFile(calibration, []byte(data), 0600))
		}
		for _, legacy := range []bool{false, true} {
			if legacy {
				path, _ = compileFixturePlan(t)
			}
			var stdout bytes.Buffer
			cli, requests := campaignCheckCLI(t, &stdout, campaignReadyMatrix)
			before := len(*requests)
			out := filepath.Join(t.TempDir(), "wave")
			err := cli.run(context.Background(), []string{"compile", path, "--out", out, "--check", "--json", "--size-calibration", calibration, "--max-changed-lines", "302"})
			if data != sample && data != `{"samples":[]}` {
				compileSizeAssert(t, !(err == nil || exitCodeFor(err) != 1), fmt.Sprintf("calibration err %v", err))
				assertDirectoryEntries(t, out)
				compileSizeAssert(t, len(*requests) == before, "readiness before calibration refusal")
			} else {
				compileSizeAssert(t, err == nil, err)
				var report campaignCompileReport
				compileSizeOK(t, json.Unmarshal(stdout.Bytes(), &report))
				if !legacy {
					size := report.Units[1].Size
					compileSizeAssert(t, !(data == sample && (size.Estimate != 302 || size.Label != "calibrated" || size.SampleCount != 1 || size.Multiplier != 1.5)), fmt.Sprintf("%+v", size))
					compileSizeAssert(t, !(data != sample && (size.Estimate != 252 || size.Label != "uncalibrated")), fmt.Sprintf("%+v", size))
				}
			}
		}
	}
	for _, check := range []bool{false, true} {
		path := compileSizeFixture(t, 10, 20)
		raw, err := os.ReadFile(path)
		compileSizeAssert(t, err == nil, err)
		compileSizeOK(t, os.WriteFile(path, bytes.Replace(raw, []byte("owner [changed-lines: 18]"), []byte(map[bool]string{false: "owner", true: "owner [changed-lines: nope]"}[check]), 1), 0600))
		var stdout bytes.Buffer
		cli, requests := campaignCheckCLI(t, &stdout, campaignReadyMatrix)
		before := len(*requests)
		out := filepath.Join(t.TempDir(), "wave")
		args := []string{"compile", path, "--out", out}
		if check {
			args = append(args, "--check")
		}
		err = cli.run(context.Background(), args)
		compileSizeAssert(t, err != nil && strings.Contains(err.Error(), "b1:") && strings.Contains(err.Error(), "line"), err)
		assertDirectoryEntries(t, out)
		compileSizeAssert(t, len(*requests) == before, "invalid annotations reached readiness")
	}
	for _, args := range [][]string{{"--max-changed-lines"}, {"--max-changed-lines", "0"}, {"--max-changed-lines", "-1"}, {"--max-changed-lines", "x"}, {"--max-changed-lines", "9223372036854775808"}, {"--max-changed-lines", "3", "--max-changed-lines", "4"}, {"--size-calibration"}, {"--size-calibration", "x", "--size-calibration", "y"}} {
		_, err := parseCampaignCompileArgs(append([]string{"plan.md", "--out", "out"}, args...))
		compileSizeAssert(t, err != nil, args)
	}
}
func compileSizeOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func compileSizeAssert(t *testing.T, ok bool, detail any) {
	t.Helper()
	if !ok {
		t.Fatal(detail)
	}
}
