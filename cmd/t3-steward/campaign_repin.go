package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/campaign"
)

// loadCompiledSource validates provenance by reproduction, not authentication.
func (c campaignCLI) loadCompiledSource(dir string) ([]byte, campaign.CompiledUnit, campaign.CompileOptions, error) {
	bundle, projected, err := c.prepare(dir)
	if err != nil {
		return nil, campaign.CompiledUnit{}, campaign.CompileOptions{}, err
	}
	planPath := filepath.Join(bundle.Campaign.Root, "inputs/plan.md")
	options := campaign.CompileOptions{ReadInput: compileInputReader(planPath, c.limits.MaxBytes)}
	refuse := func(err error) ([]byte, campaign.CompiledUnit, campaign.CompileOptions, error) {
		return nil, campaign.CompiledUnit{}, options, err
	}
	raw, err := readCompilePlan(planPath, c.limits.MaxBytes)
	if err != nil {
		return refuse(err)
	}
	plan, err := campaign.ParseCompilePlanWith(planPath, raw, options)
	if err != nil {
		return refuse(err)
	}
	units, err := selectCompileUnits(plan.Units, []string{projected.Name})
	if err != nil {
		return refuse(err)
	}
	unit := units[0]
	if filepath.Base(filepath.Clean(dir)) != unit.ID || len(unit.Files) != len(bundle.Campaign.Files) {
		return refuse(errors.New("directory or inventory differs from compiled unit"))
	}
	loaded := map[string]bool{}
	for _, file := range bundle.Campaign.Files {
		loaded[file.Path] = true
	}
	for _, file := range unit.Files {
		content, err := readCompilePlan(filepath.Join(bundle.Campaign.Root, filepath.FromSlash(file.Path)), c.limits.MaxBytes)
		if err != nil {
			return refuse(err)
		}
		if !loaded[file.Path] || !bytes.Equal(content, file.Content) {
			return refuse(fmt.Errorf("compiled source differs at %s", file.Path))
		}
	}
	return raw, unit, options, nil
}

func parseCampaignRepinArgs(args []string) (campaignCompileArgs, error) {
	parsed := campaignCompileArgs{}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--to":
			if parsed.out != "" || i+1 == len(args) || strings.HasPrefix(args[i+1], "-") {
				return parsed, errors.New("campaign repin requires one --to SHA")
			}
			i++
			parsed.out = args[i]
		case "--check":
			parsed.check = true
		case "--json":
			if parsed.asJSON {
				return parsed, errors.New("--json may be supplied only once")
			}
			parsed.asJSON = true
		default:
			if strings.HasPrefix(args[i], "-") || parsed.plan != "" {
				return parsed, fmt.Errorf("unexpected campaign repin argument %q", args[i])
			}
			parsed.plan = args[i]
		}
	}
	if parsed.plan == "" || parsed.out == "" {
		return parsed, errors.New("campaign repin requires DIR --to SHA")
	}
	return parsed, nil
}

func (c campaignCLI) runRepin(ctx context.Context, args []string) error {
	parsed, err := parseCampaignRepinArgs(args)
	if err != nil {
		return err
	}
	raw, source, options, err := c.loadCompiledSource(parsed.plan)
	if err != nil {
		return err
	}
	result, err := campaign.RepinCompileUnit(raw, source.ID, parsed.out, options)
	if err != nil {
		return err
	}
	if c.limits.MaxBytes > 0 && compiledUnitBytes(result.Unit) > c.limits.MaxBytes {
		return errors.New("repinned unit exceeds campaign size limit")
	}
	dir, err := campaign.WriteCompiledUnit(filepath.Dir(filepath.Clean(parsed.plan)), result.Unit, campaign.WriteOptions{Force: true})
	if err != nil {
		return err
	}
	bundle, projected, err := c.prepare(dir)
	if err != nil {
		return err
	}
	unit := campaignCompileUnit{ID: source.ID, Directory: dir, Valid: true, Errors: []string{},
		Plan: &campaignCompilePlanSummary{Name: projected.Name, Tasks: projected.Totals.Tasks, Edges: projected.Totals.Edges, Digest: bundle.ContentDigest}}
	if parsed.check {
		matrix, err := c.checkViability(ctx, projected, bundle, "")
		if err != nil {
			return fmt.Errorf("campaign repin wrote %s; readiness failed: %w", dir, err)
		}
		unit.Check = compileCheckSummary(matrix)
	}
	report := struct {
		OldRef string `json:"oldRef"`
		NewRef string `json:"newRef"`
		Digest string `json:"digest"`
		campaignCompileUnit
	}{result.OldRef, result.NewRef, bundle.ContentDigest, unit}
	if parsed.asJSON {
		err = encodeCampaignJSON(c.stdout, report)
	} else {
		_, err = fmt.Fprintf(c.stdout, "repinned %s: %s -> %s\ndigest %s\n", dir, result.OldRef, result.NewRef, bundle.ContentDigest)
		if err == nil {
			err = renderCampaignCompile(c.stdout, campaignCompileReport{Out: filepath.Dir(dir), Units: []campaignCompileUnit{unit}})
		}
	}
	if err != nil {
		return err
	}
	return afterDocument(campaignCompileVerdict(campaignCompileReport{Units: []campaignCompileUnit{unit}}))
}

func campaignRepinHelpPage() helpPage {
	return helpPage{
		Path: "campaign repin", Purpose: "recompile one compiled unit at a new pin; never submits.",
		Usage: []string{"t3-steward campaign repin DIR --to SHA [--check] [--json]"},
		Flags: []helpFlag{{Name: "--to", Value: "SHA", Required: true, Text: "Full pin; replaces literal old pins in plan text, preserving extra inputs."},
			{Name: "--check", Default: "off", Text: "Check readiness after publication; failures retain the written directory."}, jsonFlag("one document with oldRef, newRef and digest")},
		Exits:    []helpExit{{0, "written; with --check ready or accepted_waiting"}, {1, "invalid arguments, source or write"}, {8, "written but impossible"}},
		JSONKeys: []string{"oldRef", "newRef", "digest", "id", "directory", "valid", "errors", "plan", "check"},
		Parsers:  []parserSite{{Func: "parseCampaignRepinArgs"}},
		Notes:    "DIR must be the selected compiled unit directory. Reproduction validates generated inventory and bytes. Only direct plain or unescaped quoted ref scalars are supported.",
	}
}
