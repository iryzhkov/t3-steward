package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func (c backlogAdminCLI) runGraphAmendment(ctx context.Context, args []string) error {
	request, err := parseGraphAmendment(args)
	if err != nil {
		return err
	}
	client, ok := c.service.(interface {
		AmendGraph(context.Context, domain.GraphAmendment) (domain.GraphAmendmentResult, error)
	})
	if !ok {
		return errors.New("graph amendment client unavailable")
	}
	result, err := client.AmendGraph(ctx, request)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(c.stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func parseGraphAmendment(args []string) (domain.GraphAmendment, error) {
	var r domain.GraphAmendment
	if len(args) < 2 {
		return r, errors.New("expected task add|set, edge add|remove, or run clone")
	}
	r.Operation = args[0] + "-" + args[1]
	if r.Operation == "run-clone" {
		r.Operation = "clone"
	}
	rest := args[2:]
	if r.Operation != "clone" {
		if len(rest) < 1 {
			return r, errors.New("amendment needs <run>/<task>")
		}
		run, task, err := splitTaskTarget(rest[0])
		if err != nil {
			return r, err
		}
		r.RunID, r.TaskID = run, task
		rest = rest[1:]
	}
	fs := flag.NewFlagSet("graph amendment", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&r.ID, "request-id", "", "stable idempotency ID")
	fs.Int64Var(&r.ExpectedRevision, "expected-revision", 0, "expected graph revision")
	fs.StringVar(&r.Reason, "reason", "", "audit reason")
	var model, provider, timeout, options, needs, class string
	var maxTurns int
	fs.StringVar(&model, "model", "", "model")
	fs.StringVar(&provider, "provider", "", "provider instance")
	fs.StringVar(&timeout, "timeout", "", "elapsed assignment budget, e.g. 30m")
	fs.StringVar(&options, "options", "", "JSON object of provider options")
	fs.StringVar(&r.Prompt, "prompt", "", "task prompt")
	fs.StringVar(&needs, "needs", "", "comma-separated dependency names or run/task refs")
	fs.StringVar(&class, "class", "surplus", "required or surplus")
	fs.IntVar(&maxTurns, "max-turns", 1, "maximum turns")
	if r.Operation == "clone" {
		fs.StringVar(&r.RunID, "from", "", "source run")
	}
	if strings.HasPrefix(r.Operation, "edge-") {
		fs.StringVar(&r.Source, "from", "", "source task or run/task")
	}
	fs.Bool("json", false, "JSON output")
	if err := fs.Parse(rest); err != nil {
		return r, err
	}
	if fs.NArg() != 0 {
		return r, errors.New("unexpected amendment arguments")
	}
	supplied := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { supplied[f.Name] = true })
	common := map[string]bool{"request-id": true, "expected-revision": true, "reason": true, "json": true}
	allowed := map[string]bool{}
	switch r.Operation {
	case "task-add":
		for _, key := range []string{"model", "provider", "timeout", "options", "prompt", "needs", "class", "max-turns"} {
			allowed[key] = true
		}
	case "task-set":
		for _, key := range []string{"model", "provider", "timeout", "options"} {
			allowed[key] = true
		}
	case "edge-add", "edge-remove", "clone":
		allowed["from"] = true
	default:
		return r, errors.New("unknown graph amendment command")
	}
	for key := range supplied {
		if !common[key] && !allowed[key] {
			return r, fmt.Errorf("--%s is not valid for %s", key, r.Operation)
		}
	}
	var parsedOptions map[string]string
	if supplied["options"] {
		if err := json.Unmarshal([]byte(options), &parsedOptions); err != nil || parsedOptions == nil {
			return r, errors.New("--options must be a JSON object of strings")
		}
		r.Options = &parsedOptions
	}
	var duration time.Duration
	if supplied["timeout"] {
		var err error
		duration, err = time.ParseDuration(timeout)
		if err != nil || duration < 0 {
			return r, errors.New("invalid timeout")
		}
		r.Timeout = &duration
	}
	if supplied["model"] {
		r.Model = &model
	}
	if supplied["provider"] {
		r.Provider = &provider
	}
	if r.Operation == "task-add" {
		task := domain.Task{Name: r.TaskID, Class: domain.TaskClass(class), MaxTurns: maxTurns, Timeout: duration, Routes: []domain.ProviderRoute{{ProviderInstanceID: provider, Model: model, Options: parsedOptions}}}
		if needs != "" {
			for _, need := range strings.Split(needs, ",") {
				if strings.Contains(need, "/") {
					ref, err := domain.ParseNodeRef(need)
					if err != nil {
						return r, err
					}
					task.ExternalNeeds = append(task.ExternalNeeds, ref)
				} else {
					task.Needs = append(task.Needs, need)
				}
			}
		}
		r.Task = &task
		r.TaskID = ""
		r.Model = nil
		r.Provider = nil
		r.Options = nil
		r.Timeout = nil
	}
	return r, domain.ValidateGraphAmendment(r)
}
