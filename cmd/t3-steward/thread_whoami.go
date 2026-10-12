package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func cmdThreadWhoami(g globalFlags, args []string, out io.Writer) error {
	args, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return err
	}
	if len(args) != 0 {
		return errors.New("thread whoami usage: thread whoami [--json]")
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return threadWhoami(cfg, asJSON, out)
}

func threadWhoami(cfg config.Config, asJSON bool, out io.Writer) error {
	cli := campaignCLI{resolveThread: func(explicit string) (string, error) { return resolveThread(cfg, explicit) }}
	thread, err := cli.campaignNotifyThread("campaign submit", "current")
	if err != nil {
		return err
	}
	if asJSON {
		return json.NewEncoder(out).Encode(struct {
			ThreadID string `json:"threadId"`
		}{ThreadID: thread})
	}
	_, err = fmt.Fprintln(out, thread)
	return err
}
