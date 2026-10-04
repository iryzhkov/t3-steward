package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// runQuarantineRelease clears one intake quarantine deliberately.
//
// This authenticated, reason-bound operation clears a retained historical
// marker. It does not retry a file or reenable retired intake.
func (c backlogAdminCLI) runQuarantineRelease(ctx context.Context, args []string) error {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return err
	}
	if len(clean) == 0 {
		return errors.New("quarantine release usage: backlog quarantine release <key> --reason TEXT [--json]")
	}
	request := backlogadmin.QuarantineReleaseRequest{Key: clean[0]}
	rest := clean[1:]
	for len(rest) > 0 {
		if len(rest) < 2 {
			return fmt.Errorf("%s needs a value", rest[0])
		}
		flag, value := rest[0], rest[1]
		rest = rest[2:]
		switch flag {
		case "--reason":
			if request.Reason != "" {
				return errors.New("--reason may only be specified once")
			}
			request.Reason = value
		default:
			return fmt.Errorf("unknown quarantine release flag %q", flag)
		}
	}
	if request.Reason == "" {
		return errors.New("quarantine release needs --reason TEXT")
	}
	if c.quarantine == nil {
		return errors.New("quarantine release is unavailable on this transport")
	}
	release, err := c.quarantine.ReleaseQuarantine(ctx, c.principal, request)
	if err != nil {
		return err
	}
	if asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(release)
	}
	if !release.Released {
		fmt.Fprintf(c.stdout, "no quarantine for %s; nothing to release.\n", release.Key)
		return nil
	}
	fmt.Fprintf(c.stdout, "released %s (digest %s)\nit was refused because: %s\n"+
		"the historical marker is cleared; this never retries a file or reenables intake.\n"+
		"Submit new work with t3-steward task run or campaign submit.\n",
		release.Key, release.Digest, release.Reason)
	return nil
}
