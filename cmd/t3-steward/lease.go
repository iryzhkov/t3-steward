package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const leaseUsage = `Usage: t3-steward lease <acquire|renew|release|check|show|list> [NAME] [options]

Hold named, expiring, fenced ownership leases on repository main branches and
release manifests, so two coordinating threads cannot integrate or release the
same repository at once.

  NAME: repo:<project>/<branch> or release:<name>; case is preserved.
  --config PATH        Client configuration.
  --owner-thread ID    Holder thread, default current; resolves the calling T3 thread.
  --ttl DURATION       Acquire/renew duration, default 2h, range 5m..12h.
  --token N            Required fencing token for renew/release (except --force).
  --plan TEXT          Plan reference, up to 256 characters.
  --reason TEXT        Required for acquire and forced release.
  --force              Release another holder's lease, audited by thread/principal.
  --request-id KEY     Mutation replay key; generated if omitted. Reuse it when no
                       answer arrived. The first answer for a key is replayed, so
                       after an error answer, retry with a new --request-id.
  --json               Print one JSON response, including a refusal.

Acquire a lease before integrating or releasing; check immediately before pushing
main or upkeeper push, and release afterward. Same-holder acquire returns the
existing token without extending expiry. A fresh acquire increments the token.
Exit 0: success/held by caller; 10: conflict, fencing or authority refusal;
11: check found free/expired. Transport failures retain exits 3..8 and fail closed.
Show/list/check are reads. deploy: names are reserved and refused.
An older coordinator must be upgraded before it can serve leases.
`

type leaseCLI struct {
	stdout   io.Writer
	resolve  func(string) (string, error)
	exchange func(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error)
	// now judges whether a replayed grant is still live; time.Now when nil.
	now func() time.Time
}

func cmdLease(g globalFlags, args []string) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	cli := leaseCLI{stdout: os.Stdout, resolve: func(explicit string) (string, error) { return resolveThread(cfg, explicit) },
		exchange: func(ctx context.Context, req domain.LeaseRequest) (domain.LeaseResponse, error) {
			transport, err := newCoordinatorTransport(cfg)
			if err != nil {
				return domain.LeaseResponse{}, err
			}
			client, ok := transport.client.(backlogadmin.LeaseTransport)
			if !ok {
				return domain.LeaseResponse{}, errors.New("the coordinator does not support leases; upgrade it")
			}
			return client.Lease(ctx, req)
		}}
	return cli.run(context.Background(), args)
}

func parseLeaseArgs(args []string) (domain.LeaseRequest, string, bool, error) {
	req := domain.LeaseRequest{TTL: 2 * time.Hour}
	owner := "current"
	asJSON := false
	if len(args) == 0 {
		return req, owner, asJSON, errors.New("lease requires a verb")
	}
	req.Action = args[0]
	index := 1
	switch req.Action {
	case "list":
	case "acquire", "renew", "release", "check", "show":
		if len(args) < 2 || strings.HasPrefix(args[1], "--") {
			return req, owner, asJSON, errors.New("lease requires NAME")
		}
		req.Name = args[1]
		index = 2
	default:
		return req, owner, asJSON, fmt.Errorf("unknown lease verb %q", req.Action)
	}
	seen := map[string]bool{}
	for ; index < len(args); index++ {
		option := args[index]
		if seen[option] {
			return req, owner, asJSON, fmt.Errorf("%s provided more than once", option)
		}
		seen[option] = true
		switch option {
		case "--json":
			asJSON = true
		case "--force":
			req.Force = true
		case "--owner-thread", "--ttl", "--token", "--plan", "--reason", "--request-id":
			if index+1 >= len(args) {
				return req, owner, asJSON, fmt.Errorf("%s needs a value", option)
			}
			index++
			value := args[index]
			var err error
			switch option {
			case "--owner-thread":
				owner = value
			case "--ttl":
				req.TTL, err = time.ParseDuration(value)
				if err == nil && (req.TTL < domain.MinLeaseTTL || req.TTL > domain.MaxLeaseTTL) {
					err = errors.New("lease TTL must be between 5m and 12h")
				}
			case "--token":
				req.Token, err = strconv.ParseInt(value, 10, 64)
			case "--plan":
				req.Plan = value
			case "--reason":
				req.Reason = value
			case "--request-id":
				req.RequestID = value
			}
			if err != nil {
				return req, owner, asJSON, fmt.Errorf("%s: %w", option, err)
			}
		default:
			return req, owner, asJSON, fmt.Errorf("unknown lease option %q", option)
		}
	}
	if owner == "" {
		return req, owner, asJSON, errors.New("--owner-thread must not be empty")
	}
	if req.Mutating() && req.RequestID == "" {
		var err error
		req.RequestID, err = newAdminCommandID()
		if err != nil {
			return req, owner, asJSON, err
		}
	}
	return req, owner, asJSON, nil
}

func (c leaseCLI) run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		_, err := fmt.Fprint(c.stdout, leaseUsage)
		return err
	}
	req, owner, asJSON, err := parseLeaseArgs(args)
	if err != nil {
		return err
	}
	if req.Action != "show" && req.Action != "list" {
		if owner == "current" {
			owner = ""
		}
		req.OwnerThread, err = c.resolve(owner)
		if err != nil || req.OwnerThread == "" {
			return fmt.Errorf("cannot resolve current holder; pass --owner-thread with a canonical T3 thread id: %v", err)
		}
	}
	// Principal is bound by the coordinator, never supplied by this client.
	validation := req
	validation.Principal = "client-validation"
	if err := validation.Validate(); err != nil {
		return err
	}
	response, err := c.exchange(ctx, req)
	if err != nil {
		return err
	}
	// The coordinator replays the first answer for a request id verbatim. A
	// replayed grant whose lease has since expired no longer means the caller
	// holds it: the name may already belong to another thread.
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	if response.Replay && response.Code == 0 && (req.Action == "acquire" || req.Action == "renew") && response.Lease != nil && !response.Lease.Live(now()) {
		response.Code = 10
		response.Message = fmt.Sprintf("refused: replayed answer for request id %s granted token %d until %s, which has expired; %s again with a new --request-id", req.RequestID, response.Lease.Token, response.Lease.ExpiresAt.UTC().Format(time.RFC3339), req.Action)
	}
	if asJSON {
		if err := json.NewEncoder(c.stdout).Encode(response); err != nil {
			return err
		}
	} else if response.Message != "" {
		if _, err := fmt.Fprintln(c.stdout, response.Message); err != nil {
			return err
		}
	} else if response.Lease != nil {
		l := response.Lease
		var outputErr error
		if l.Released {
			_, outputErr = fmt.Fprintf(c.stdout, "lease %s released token %d\n", l.Name, l.Token)
		} else {
			_, outputErr = fmt.Fprintf(c.stdout, "lease %s held by thread %s token %d until %s\n", l.Name, l.OwnerThread, l.Token, l.ExpiresAt.UTC().Format(time.RFC3339))
		}
		if outputErr != nil {
			return outputErr
		}
	} else {
		for _, l := range response.Leases {
			if _, err := fmt.Fprintf(c.stdout, "%s thread %s token %d until %s\n", l.Name, l.OwnerThread, l.Token, l.ExpiresAt.UTC().Format(time.RFC3339)); err != nil {
				return err
			}
		}
	}
	if response.Code != 0 {
		err := exitCodeError{code: response.Code, error: errors.New(response.Message)}
		if asJSON {
			return afterDocument(err)
		}
		return err
	}
	return nil
}
