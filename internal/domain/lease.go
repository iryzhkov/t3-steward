package domain

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const DefaultLeaseTTL = 2 * time.Hour
const MinLeaseTTL = 5 * time.Minute
const MaxLeaseTTL = 12 * time.Hour

// Lease retains the last fencing token even after release or expiry.
type Lease struct {
	Name        string    `json:"name"`
	OwnerThread string    `json:"ownerThread"`
	Principal   string    `json:"principal"`
	Plan        string    `json:"plan,omitempty"`
	Reason      string    `json:"reason"`
	AcquiredAt  time.Time `json:"acquiredAt"`
	ExpiresAt   time.Time `json:"expiresAt"`
	Token       int64     `json:"token"`
	Released    bool      `json:"released,omitempty"`
}

func (l Lease) Live(now time.Time) bool { return !l.Released && now.Before(l.ExpiresAt) }

type LeaseRequest struct {
	Action      string        `json:"action"`
	Name        string        `json:"name,omitempty"`
	OwnerThread string        `json:"ownerThread,omitempty"`
	Principal   string        `json:"principal,omitempty"`
	Plan        string        `json:"plan,omitempty"`
	Reason      string        `json:"reason,omitempty"`
	RequestID   string        `json:"requestId,omitempty"`
	TTL         time.Duration `json:"ttl,omitempty"`
	Token       int64         `json:"token,omitempty"`
	Force       bool          `json:"force,omitempty"`
}

func (r LeaseRequest) Mutating() bool {
	return r.Action == "acquire" || r.Action == "renew" || r.Action == "release"
}
func (r LeaseRequest) EffectiveTTL() time.Duration {
	if r.TTL == 0 {
		return DefaultLeaseTTL
	}
	return r.TTL
}

type LeaseResponse struct {
	Lease   *Lease  `json:"lease,omitempty"`
	Leases  []Lease `json:"leases,omitempty"`
	Code    int     `json:"code"`
	Message string  `json:"message,omitempty"`
	Replay  bool    `json:"replay,omitempty"`
}

var leaseNamePattern = regexp.MustCompile(`^[A-Za-z0-9._/:-]+$`)

// ValidateLeaseName preserves catalog case and accepts only the two supported
// resource namespaces. Deployment windows are deliberately reserved.
func ValidateLeaseName(name string) error {
	if strings.HasPrefix(name, "deploy:") {
		return fmt.Errorf("deploy: leases are reserved for a follow-up")
	}
	if len(name) == 0 || len(name) > 128 || !leaseNamePattern.MatchString(name) {
		return fmt.Errorf("lease name must be 1-128 characters of [A-Za-z0-9._/:-]")
	}
	switch {
	case strings.HasPrefix(name, "repo:"):
		parts := strings.Split(strings.TrimPrefix(name, "repo:"), "/")
		if len(parts) < 2 || parts[0] == "" {
			return fmt.Errorf("repository lease must be repo:<project>/<branch>")
		}
		for _, p := range parts {
			if p == "" || p == "." || p == ".." {
				return fmt.Errorf("repository lease has an empty or invalid component")
			}
		}
	case strings.HasPrefix(name, "release:"):
		if strings.TrimPrefix(name, "release:") == "" {
			return fmt.Errorf("release lease must be release:<name>")
		}
	default:
		return fmt.Errorf("lease name must start with repo: or release: followed by a name")
	}
	return nil
}
func leaseText(label, value string, max int, required bool) error {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > max {
		return fmt.Errorf("lease %s exceeds %d characters or is invalid UTF-8", label, max)
	}
	if (required || value != "") && (value == "" || strings.TrimSpace(value) != value) {
		return fmt.Errorf("lease %s must be nonempty and trimmed", label)
	}
	// Format characters (bidi overrides, zero-width marks) and line or
	// paragraph separators would let a holder disguise the refusal text that
	// other threads read, so they are refused alongside control characters.
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			return fmt.Errorf("lease %s contains a control, format or separator character", label)
		}
	}
	return nil
}
func (r LeaseRequest) Validate() error {
	switch r.Action {
	case "acquire", "renew", "release", "check", "show", "list":
	default:
		return fmt.Errorf("unknown lease action %q", r.Action)
	}
	if r.Action != "list" {
		if err := ValidateLeaseName(r.Name); err != nil {
			return err
		}
	} else if r.Name != "" {
		return fmt.Errorf("lease list takes no name")
	}
	if r.Action != "list" && r.Action != "show" {
		if err := leaseText("owner thread", r.OwnerThread, 256, true); err != nil {
			return err
		}
	}
	if err := leaseText("plan", r.Plan, 256, false); err != nil {
		return err
	}
	if err := leaseText("reason", r.Reason, 4096, r.Action == "acquire" || r.Force); err != nil {
		return err
	}
	if r.Force && r.Action != "release" {
		return fmt.Errorf("--force is only valid for lease release")
	}
	if r.Token < 0 {
		return fmt.Errorf("lease token must be positive")
	}
	if (r.Action == "renew" || r.Action == "release" && !r.Force) && r.Token < 1 {
		return fmt.Errorf("lease %s requires a positive token", r.Action)
	}
	if r.Action == "acquire" || r.Action == "renew" {
		ttl := r.EffectiveTTL()
		if ttl < MinLeaseTTL || ttl > MaxLeaseTTL {
			return fmt.Errorf("lease TTL must be between 5m and 12h")
		}
	}
	if r.Mutating() {
		if err := leaseText("request id", r.RequestID, 256, true); err != nil {
			return err
		}
		if err := leaseText("principal", r.Principal, 256, true); err != nil {
			return err
		}
	}
	return nil
}
