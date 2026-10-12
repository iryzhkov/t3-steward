package domain

import (
	"errors"
	"regexp"
)

var campaignGroupID = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// ValidateCampaignGroup accepts an absent group or a bounded campaign identifier.
func ValidateCampaignGroup(group string) error {
	if group != "" && (len(group) > 64 || !campaignGroupID.MatchString(group)) {
		return errors.New("group must be at most 64 bytes of lower-case letters, digits and single hyphens")
	}
	return nil
}
