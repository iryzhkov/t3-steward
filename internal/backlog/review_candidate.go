package backlog

import (
	"errors"
	"slices"

	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
)

func validateReviewCandidateManifest(m Manifest) error {
	r := m.Review
	switch r.CandidateMode {
	case "":
		if r.BundleInput != "" {
			return errors.New("review bundle input requires bundle candidate mode")
		}
		if r.HeadCommit != "" && (m.Environment.Type != EnvironmentGit || m.Environment.Ref != r.HeadCommit || r.BaseCommit == "") {
			return errors.New("diff review requires checkout at the pinned head and a pinned base")
		}
	case "commit":
		if m.Environment.Type != EnvironmentGit || !review.FullCommitID(r.HeadCommit) || m.Environment.Ref != r.HeadCommit || r.BundleInput != "" ||
			(r.BaseCommit != "" && !review.FullCommitID(r.BaseCommit)) {
			return errors.New("commit review requires checkout at its pinned head")
		}
	case "bundle":
		if m.Environment.Type != EnvironmentGit || !review.FullCommitID(r.HeadCommit) || !pinnedinput.ValidName(r.BundleInput) ||
			!m.PinnedInputs || !slices.Contains(m.Inputs, r.BundleInput) || m.Environment.Ref == "" ||
			(r.BaseCommit != "" && (!review.FullCommitID(r.BaseCommit) || m.Environment.Ref != r.BaseCommit)) {
			return errors.New("bundle review requires a pinned bundle input and prerequisite checkout")
		}
	default:
		return errors.New("unknown review candidate mode")
	}
	return nil
}
