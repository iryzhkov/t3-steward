package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var ErrArtifactContentUnavailable = errors.New("artifact content is unavailable")

type ArtifactOpenFunc func(context.Context, string) (domain.Artifact, io.ReadCloser, error)

type ArtifactContent struct {
	Metadata ArtifactMetadata
	Content  io.ReadCloser
}

func (s *Service) SetArtifactOpener(open ArtifactOpenFunc) {
	s.artifactOpen = open
}

// OpenArtifact authorizes content retrieval separately from metadata queries.
func (s *Service) OpenArtifact(ctx context.Context, principal Principal, artifactID string) (ArtifactContent, error) {
	if strings.TrimSpace(artifactID) != artifactID || artifactID == "" {
		return ArtifactContent{}, fmt.Errorf("%w: artifact ID is required", ErrInvalidQuery)
	}
	if err := s.authorizer.Authorize(ctx, principal, Action{Kind: QueryArtifact, ArtifactID: artifactID}); err != nil {
		return ArtifactContent{}, fmt.Errorf("authorize artifact content: %w", err)
	}
	if s.artifactOpen == nil {
		return ArtifactContent{}, ErrArtifactContentUnavailable
	}
	artifact, content, err := s.artifactOpen(ctx, artifactID)
	if err != nil {
		return ArtifactContent{}, err
	}
	return ArtifactContent{Metadata: artifactDTO(artifact).Metadata, Content: content}, nil
}
