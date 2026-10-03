package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Content is base64 on the wire: one bounded document cannot expand sixfold
// through JSON string escaping or duplicate findings in a round response.
type ReviewDocument struct {
	RoundID    string `json:"roundId"`
	ReviewerID string `json:"reviewerId"`
	Name       string `json:"name"`
	Content    []byte `json:"content"`
}
type reviewRoundReader interface {
	GetReviewRound(context.Context, string) (review.Round, error)
}

func (s *Service) queryReviewRound(ctx context.Context, q Query) (Response, error) {
	reader, ok := s.reader.(reviewRoundReader)
	if !ok {
		return Response{}, fmt.Errorf("%w: this coordinator does not record review rounds; upgrade the coordinator", ErrInvalidQuery)
	}
	round, err := reader.GetReviewRound(ctx, q.RoundID)
	if errors.Is(err, sqlite.ErrReviewRoundNotFound) {
		return Response{}, notFound("review round", q.RoundID)
	}
	if err != nil {
		return Response{}, err
	}
	if q.Kind == QueryReviewDocument {
		for _, v := range round.Reviewers {
			if v.ID != q.ReviewerID {
				continue
			}
			var raw []byte
			if q.ReviewDocument == "review.md" {
				raw = []byte(v.ReviewMD)
			} else {
				raw = v.VerdictJSON
			}
			if len(raw) == 0 {
				return Response{}, notFound("review document", q.ReviewDocument)
			}
			return Response{Version: Version, Kind: q.Kind, GeneratedAt: s.now().UTC(), ReviewDocument: &ReviewDocument{RoundID: q.RoundID, ReviewerID: q.ReviewerID, Name: q.ReviewDocument, Content: raw}}, nil
		}
		return Response{}, notFound("reviewer", q.ReviewerID)
	}
	metadata := round.Metadata()
	return Response{Version: Version, Kind: q.Kind, GeneratedAt: s.now().UTC(), ReviewRound: &metadata}, nil
}
