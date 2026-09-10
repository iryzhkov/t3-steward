package backlogadmin

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestOpenArtifactAuthorizesBeforeOpeningContent(t *testing.T) {
	denied := errors.New("permission denied")
	authorizer := &allowAuthorizer{err: denied}
	service, err := New(&countingReader{}, authorizer)
	if err != nil {
		t.Fatal(err)
	}
	opened := false
	service.SetArtifactOpener(func(context.Context, string) (domain.Artifact, io.ReadCloser, error) {
		opened = true
		return domain.Artifact{}, nil, nil
	})
	_, err = service.OpenArtifact(context.Background(), Principal{ID: "denied"}, "artifact-1")
	if !errors.Is(err, denied) {
		t.Fatalf("authorization error = %v", err)
	}
	if opened {
		t.Fatal("content opener ran before authorization")
	}
	if len(authorizer.actions) != 1 || authorizer.actions[0].Kind != QueryArtifact ||
		authorizer.actions[0].ArtifactID != "artifact-1" {
		t.Fatalf("actions = %#v", authorizer.actions)
	}
}

func TestOpenArtifactReturnsVerifiedOpenerContent(t *testing.T) {
	service, err := New(&countingReader{}, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetArtifactOpener(func(_ context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return domain.Artifact{ID: id, Name: "result.txt", MediaType: "text/plain", Size: 6},
			io.NopCloser(strings.NewReader("result")), nil
	})
	content, err := service.OpenArtifact(context.Background(), Principal{ID: "operator"}, "artifact-1")
	if err != nil {
		t.Fatal(err)
	}
	defer content.Content.Close()
	raw, err := io.ReadAll(content.Content)
	if err != nil || string(raw) != "result" || content.Metadata.ID != "artifact-1" {
		t.Fatalf("content = %#v, %q, %v", content.Metadata, raw, err)
	}
}
