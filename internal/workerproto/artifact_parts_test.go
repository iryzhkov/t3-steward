package workerproto

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"
	"time"
)

func TestClientFetchSplitArtifactBoundedAndVerified(t *testing.T) {
	now := time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)
	data := bytes.Repeat([]byte("x"), 26929544)
	// Distinct parts make accidental reordering observable.
	for index := 16 << 20; index < len(data); index++ {
		data[index] = 'y'
	}
	object := artifactObject("large-output", "results/metrics.json", data)
	object.Kind = "output"
	split, err := SplitArtifact(bytes.NewReader(data), object, StreamArtifactLimit)
	if err != nil {
		t.Fatal(err)
	}
	manifest := ArtifactTransferManifest{Version: 1, ID: "upload-assignment-1-result", Direction: "upload", CoordinatorEpoch: 9, WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: "assignment-1", AssignmentEpoch: 1, Objects: []ArtifactObject{split}, TotalBytes: int64(len(data)), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	announced := ArtifactUploadResponse{Manifest: manifest}
	for _, mutation := range []string{"valid", "corrupt", "truncated", "wrong-whole"} {
		t.Run(mutation, func(t *testing.T) {
			response := announced
			if mutation == "wrong-whole" {
				response.Manifest.Objects = append([]ArtifactObject(nil), manifest.Objects...)
				response.Manifest.Objects[0].SHA256 = hex.EncodeToString(make([]byte, sha256.Size))
			}
			calls := 0
			transport := artifactClientTransport{fetch: func(_ context.Context, request Envelope, _ RetryPolicy, limit int64) (Envelope, []byte, error) {
				calls++
				var download ArtifactDownloadRequest
				if err := DecodePayload(request, MessageArtifactUpload, &download); err != nil {
					t.Fatal(err)
				}
				if download.Selection == nil || download.Selection.ObjectID != object.ID || limit > StreamArtifactLimit {
					t.Fatalf("unbounded selection: %+v limit %d", download.Selection, limit)
				}
				index := download.Selection.PartIndex
				start := int64(index) * StreamArtifactLimit
				part := response.Manifest.Objects[0].Parts[index]
				raw := append([]byte(nil), data[start:start+part.Size]...)
				if index == 1 && mutation == "corrupt" {
					raw[0] ^= 1
				}
				if index == 1 && mutation == "truncated" {
					raw = raw[:len(raw)-1]
				}
				envelope, err := clientResponse(t, request, MessageArtifactUpload, response)
				return envelope, raw, err
			}}
			client := newProtocolClient(t, now, transport)
			fetched, err := client.FetchArtifact(context.Background(), response, StreamArtifactLimit, 64<<20)
			if mutation != "valid" {
				if err == nil {
					t.Fatal("invalid split payload accepted")
				}
				return
			}
			if err != nil || calls != 2 {
				t.Fatalf("fetch: %v calls %d", err, calls)
			}
			reader, err := fetched.OpenWorkerUpload(context.Background(), split)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			got, err := io.ReadAll(reader)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatal("assembled output mismatch")
			}
		})
	}
	if err := VerifyArtifact(bytes.NewReader(data[:len(data)-1]), split, StreamArtifactLimit); err == nil {
		t.Fatal("truncated part verified")
	}
	small := artifactObject("small-output", "results/small.txt", []byte("small"))
	unchanged, err := SplitArtifact(bytes.NewReader([]byte("small")), small, StreamArtifactLimit)
	if err != nil || len(unchanged.Parts) != 0 || unchanged.SHA256 != small.SHA256 {
		t.Fatal("small output changed")
	}
}

func TestSplitArtifactMetadataBounds(t *testing.T) {
	data := bytes.Repeat([]byte("z"), 20)
	object := artifactObject("output", "results/result.txt", data)
	split, err := SplitArtifact(bytes.NewReader(data), object, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateArtifactObject(split, 8); err != nil {
		t.Fatal(err)
	}
	changed := split
	changed.Parts = append([]ArtifactPart(nil), split.Parts...)
	changed.Parts[0].Size = 9
	if err := ValidateArtifactObject(changed, 8); err == nil {
		t.Fatal("oversized physical part accepted")
	}
	changed = split
	changed.Size++
	if err := ValidateArtifactObject(changed, 8); err == nil {
		t.Fatal("parts total mismatch accepted")
	}
	if _, err := ValidateUploadObjects([]ArtifactObject{split}, 8, 19); err == nil {
		t.Fatal("aggregate bound bypassed by split")
	}
}
