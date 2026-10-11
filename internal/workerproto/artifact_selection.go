package workerproto

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// ArtifactSelection requests a bounded physical object from an immutable upload.
// PartIndex -1 selects an unsplit object; nonnegative indices select a part.
type ArtifactSelection struct {
	ObjectID  string `json:"objectId"`
	PartIndex int    `json:"partIndex"`
}

func SelectedArtifactObjects(manifest ArtifactTransferManifest, selection *ArtifactSelection) ([]ArtifactObject, error) {
	if selection == nil {
		return manifest.Objects, nil
	}
	for _, object := range manifest.Objects {
		if object.ID != selection.ObjectID {
			continue
		}
		if selection.PartIndex == -1 && len(object.Parts) == 0 && object.Size <= StreamArtifactLimit {
			return []ArtifactObject{object}, nil
		}
		if selection.PartIndex >= 0 && selection.PartIndex < len(object.Parts) {
			return []ArtifactObject{ArtifactPartObject(object, selection.PartIndex)}, nil
		}
		return nil, errors.New("artifact selection: invalid part")
	}
	return nil, errors.New("artifact selection: unknown object")
}

func (c *Client) fetchArtifactBytes(ctx context.Context, transport ArtifactRoundTripper, announced ArtifactUploadResponse, ids []string, maxTotal int64) ([]byte, error) {
	manifest := announced.Manifest
	selections := []*ArtifactSelection{nil}
	if manifest.TotalBytes > StreamArtifactLimit {
		selections = nil
		for _, object := range manifest.Objects {
			if len(object.Parts) == 0 {
				if object.Size > StreamArtifactLimit {
					return nil, errors.New("artifact fetch: oversized unsplit object")
				}
				selections = append(selections, &ArtifactSelection{ObjectID: object.ID, PartIndex: -1})
			} else {
				for index := range object.Parts {
					selections = append(selections, &ArtifactSelection{ObjectID: object.ID, PartIndex: index})
				}
			}
		}
	}
	var raw []byte
	for _, selection := range selections {
		c.sequence++
		now := c.config.Now().UTC()
		requestID := c.config.SessionID + "-" + strconv.FormatInt(c.sequence, 10)
		request, err := NewEnvelope(MessageArtifactUpload, c.config.SessionID, requestID, c.config.CoordinatorID, c.config.WorkerID, c.config.CoordinatorEpoch, c.config.WorkerEpoch, c.sequence, now, now.Add(c.config.RequestTimeout), ArtifactDownloadRequest{ManifestID: manifest.ID, ObjectIDs: ids, Selection: selection})
		if err != nil {
			return nil, err
		}
		if err := SignEnvelope(&request, c.config.SignerPrincipal, c.config.SignerKeyID, c.config.SignerSecret); err != nil {
			return nil, err
		}
		objects, err := SelectedArtifactObjects(manifest, selection)
		if err != nil {
			return nil, err
		}
		expected := manifest.TotalBytes
		if selection != nil {
			expected = objects[0].Size
		}
		limit := maxTotal
		if selection != nil {
			limit = expected
		}
		envelope, data, err := transport.RoundTripArtifactWithRetry(ctx, request, c.config.RetryPolicy, limit)
		if err != nil {
			c.unusable = true
			return nil, fmt.Errorf("worker protocol client: artifact exchange %s: %w", requestID, err)
		}
		if envelope.Type == MessageError {
			var protocolErr ProtocolError
			if err := DecodePayload(envelope, MessageError, &protocolErr); err != nil {
				return nil, err
			}
			return nil, &protocolErr
		}
		var response ArtifactUploadResponse
		if err := DecodePayload(envelope, MessageArtifactUpload, &response); err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(response, announced) {
			return nil, errors.New("worker protocol client: fetched artifact metadata changed after announcement")
		}
		if int64(len(data)) != expected || int64(len(data)) > maxTotal-int64(len(raw)) {
			return nil, errors.New("artifact fetch: physical payload size mismatch")
		}
		raw = append(raw, data...)
	}
	return raw, nil
}
