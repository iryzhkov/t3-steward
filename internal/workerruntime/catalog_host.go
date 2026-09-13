package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const MessageCatalog workerproto.MessageType = "catalog-projection"

type CatalogRequest struct {
	Projection       CatalogProjection `json:"projection"`
	ExpectedRevision string            `json:"expectedRevision"`
}

type retainedCatalog struct {
	AppliedAt        time.Time         `json:"appliedAt"`
	Projection       CatalogProjection `json:"projection"`
	CoordinatorEpoch int64             `json:"coordinatorEpoch"`
}

// CatalogHost owns a single worker runtime independently of SSH connections.
// Calls serialize catalog publication, epoch adoption and execution effects.
type CatalogHost struct {
	Home              string
	Bootstrap         WorkerBootstrap
	Options           WorkerServiceOptions
	mu                sync.Mutex
	retained          *retainedCatalog
	service           *WorkerService
	activeCredentials ProtocolCredentials
}

func (h *CatalogHost) catalogPath() string {
	return filepath.Join(h.Home, ".local/state/t3-steward/worker/catalog.json")
}
func (h *CatalogHost) Load(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	raw, err := readPrivateFile(h.catalogPath(), 8<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var retained retainedCatalog
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&retained); err != nil {
		return err
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("retained catalog has trailing data")
	}
	if err = h.activate(ctx, retained); err != nil {
		return err
	}
	return nil
}
func (h *CatalogHost) activate(ctx context.Context, c retainedCatalog) error {
	settings, err := c.Projection.Settings(h.Bootstrap, h.Home)
	if err != nil {
		return err
	}
	options := h.Options
	if options.RuntimeIdentity != nil {
		identity := *options.RuntimeIdentity
		identity.LastReload = c.AppliedAt
		options.RuntimeIdentity = &identity
	}
	options.Settings = settings
	options.WorkerID = h.Bootstrap.WorkerID
	options.WorkerEpoch = c.Projection.Worker.Epoch
	options.CoordinatorEpoch = c.CoordinatorEpoch
	_, _, root := WorkerRoots(settings, options.WorkerID)
	epoch, err := JournalCoordinatorEpoch(root)
	if err != nil {
		return err
	}
	options.CoordinatorEpoch = max(options.CoordinatorEpoch, epoch)
	credentials, err := options.ProtocolCredentials.ResolveProtocol(ctx, h.Bootstrap.CredentialRef)
	if err != nil {
		return err
	}
	serviceOptions := options
	serviceOptions.ProtocolCredentials = fixedProtocolCredentials{credentials: credentials}
	service, err := NewWorkerService(ctx, serviceOptions)
	if err != nil {
		return err
	}
	h.retained = &c
	h.service = service
	h.activeCredentials = credentials
	h.Options = options
	return nil
}

// Reconcile continues retained execution even when every SSH bridge disconnects.
func (h *CatalogHost) Reconcile(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.service == nil {
		return nil
	}
	return h.service.Exchange.Runtime.Reconcile(ctx)
}

func (h *CatalogHost) HandleFrame(ctx context.Context, raw []byte) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	envelope, buffered, err := ReadStreamEnvelope(bytes.NewReader(raw), codec)
	if err != nil {
		return nil, err
	}
	if envelope.Type == MessageCatalog {
		if _, err = buffered.ReadByte(); !errors.Is(err, io.EOF) {
			return nil, errors.New("catalog request has trailing bytes")
		}
		return h.acceptCatalog(ctx, envelope)
	}
	if h.service == nil {
		return nil, errors.New("worker is configured but has no accepted catalog")
	}
	epoch, err := AdoptCoordinatorEpoch(ctx, h.Options, envelope)
	if err != nil {
		return nil, err
	}
	if epoch > h.Options.CoordinatorEpoch {
		c := *h.retained
		c.CoordinatorEpoch = epoch
		if err = h.activate(ctx, c); err != nil {
			return nil, err
		}
	}
	var output bytes.Buffer
	switch envelope.Type {
	case workerproto.MessageArtifactDownload:
		err = h.service.ServeArtifactReceiveEnvelope(ctx, envelope, buffered, &output)
	case workerproto.MessageArtifactUpload:
		err = h.service.ServeArtifactSendEnvelope(ctx, envelope, buffered, &output)
	default:
		if _, e := buffered.ReadByte(); !errors.Is(e, io.EOF) {
			return nil, errors.New("worker control has trailing bytes")
		}
		err = h.service.ServeEnvelope(ctx, envelope, &output)
	}
	return output.Bytes(), err
}
func (h *CatalogHost) acceptCatalog(ctx context.Context, envelope workerproto.Envelope) ([]byte, error) {
	credentials, err := h.Options.ProtocolCredentials.ResolveProtocol(ctx, h.Bootstrap.CredentialRef)
	if err != nil {
		return nil, err
	}
	if credentials.CoordinatorPrincipal != "ssh:"+h.Bootstrap.CoordinatorID {
		return nil, errors.New("bootstrap coordinator principal mismatch")
	}
	floor := int64(1)
	if h.retained != nil {
		floor = h.Options.CoordinatorEpoch
	}
	if envelope.CoordinatorEpoch < floor {
		return nil, errors.New("stale catalog coordinator epoch")
	}
	// Constructing the protocol validator creates no filesystem/runtime state.
	server, err := workerproto.NewServer(workerproto.ServerConfig{CoordinatorID: h.Bootstrap.CoordinatorID, WorkerID: h.Bootstrap.WorkerID, CoordinatorEpoch: envelope.CoordinatorEpoch, WorkerEpoch: envelope.WorkerEpoch,
		PeerPrincipal: credentials.CoordinatorPrincipal, PeerKeyID: credentials.CoordinatorKeyID, PeerSecret: credentials.CoordinatorSecret,
		SignerPrincipal: credentials.WorkerPrincipal, SignerKeyID: credentials.WorkerKeyID, SignerSecret: credentials.WorkerSecret,
		Allowed: map[workerproto.MessageType]bool{MessageCatalog: true}, MaxClockSkew: time.Minute})
	if err != nil {
		return nil, err
	}
	if err = server.ValidateRequest(envelope); err != nil {
		return nil, err
	}
	var request CatalogRequest
	if err = workerproto.DecodePayload(envelope, MessageCatalog, &request); err != nil {
		return nil, err
	}
	projection := request.Projection
	if h.retained != nil && h.retained.Projection.Revision != projection.Revision && request.ExpectedRevision != h.retained.Projection.Revision {
		return nil, errors.New("stale expected catalog revision")
	}
	settings, err := projection.Settings(h.Bootstrap, h.Home)
	if err != nil {
		return nil, err
	}
	if projection.Worker.Epoch != envelope.WorkerEpoch {
		return nil, errors.New("catalog worker epoch mismatch")
	}
	if h.retained != nil && h.retained.Projection.Revision != projection.Revision {
		state, err := h.service.Exchange.Runtime.journal.snapshot()
		if err != nil {
			return nil, err
		}
		for _, record := range state.Attempts {
			if record.SettlePending || (record.Phase != PhaseCompleted && record.Phase != PhaseFailed) {
				return nil, errors.New("catalog change requires draining retained execution")
			}
		}
		if projection.Worker.Epoch != h.retained.Projection.Worker.Epoch {
			return nil, errors.New("worker epoch change requires explicit custody recovery")
		}
	}
	_, _, root := WorkerRoots(settings, h.Bootstrap.WorkerID)
	replay, err := OpenProtocolReplayStore(filepath.Join(root, "catalog-protocol"), h.Bootstrap.CoordinatorID, h.Bootstrap.WorkerID, envelope.CoordinatorEpoch, envelope.WorkerEpoch)
	if err != nil {
		return nil, err
	}
	// Retain catalog command identities separately from execution sessions.
	server, err = workerproto.NewServer(workerproto.ServerConfig{CoordinatorID: h.Bootstrap.CoordinatorID, WorkerID: h.Bootstrap.WorkerID, CoordinatorEpoch: envelope.CoordinatorEpoch, WorkerEpoch: envelope.WorkerEpoch,
		PeerPrincipal: credentials.CoordinatorPrincipal, PeerKeyID: credentials.CoordinatorKeyID, PeerSecret: credentials.CoordinatorSecret,
		SignerPrincipal: credentials.WorkerPrincipal, SignerKeyID: credentials.WorkerKeyID, SignerSecret: credentials.WorkerSecret,
		Allowed: map[workerproto.MessageType]bool{MessageCatalog: true}, MaxClockSkew: time.Minute, ReplayStore: replay})
	if err != nil {
		return nil, err
	}
	response, err := server.Handle(ctx, envelope, func(ctx context.Context, _ workerproto.Envelope) (workerproto.MessageType, any, error) {
		if h.retained != nil && h.retained.Projection.Revision == projection.Revision && h.Options.CoordinatorEpoch == envelope.CoordinatorEpoch && reflect.DeepEqual(credentials, h.activeCredentials) {
			return MessageCatalog, map[string]string{"revision": projection.Revision, "workerId": h.Bootstrap.WorkerID}, nil
		}
		next := retainedCatalog{Projection: projection, CoordinatorEpoch: envelope.CoordinatorEpoch, AppliedAt: time.Now().UTC()}
		// Validate the complete runtime before publishing its durable pointer.
		candidate := &CatalogHost{Home: h.Home, Bootstrap: h.Bootstrap, Options: h.Options}
		if err := candidate.activate(ctx, next); err != nil {
			return "", nil, err
		}
		raw, err := json.Marshal(next)
		if err != nil {
			return "", nil, err
		}
		if err = writeCatalogFile(h.catalogPath(), raw); err != nil {
			return "", nil, err
		}
		h.retained = candidate.retained
		h.service = candidate.service
		h.activeCredentials = candidate.activeCredentials
		h.Options = candidate.Options
		return MessageCatalog, map[string]string{"revision": projection.Revision, "workerId": h.Bootstrap.WorkerID}, nil
	})
	if err != nil {
		return nil, err
	}
	// A response lost after durable publication is replayed without reapplying.
	var output bytes.Buffer
	err = (workerproto.Codec{MaxBytes: 8 << 20}).Encode(&output, response)
	return output.Bytes(), err
}

type fixedProtocolCredentials struct{ credentials ProtocolCredentials }

func (r fixedProtocolCredentials) ResolveProtocol(context.Context, string) (ProtocolCredentials, error) {
	return r.credentials, nil
}

func writeCatalogFile(path string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".catalog-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if _, err = f.Write(raw); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
