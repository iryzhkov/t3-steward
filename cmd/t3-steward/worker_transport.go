package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"net"
	"time"
)

func newPersistentWorkerTransport(worker config.V2Worker, credentials workerruntime.ProtocolCredentials, settings config.BacklogV2, factory workerproto.CommandFactory) (*workerproto.StreamTransport, error) {
	config := workerproto.SSHConfig{Address: worker.Address, RemoteCommand: ".local/bin/t3-steward", RemoteArguments: []string{"worker", "bridge"}, RequestTimeout: settings.Transport.RequestTimeout.D(), ConnectTimeout: 10 * time.Second, MaxMessageBytes: settings.MessageLimits.MaxBytes, MaxStderrBytes: 64 << 10, ResponsePrincipal: credentials.WorkerPrincipal, ResponseKeyID: credentials.WorkerKeyID, ResponseSecret: credentials.WorkerSecret, Factory: factory}
	var dial workerproto.StreamDialer
	if worker.Connection == "unix" {
		dial = func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{Timeout: config.ConnectTimeout}).DialContext(ctx, "unix", worker.Address)
		}
	} else {
		var err error
		dial, err = workerproto.SSHStreamDialer(config)
		if err != nil {
			return nil, err
		}
	}
	return workerproto.NewStreamTransport(config, dial)
}
