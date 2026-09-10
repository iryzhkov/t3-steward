package workerruntime

import (
	"context"
	"errors"
	"io"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ServeOne is the restricted-command worker entrypoint. It accepts exactly one
// bounded protocol envelope on stdin, executes it through the authenticated
// exchange, writes exactly one response, and then exits.
func ServeOne(ctx context.Context, input io.Reader, output io.Writer, codec workerproto.Codec, exchange Exchange) error {
	if input == nil || output == nil {
		return errors.New("worker exchange: input and output are required")
	}
	var request workerproto.Envelope
	if err := codec.Decode(input, &request); err != nil {
		return err
	}
	response, err := exchange.Handle(ctx, request)
	if err != nil {
		return err
	}
	return codec.Encode(output, response)
}
