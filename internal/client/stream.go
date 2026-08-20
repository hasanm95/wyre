package client

import (
	"context"
	"fmt"
	"io"

	"github.com/hasanm95/wyre/internal/wire"
)

type ClientStream struct {
	StreamID uint32
	ch <-chan wire.Frame
	ctx context.Context
}

func (c *Client) Stream(ctx context.Context, method string, request []byte) (*ClientStream, error) {
	return nil, nil
}

func (c *ClientStream) Recv() ([]byte, error) {

	for {
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		case frame, ok := <-c.ch:
			if !ok {
				return nil, fmt.Errorf("stream closed")
			}

			if frame.Type == wire.FrameTypeEnd {
				return nil, io.EOF
			}

			if frame.Type == wire.FrameTypeStatus {
				code, msg, err := wire.DecodeStatus(frame.Payload)

				if err != nil {
					return nil, err
				}

				if code != wire.StatusOK {
					statusErr := &StatusError {
						Code: code,
						Message: msg,
					}
					return nil, statusErr
				}
			}

			return frame.Payload, nil
		}
	}
}