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
	err := c.ensureConnected(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	conn := c.conn
	demux := c.demux
	c.mu.Unlock()

	streamID := c.newStreamID()
	ch := demux.Register(streamID)

	headerFrame := wire.Frame{StreamID: streamID, Type: wire.FrameTypeHeader, Payload: []byte(method)}
	if _, err := conn.Write(wire.EncodeFrame(headerFrame)); err != nil {
		return nil, fmt.Errorf("failed to write header frame: %w", err)
	}

	dataFrame := wire.Frame{StreamID: streamID, Type: wire.FrameTypeData, Payload: request}
	if _, err := conn.Write(wire.EncodeFrame(dataFrame)); err != nil {
		return nil, fmt.Errorf("failed to write data frame: %w", err)
	}

	return &ClientStream{
		StreamID: streamID,
		ch: ch,
		ctx: ctx,
	}, nil
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