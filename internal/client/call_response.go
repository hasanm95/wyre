package client

import (
	"context"
	"fmt"

	"github.com/hasanm95/wyre/internal/wire"
)

func readCallResponse(ctx context.Context, ch <-chan wire.Frame) ([]byte, error) {
	var response []byte

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case frame, ok := <-ch:
			if !ok {
				return nil, fmt.Errorf("connection closed before response completed")
			}

			switch frame.Type {
			case wire.FrameTypeData:
				response = frame.Payload

			case wire.FrameTypeStatus:
				code, message, err := wire.DecodeStatus(frame.Payload)
				if err != nil {
					return nil, fmt.Errorf("failed to decode status: %w", err)
				}

				if code != wire.StatusOK {
					return nil, &StatusError{
						Code:    code,
						Message: message,
					}
				}

				return response, nil

			default:
				return nil, fmt.Errorf("unexpected frame type: %d", frame.Type)
			}
		}
	}
}