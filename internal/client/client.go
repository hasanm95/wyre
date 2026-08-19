package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hasanm95/wyre/internal/wire"
)

type Client struct {
	mu           sync.Mutex
	address      string
	conn         net.Conn
	demux        *wire.Demultiplexer
	nextStreamID atomic.Uint32
	closed       chan struct{}
	closeErr     error
}


func Dial(address string) (*Client, error) {
	c := &Client{address: address}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	demux := wire.NewDemultiplexer()
	closed := make(chan struct{})

	c.mu.Lock()
	c.conn = conn
	c.demux = demux
	c.closed = closed
	c.mu.Unlock()

	reader := wire.NewReader(conn, demux)
	go func() {
		err := reader.Read()
		c.closeErr = err
		close(closed)
		demux.Shutdown()
	}()

	return nil
}


func (c *Client) newStreamID() uint32 {
	return c.nextStreamID.Add(1)
}

const callTimeout = 3 * time.Second

func (c *Client) Call(ctx context.Context, method string, request []byte) ([]byte, error) {
	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}

	c.mu.Lock()
	conn := c.conn
	demux := c.demux
	c.mu.Unlock()

	streamID := c.newStreamID()
	ch := demux.Register(streamID)
	defer demux.Unregister(streamID)

	headerFrame := wire.Frame{StreamID: streamID, Type: wire.FrameTypeHeader, Payload: []byte(method)}
	if _, err := conn.Write(wire.EncodeFrame(headerFrame)); err != nil {
		return nil, fmt.Errorf("failed to write header frame: %w", err)
	}

	dataFrame := wire.Frame{StreamID: streamID, Type: wire.FrameTypeData, Payload: request}
	if _, err := conn.Write(wire.EncodeFrame(dataFrame)); err != nil {
		return nil, fmt.Errorf("failed to write data frame: %w", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	response, err := readCallResponse(callCtx, ch)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("call to %q timed out after %s", method, callTimeout)
		}

		return nil, err
	}

	return response, nil
}

func (c *Client) Closed() <-chan struct{} {
	return c.closed
}

const (
	maxReconnectAttempts = 3
	baseDelay            = 500 * time.Millisecond 
	maxDelay             = 5 * time.Second 
)

func (c *Client) ensureConnected(ctx context.Context) error {
	select {
	case <-c.Closed():
		// dead, fall through to reconnect
	default:
		return nil // still alive, nothing to do
	}

	var err error
	for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
		if err = c.connect(); err == nil {
			return nil
		}

		if attempt == maxReconnectAttempts {
			break
		}

		delay := baseDelay * (1 << uint(attempt-1))

		if delay > maxDelay {
			delay = maxDelay
		}

		jitter := time.Duration(rand.Int63n(int64(delay / 2)))
		totalSleep := delay + jitter

		fmt.Printf("Reconnect attempt %d failed. Retrying in %v...\n", attempt, totalSleep)

		select {
		case <-ctx.Done():
			return fmt.Errorf("reconnection aborted: %w", ctx.Err())
		case <-time.After(totalSleep):
			// Sleep finished, loop continues to the next connection attempt
		}
	}
	return fmt.Errorf("failed to reconnect after %d attempts: %w", maxReconnectAttempts, err)
}