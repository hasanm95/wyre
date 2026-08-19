package client

import (
	"fmt"
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

func (c *Client) Call(method string, request []byte) ([]byte, error) {
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

	select {
	case respFrame, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("connection closed before response arrived")
		}
		return respFrame.Payload, nil
	case <-time.After(callTimeout):
		return nil, fmt.Errorf("call to %q timed out after %s", method, callTimeout)
	}
}

func (c *Client) Closed() <-chan struct{} {
	return c.closed
}