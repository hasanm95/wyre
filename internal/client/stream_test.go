package client

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hasanm95/wyre/internal/wire"
)

func TestClientStream_Recv_ReturnsData(t *testing.T){
	ch := make(chan wire.Frame, 1)
	frame := wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeData,
		Payload:  []byte("hello"),
	}

	ch <- frame

	cs := ClientStream {
		StreamID: 1,
		ch: ch,
		ctx: t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	data, err := cs.Recv()

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if string(data) != "hello" {
		t.Errorf("expected %q, got %q", "hello", data)
	}
}

func TestClientStream_Recv_ReturnsEOFOnEnd(t *testing.T) {
	ch := make(chan wire.Frame, 1)

	ch <- wire.Frame{
		StreamID: 1,
		Type: wire.FrameTypeEnd,
	}

	cs := ClientStream {
		StreamID: 1,
		ch: ch,
		ctx: t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	data, err := cs.Recv()

	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}

	if data != nil {
		t.Errorf("expected nil data, got %q", data)
	}
}

func TestClientStream_Recv_ReturnsStatusError(t *testing.T) {
	ch := make(chan wire.Frame, 1)

	ch <- wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeStatus,
		Payload:  wire.EncodeStatus(wire.StatusInternal, "handler failed"),
	}

	cs := ClientStream{
		StreamID: 1,
		ch:       ch,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	data, err := cs.Recv()

	if err == nil {
		t.Fatal("expected status error, got nil")
	}

	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected *StatusError, got %T: %v", err, err)
	}

	if statusErr.Code != wire.StatusInternal {
		t.Errorf(
			"expected code %d, got %d",
			wire.StatusInternal,
			statusErr.Code,
		)
	}

	if statusErr.Message != "handler failed" {
		t.Errorf(
			"expected message %q, got %q",
			"handler failed",
			statusErr.Message,
		)
	}

	if data != nil {
		t.Errorf("expected data to be nil, got %q", data)
	}
}

func TestClientStream_Recv_MultipleDataThenEOF(t *testing.T) {
	ch := make(chan wire.Frame, 3)

	ch <- wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeData,
		Payload:  []byte("first"),
	}
	ch <- wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeData,
		Payload:  []byte("second"),
	}
	ch <- wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeEnd,
	}

	cs := ClientStream{
		StreamID: 1,
		ch:       ch,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	data, err := cs.Recv()
	if err != nil {
		t.Fatalf("first Recv: expected no error, got %v", err)
	}
	if string(data) != "first" {
		t.Errorf("first Recv: expected %q, got %q", "first", data)
	}

	data, err = cs.Recv()
	if err != nil {
		t.Fatalf("second Recv: expected no error, got %v", err)
	}
	if string(data) != "second" {
		t.Errorf("second Recv: expected %q, got %q", "second", data)
	}

	data, err = cs.Recv()
	if err != io.EOF {
		t.Errorf("third Recv: expected io.EOF, got %v", err)
	}
	if data != nil {
		t.Errorf("third Recv: expected nil data, got %q", data)
	}
}

func TestClientStream_Recv_ClosedChannel(t *testing.T) {
	ch := make(chan wire.Frame, 1)
	close(ch)
	cs := ClientStream{
		StreamID: 1,
		ch:       ch,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	data, err := cs.Recv()
	if err == nil {
		t.Errorf("expected close connection error, got nil")
	}

	if data != nil {
		t.Errorf("expected no data, got %q", data)
	}
}

func TestClientStream_Recv_InvalidStatus(t *testing.T) {
	ch := make(chan wire.Frame, 1)

	ch <- wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeStatus,
		Payload:  []byte{},
	}

	cs := ClientStream{
		StreamID: 1,
		ch:       ch,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	data, err := cs.Recv()

	if err == nil {
		t.Fatal("expected error for invalid status, got nil")
	}

	if data != nil {
		t.Errorf("expected nil data, got %q", data)
	}
}

func TestClientStream_Recv_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch := make(chan wire.Frame)

	cs := ClientStream{
		StreamID: 1,
		ch:       ch,
		ctx:      ctx,
		demux: wire.NewDemultiplexer(),
	}

	cancel()

	data, err := cs.Recv()

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	if data != nil {
		t.Errorf("expected nil data, got %q", data)
	}
}

func TestClient_Stream_SendsHeaderAndData(t *testing.T) {
	c1, c2 := net.Pipe()

	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	demux := wire.NewDemultiplexer()
	closed := make(chan struct{})

	c := &Client{
		conn:   c1,
		demux:  demux,
		closed: closed,
	}

	type streamResult struct {
		stream *ClientStream
		err    error
	}

	result := make(chan streamResult, 1)

	go func() {
		stream, err := c.Stream(
			t.Context(),
			"Echo",
			[]byte("hello"),
		)
		result <- streamResult{stream, err}
	}()

	header, err := wire.ReadFrame(c2)
	if err != nil {
		t.Fatalf("failed to read header frame: %v", err)
	}

	if header.Type != wire.FrameTypeHeader {
		t.Errorf("expected FrameTypeHeader, got %d", header.Type)
	}

	if string(header.Payload) != "Echo" {
		t.Errorf("expected method %q, got %q", "Echo", header.Payload)
	}

	data, err := wire.ReadFrame(c2)
	if err != nil {
		t.Fatalf("failed to read data frame: %v", err)
	}

	if data.Type != wire.FrameTypeData {
		t.Errorf("expected FrameTypeData, got %d", data.Type)
	}

	if string(data.Payload) != "hello" {
		t.Errorf("expected payload %q, got %q", "hello", data.Payload)
	}

	if header.StreamID != data.StreamID {
		t.Errorf(
			"expected same stream ID, got header=%d, data=%d",
			header.StreamID,
			data.StreamID,
		)
	}

	select {
	case result := <-result:
		if result.err != nil {
			t.Fatalf("Stream failed: %v", result.err)
		}

		if result.stream == nil {
			t.Fatal("expected non-nil ClientStream")
		}

		if result.stream.StreamID != header.StreamID {
			t.Errorf(
				"expected stream ID %d, got %d",
				header.StreamID,
				result.stream.StreamID,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("Stream did not return")
	}
}

func TestClient_Stream_WriteFailure_UnregistersStream(t *testing.T) {
	c1, c2 := net.Pipe()

	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	demux := wire.NewDemultiplexer()
	closed := make(chan struct{})

	c := &Client{
		conn:   c1,
		demux:  demux,
		closed: closed,
	}

	// Force the next write to fail.
	c1.Close()

	stream, err := c.Stream(
		t.Context(),
		"Echo",
		[]byte("payload"),
	)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if stream != nil {
		t.Fatalf("expected nil stream, got %+v", stream)
	}

	// The first stream ID should be 1.
	_, err = demux.GetRegistry(1)
	if err == nil {
		t.Error("expected stream to be unregistered after write failure")
	}
}

func TestClient_Stream_RemainsRegistered(t *testing.T) {
	c1, c2 := net.Pipe()

	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	demux := wire.NewDemultiplexer()
	closed := make(chan struct{})

	c := &Client{
		conn:   c1,
		demux:  demux,
		closed: closed,
	}

	type streamResult struct {
		stream *ClientStream
		err    error
	}

	resultCh := make(chan streamResult, 1)

	go func() {
		stream, err := c.Stream(
			t.Context(),
			"Echo",
			[]byte("payload"),
		)

		resultCh <- streamResult{
			stream: stream,
			err:    err,
		}
	}()

	// Read the HEADER frame.
	header, err := wire.ReadFrame(c2)
	if err != nil {
		t.Fatalf("failed to read header frame: %v", err)
	}

	if header.Type != wire.FrameTypeHeader {
		t.Errorf("expected header frame, got %d", header.Type)
	}

	// Read the DATA frame.
	data, err := wire.ReadFrame(c2)
	if err != nil {
		t.Fatalf("failed to read data frame: %v", err)
	}

	if data.Type != wire.FrameTypeData {
		t.Errorf("expected data frame, got %d", data.Type)
	}

	// Wait for Stream() to finish.
	result := <-resultCh

	if result.err != nil {
		t.Fatalf("Stream failed: %v", result.err)
	}

	if result.stream == nil {
		t.Fatal("expected non-nil stream")
	}

	// The stream must still be registered after Stream() returns.
	ch, err := demux.GetRegistry(result.stream.StreamID)
	if err != nil {
		t.Fatalf("expected stream to remain registered: %v", err)
	}

	if ch == nil {
		t.Fatal("expected registered stream channel, got nil")
	}
}

func TestClient_Stream_RecvReceivesDispatchedData(t *testing.T) {
	demux := wire.NewDemultiplexer()
	closed := make(chan struct{})

	c := &Client{
		address: "unused",
		demux:   demux,
		closed:  closed,
	}

	streamID := c.newStreamID()
	ch := demux.Register(streamID)

	stream := &ClientStream{
		StreamID: streamID,
		ch:       ch,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	frame := wire.Frame{
		StreamID: stream.StreamID,
		Type:     wire.FrameTypeData,
		Payload:  []byte("frame data"),
	}

	c.demux.Dispatch(frame)

	data, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}

	if string(data) != "frame data" {
		t.Errorf("expected %q, got %q", "frame data", data)
	}
}

func TestClient_Stream_RecvEndReturnsEOF(t *testing.T) {
	demux := wire.NewDemultiplexer()
	closed := make(chan struct{})

	c := &Client{
		address: "unused",
		demux:   demux,
		closed:  closed,
	}

	streamID := c.newStreamID()
	ch := demux.Register(streamID)

	stream := &ClientStream{
		StreamID: streamID,
		ch:       ch,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	frame := wire.Frame {
		StreamID: streamID,
		Type: wire.FrameTypeEnd,
		Payload: []byte(""),
	}

	demux.Dispatch(frame)

	data, err := stream.Recv()

	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}

	if data != nil {
		t.Errorf("expected nil data, got %q", data)
	}
}

func TestClientStream_RecvMultipleFrames(t *testing.T) {
	demux := wire.NewDemultiplexer()
	streamID := uint32(1)

	ch := demux.Register(streamID)

	stream := &ClientStream{
		StreamID: streamID,
		ch:       ch,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	demux.Dispatch(wire.Frame{
		StreamID: streamID,
		Type:     wire.FrameTypeData,
		Payload:  []byte("hello"),
	})

	data, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv: expected no error, got %v", err)
	}

	if string(data) != "hello" {
		t.Errorf("first Recv: expected %q, got %q", "hello", data)
	}

	demux.Dispatch(wire.Frame{
		StreamID: streamID,
		Type:     wire.FrameTypeData,
		Payload:  []byte("world"),
	})

	data, err = stream.Recv()
	if err != nil {
		t.Fatalf("second Recv: expected no error, got %v", err)
	}

	if string(data) != "world" {
		t.Errorf("second Recv: expected %q, got %q", "world", data)
	}

	demux.Dispatch(wire.Frame{
		StreamID: streamID,
		Type:     wire.FrameTypeEnd,
	})

	data, err = stream.Recv()
	if err != io.EOF {
		t.Errorf("third Recv: expected io.EOF, got %v", err)
	}

	if data != nil {
		t.Errorf("third Recv: expected nil data, got %q", data)
	}
}


func TestClientStream_MultipleStreamsDoNotMixFrames(t *testing.T) {
	demux := wire.NewDemultiplexer()

	streamID1 := uint32(1)
	streamID2 := uint32(2)

	ch1 := demux.Register(streamID1)
	ch2 := demux.Register(streamID2)

	stream1 := &ClientStream{
		StreamID: streamID1,
		ch:       ch1,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	stream2 := &ClientStream{
		StreamID: streamID2,
		ch:       ch2,
		ctx:      t.Context(),
		demux: wire.NewDemultiplexer(),
	}

	demux.Dispatch(wire.Frame{
		StreamID: streamID2,
		Type:     wire.FrameTypeData,
		Payload:  []byte("stream two"),
	})

	demux.Dispatch(wire.Frame{
		StreamID: streamID1,
		Type:     wire.FrameTypeData,
		Payload:  []byte("stream one"),
	})

	data, err := stream1.Recv()
	if err != nil {
		t.Fatalf("stream1 Recv failed: %v", err)
	}

	if string(data) != "stream one" {
		t.Errorf("stream1 expected %q, got %q", "stream one", data)
	}

	data, err = stream2.Recv()
	if err != nil {
		t.Fatalf("stream2 Recv failed: %v", err)
	}

	if string(data) != "stream two" {
		t.Errorf("stream2 expected %q, got %q", "stream two", data)
	}
}

func TestClient_Stream_CancelledContext(t *testing.T) {
	c1, c2 := net.Pipe()

	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	demux := wire.NewDemultiplexer()
	closed := make(chan struct{})

	c := &Client{
		conn:   c1,
		demux:  demux,
		closed: closed,
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	stream, err := c.Stream(
		ctx,
		"Echo",
		[]byte("payload"),
	)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if stream != nil {
		t.Fatalf("expected nil stream, got %+v", stream)
	}
}

func TestClientStream_EndUnregistersStream(t *testing.T) {
	demux := wire.NewDemultiplexer()
	streamID := uint32(1)

	ch := demux.Register(streamID)

	stream := &ClientStream{
		StreamID: streamID,
		ch:       ch,
		ctx:      t.Context(),
		demux: 	demux,
	}

	demux.Dispatch(wire.Frame{
		StreamID: streamID,
		Type:     wire.FrameTypeEnd,
	})

	data, err := stream.Recv()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	if data != nil {
		t.Fatalf("expected nil data, got %q", data)
	}

	t.Logf("GetRegistry returned err=%v", err)

	if err == nil {
		t.Errorf("expected stream to be unregistered after END")
	}
}