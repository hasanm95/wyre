package server

import (
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hasanm95/wyre/internal/wire"
)

func TestConnHandler_HandlesSingleCall(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	dispatcher := NewDispatcher()
	dispatcher.Register("Greeter.SayHello", func(payload []byte) ([]byte, error) {
		return []byte("Hello, " + string(payload)), nil
	})

	handler := NewConnHandler(c2, dispatcher)

	serveErr := make(chan error, 1)
	go func() { serveErr <- handler.Serve() }()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1, Type: wire.FrameTypeHeader, Payload: []byte("Greeter.SayHello"),
		}))
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1, Type: wire.FrameTypeData, Payload: []byte("Hasan"),
		}))
	}()

	respFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read response frame: %v", err)
	}
	if respFrame.StreamID != 1 {
		t.Errorf("expected stream ID 1, got %d", respFrame.StreamID)
	}
	if string(respFrame.Payload) != "Hello, Hasan" {
		t.Errorf("expected 'Hello, Hasan', got %q", respFrame.Payload)
	}

	c1.Close()
	<-serveErr
}

func TestConnHandler_ConcurrentCallsGetCorrectResponses(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	dispatcher := NewDispatcher()
	dispatcher.Register("Echo", func(payload []byte) ([]byte, error) {
		return payload, nil
	})

	handler := NewConnHandler(c2, dispatcher)
	go handler.Serve()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{StreamID: 1, Type: wire.FrameTypeHeader, Payload: []byte("Echo")}))
		c1.Write(wire.EncodeFrame(wire.Frame{StreamID: 1, Type: wire.FrameTypeData, Payload: []byte("first")}))
	}()
	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{StreamID: 2, Type: wire.FrameTypeHeader, Payload: []byte("Echo")}))
		c1.Write(wire.EncodeFrame(wire.Frame{StreamID: 2, Type: wire.FrameTypeData, Payload: []byte("second")}))
	}()

	responses := map[uint32]string{}
	for i := 0; i < 2; i++ {
		f, err := wire.ReadFrame(c1)
		if err != nil {
			t.Fatalf("failed to read response frame: %v", err)
		}
		responses[f.StreamID] = string(f.Payload)
	}

	if responses[1] != "first" {
		t.Errorf("expected stream 1 response %q, got %q", "first", responses[1])
	}
	if responses[2] != "second" {
		t.Errorf("expected stream 2 response %q, got %q", "second", responses[2])
	}
}

func TestConnHandler_UnknownMethod_NoResponseSent(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	dispatcher := NewDispatcher() // no methods registered
	handler := NewConnHandler(c2, dispatcher)
	go handler.Serve()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{StreamID: 1, Type: wire.FrameTypeHeader, Payload: []byte("Greeter.Unknown")}))
		c1.Write(wire.EncodeFrame(wire.Frame{StreamID: 1, Type: wire.FrameTypeData, Payload: []byte("hi")}))
	}()

	readDone := make(chan struct{})
	go func() {
		wire.ReadFrame(c1) // documents the known gap: this should never return
		close(readDone)
	}()

	select {
	case <-readDone:
		t.Fatal("expected no response for an unknown method (documented known gap) — one arrived unexpectedly")
	case <-time.After(200 * time.Millisecond):
		// expected: no response ever comes
	}
}

func TestConnHandler_ConnectionClosedBeforeDataFrame(t *testing.T) {
	c1, c2 := net.Pipe()

	dispatcher := NewDispatcher()
	handler := NewConnHandler(c2, dispatcher)

	serveErr := make(chan error, 1)
	go func() { serveErr <- handler.Serve() }()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{StreamID: 1, Type: wire.FrameTypeHeader, Payload: []byte("Greeter.SayHello")}))
	}()

	time.Sleep(20 * time.Millisecond) // let the header get processed and the stream registered
	c1.Close()                        // connection dies before the Data frame ever arrives

	select {
	case err := <-serveErr:
		if err == nil {
			t.Error("expected Serve to return an error when the connection closes")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Serve did not return after connection close — possible goroutine leak/hang")
	}
}

func TestConnHandler_WriteFrame(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	handler := NewConnHandler(c2, NewDispatcher())

	frame := wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeData,
		Payload:  []byte("Hello Hasan"),
	}

	go func() {
		err := handler.writeFrame(frame)

		if err != nil {
			t.Errorf("writeFrame failed: %v", err)
		}
	}()

	got, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read frame: %v", err)
	}

	if diff := cmp.Diff(frame, got); diff != "" {
		t.Errorf("mismatch (-want, +got):\n%s", diff)
	}
}