package server

import (
	"fmt"
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
	go func() {
		serveErr <- handler.Serve()
	}()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1,
			Type:     wire.FrameTypeHeader,
			Payload:  []byte("Greeter.SayHello"),
		}))

		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1,
			Type:     wire.FrameTypeData,
			Payload:  []byte("Hasan"),
		}))
	}()

	respFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read response frame: %v", err)
	}

	if respFrame.StreamID != 1 {
		t.Errorf("expected stream ID 1, got %d", respFrame.StreamID)
	}

	if respFrame.Type != wire.FrameTypeData {
		t.Errorf("expected FrameTypeData, got %d", respFrame.Type)
	}

	if string(respFrame.Payload) != "Hello, Hasan" {
		t.Errorf("expected 'Hello, Hasan', got %q", respFrame.Payload)
	}

	statusFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read status frame: %v", err)
	}

	if statusFrame.StreamID != 1 {
		t.Errorf("expected status stream ID 1, got %d", statusFrame.StreamID)
	}

	if statusFrame.Type != wire.FrameTypeStatus {
		t.Errorf("expected FrameTypeStatus, got %d", statusFrame.Type)
	}

	code, message, err := wire.DecodeStatus(statusFrame.Payload)
	if err != nil {
		t.Fatalf("failed to decode status: %v", err)
	}

	if code != wire.StatusOK {
		t.Errorf("expected status %v, got %v", wire.StatusOK, code)
	}

	if message != "" {
		t.Errorf("expected empty status message, got %q", message)
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
		dataFrame, err := wire.ReadFrame(c1)
		if err != nil {
			t.Fatalf("failed to read response frame: %v", err)
		}

		if dataFrame.Type != wire.FrameTypeData {
			t.Fatalf("expected FrameTypeData, got %d", dataFrame.Type)
		}

		responses[dataFrame.StreamID] = string(dataFrame.Payload)

		statusFrame, err := wire.ReadFrame(c1)
		if err != nil {
			t.Fatalf("failed to read status frame: %v", err)
		}

		if statusFrame.StreamID != dataFrame.StreamID {
			t.Errorf(
				"expected status stream ID %d, got %d",
				dataFrame.StreamID,
				statusFrame.StreamID,
			)
		}

		if statusFrame.Type != wire.FrameTypeStatus {
			t.Errorf("expected FrameTypeStatus, got %d", statusFrame.Type)
		}

		code, message, err := wire.DecodeStatus(statusFrame.Payload)
		if err != nil {
			t.Fatalf("failed to decode status: %v", err)
		}

		if code != wire.StatusOK {
			t.Errorf(
				"expected status %v for stream %d, got %v",
				wire.StatusOK,
				statusFrame.StreamID,
				code,
			)
		}

		if message != "" {
			t.Errorf(
				"expected empty status message for stream %d, got %q",
				statusFrame.StreamID,
				message,
			)
		}
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

	respFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read response frame: %v", err)
	}

	if respFrame.StreamID != 1 {
		t.Errorf("expected stream ID 1, got %d", respFrame.StreamID)
	}

	if respFrame.Type != wire.FrameTypeStatus {
		t.Errorf("expected status frame, got %d", respFrame.Type)
	}

	code, message, err := wire.DecodeStatus(respFrame.Payload)

	if err != nil {
		t.Fatalf("failed to decode status: %v", err)
	}

	if code != wire.StatusNotFound {
		t.Errorf("expected status %v, got %v", wire.StatusNotFound, code)
	}

	if message == "" {
		t.Error("expected a non-empty status message")
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

func TestConnHandler_SuccessfulCall_SendsOKStatus(t *testing.T) {
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
	go handler.Serve()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1,
			Type:     wire.FrameTypeHeader,
			Payload:  []byte("Greeter.SayHello"),
		}))
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1,
			Type:     wire.FrameTypeData,
			Payload:  []byte("Hasan"),
		}))
	}()

	respFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read response frame: %v", err)
	}

	if respFrame.StreamID != 1 {
		t.Errorf("expected stream ID 1, got %d", respFrame.StreamID)
	}

	if respFrame.Type != wire.FrameTypeData {
		t.Errorf("expected FrameTypeData, got %d", respFrame.Type)
	}

	if string(respFrame.Payload) != "Hello, Hasan" {
		t.Errorf("expected 'Hello, Hasan', got %q", respFrame.Payload)
	}

	statusFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read status frame: %v", err)
	}

	if statusFrame.StreamID != 1 {
		t.Errorf("expected status stream ID 1, got %d", statusFrame.StreamID)
	}

	if statusFrame.Type != wire.FrameTypeStatus {
		t.Errorf("expected FrameTypeStatus, got %d", statusFrame.Type)
	}

	code, message, err := wire.DecodeStatus(statusFrame.Payload)
	if err != nil {
		t.Fatalf("failed to decode status: %v", err)
	}

	if code != wire.StatusOK {
		t.Errorf("expected status %v, got %v", wire.StatusOK, code)
	}

	if message != "" {
		t.Errorf("expected empty status message, got %q", message)
	}
}

func TestConnHandler_HandlerError_SendsInternalStatus(t *testing.T) {
	c1, c2 := net.Pipe()

	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	dispatcher := NewDispatcher()

	expectedError := "handler failed"

	dispatcher.Register("Greeter.SayHello", func(payload []byte) ([]byte, error) {
		return nil, fmt.Errorf("%s", expectedError)
	})

	handler := NewConnHandler(c2, dispatcher)
	go handler.Serve()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1,
			Type:     wire.FrameTypeHeader,
			Payload:  []byte("Greeter.SayHello"),
		}))

		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1,
			Type:     wire.FrameTypeData,
			Payload:  []byte("Hasan"),
		}))
	}()

	respFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read response frame: %v", err)
	}

	if respFrame.StreamID != 1 {
		t.Errorf("expected stream ID 1, got %d", respFrame.StreamID)
	}

	if respFrame.Type != wire.FrameTypeStatus {
		t.Errorf("expected FrameTypeStatus, got %d", respFrame.Type)
	}

	code, message, err := wire.DecodeStatus(respFrame.Payload)
	if err != nil {
		t.Fatalf("failed to decode status: %v", err)
	}

	if code != wire.StatusInternal {
		t.Errorf("expected status %v, got %v", wire.StatusInternal, code)
	}

	if message != expectedError {
		t.Errorf("expected message %q, got %q", expectedError, message)
	}
}

func TestConnHandler_HandlesStreamingCall(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	dispatcher := NewDispatcher()
	dispatcher.RegisterStream("Greeter.SayHelloStream", func(payload []byte, send func([]byte) error) error {
		if err := send([]byte("chunk1")); err != nil {
			return err
		}
		if err := send([]byte("chunk2")); err != nil {
			return err
		}
		return nil
	})

	handler := NewConnHandler(c2, dispatcher)

	serveErr := make(chan error, 1)
	go func() { serveErr <- handler.Serve() }()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1, Type: wire.FrameTypeHeader, Payload: []byte("Greeter.SayHelloStream"),
		}))
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1, Type: wire.FrameTypeData, Payload: []byte("request"),
		}))
	}()

	frame1, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read first chunk: %v", err)
	}
	if frame1.Type != wire.FrameTypeData {
		t.Errorf("expected FrameTypeData, got %d", frame1.Type)
	}
	if string(frame1.Payload) != "chunk1" {
		t.Errorf("expected %q, got %q", "chunk1", frame1.Payload)
	}

	frame2, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read second chunk: %v", err)
	}
	if frame2.Type != wire.FrameTypeData {
		t.Errorf("expected FrameTypeData, got %d", frame2.Type)
	}
	if string(frame2.Payload) != "chunk2" {
		t.Errorf("expected %q, got %q", "chunk2", frame2.Payload)
	}

	endFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read end frame: %v", err)
	}
	if endFrame.Type != wire.FrameTypeEnd {
		t.Errorf("expected FrameTypeEnd, got %d", endFrame.Type)
	}
	if endFrame.StreamID != 1 {
		t.Errorf("expected stream ID 1, got %d", endFrame.StreamID)
	}

	c1.Close()
	<-serveErr
}

func TestConnHandler_StreamingCall_HandlerError_SendsInternalStatus(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	dispatcher := NewDispatcher()
	expectedErr := "stream handler failed"

	dispatcher.RegisterStream("Greeter.SayHelloStream", func(payload []byte, send func([]byte) error) error {
		if err := send([]byte("chunk1")); err != nil {
			return err
		}
		return fmt.Errorf("%s", expectedErr)
	})

	handler := NewConnHandler(c2, dispatcher)
	go handler.Serve()

	go func() {
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1, Type: wire.FrameTypeHeader, Payload: []byte("Greeter.SayHelloStream"),
		}))
		c1.Write(wire.EncodeFrame(wire.Frame{
			StreamID: 1, Type: wire.FrameTypeData, Payload: []byte("request"),
		}))
	}()

	chunkFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read chunk: %v", err)
	}
	if string(chunkFrame.Payload) != "chunk1" {
		t.Errorf("expected %q, got %q", "chunk1", chunkFrame.Payload)
	}

	statusFrame, err := wire.ReadFrame(c1)
	if err != nil {
		t.Fatalf("failed to read status frame: %v", err)
	}
	if statusFrame.Type != wire.FrameTypeStatus {
		t.Errorf("expected FrameTypeStatus, got %d", statusFrame.Type)
	}

	code, message, err := wire.DecodeStatus(statusFrame.Payload)
	if err != nil {
		t.Fatalf("failed to decode status: %v", err)
	}
	if code != wire.StatusInternal {
		t.Errorf("expected %v, got %v", wire.StatusInternal, code)
	}
	if message != expectedErr {
		t.Errorf("expected %q, got %q", expectedErr, message)
	}
}