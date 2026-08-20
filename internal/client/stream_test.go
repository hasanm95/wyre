package client

import (
	"context"
	"errors"
	"io"
	"testing"

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