package client

import (
	"context"
	"testing"

	"github.com/hasanm95/wyre/internal/wire"
)

func TestReadCallResponse_DataThenOK(t *testing.T) {
	ctx := context.Background()

	ch := make(chan wire.Frame, 2)

	ch <- wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeData,
		Payload:  []byte("Hello, Hasan"),
	}

	ch <- wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeStatus,
		Payload:  wire.EncodeStatus(wire.StatusOK, ""),
	}

	close(ch)

	got, err := readCallResponse(ctx, ch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if string(got) != "Hello, Hasan" {
		t.Errorf("expected %q, got %q", "Hello, Hasan", got)
	}
}

func TestReadCallResponse_StatusError(t *testing.T) {
	ctx := context.Background()

	ch := make(chan wire.Frame, 1)

	ch <- wire.Frame{
		StreamID: 1,
		Type:     wire.FrameTypeStatus,
		Payload:  wire.EncodeStatus(wire.StatusInternal, "handler failed"),
	}

	close(ch)

	_, err := readCallResponse(ctx, ch)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	statusErr, ok := err.(*StatusError)
	if !ok {
		t.Fatalf("expected *StatusError, got %T", err)
	}

	if statusErr.Code != wire.StatusInternal {
		t.Errorf(
			"expected status %v, got %v",
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
}

func TestReadCallResponse_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan wire.Frame)

	_, err := readCallResponse(ctx, ch)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}