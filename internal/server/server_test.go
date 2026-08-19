package server

import (
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hasanm95/wyre/internal/wire"
)

func TestServer_DispatchesRequestToHandler(t *testing.T) {
	dispatcher := NewDispatcher()
	server := NewServer(dispatcher)

	request := []byte("Hello")

	expectedResponse := []byte("Hello Hasan")

	handler := func(payload []byte) ([]byte, error) {
		return expectedResponse, nil
	}

	dispatcher.Register("Greeter.SayHello", handler)

	got, err := server.Dispatch("Greeter.SayHello", request)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if diff := cmp.Diff(expectedResponse, got); diff != "" {
		t.Errorf("mismatch (-want, +got):\n%s", diff)
	}
}

func TestServer_DispatchUnknownMethod(t *testing.T) {
	dispatcher := NewDispatcher()
	server := NewServer(dispatcher)

	_, err := server.Dispatch(
		"Greeter.Unknown",
		[]byte("Hello"),
	)

	if err == nil {
		t.Fatal("expected error for unknown method, got nil")
	}
}

func TestServer_Listen(t *testing.T) {
	dispatcher := NewDispatcher()
	server := NewServer(dispatcher)

	expectedHost := "127.0.0.1"
	err := server.Listen(expectedHost + ":0")
	t.Cleanup(func() { server.Close() })

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	addr := server.Addr()
	host, _, _ := net.SplitHostPort(addr.String())

	if host != expectedHost {
		t.Errorf("expected address %s, got %s", expectedHost, host)
	}
}

func TestServer_Serve(t *testing.T) {
	dispatcher := NewDispatcher()
	dispatcher.Register("Greeter.SayHello", func(payload []byte) ([]byte, error) {
		return []byte("Hello, " + string(payload)), nil
	})

	server := NewServer(dispatcher)

	if err := server.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() { server.Close() })

	go server.Serve()

	conn, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	conn.Write(wire.EncodeFrame(wire.Frame{
		StreamID: 1, Type: wire.FrameTypeHeader, Payload: []byte("Greeter.SayHello"),
	}))
	conn.Write(wire.EncodeFrame(wire.Frame{
		StreamID: 1, Type: wire.FrameTypeData, Payload: []byte("Hasan"),
	}))

	type result struct {
		frame wire.Frame
		err   error
	}
	done := make(chan result, 1)

	go func() {
		f, err := wire.ReadFrame(conn)
		done <- result{f, err}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("failed to read response frame: %v", res.err)
		}
		if res.frame.StreamID != 1 {
			t.Errorf("expected stream ID 1, got %d", res.frame.StreamID)
		}
		if string(res.frame.Payload) != "Hello, Hasan" {
			t.Errorf("expected 'Hello, Hasan', got %q", res.frame.Payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive response in time — likely a bug somewhere in the Listen/Serve/ConnHandler chain")
	}
}