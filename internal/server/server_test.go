package server

import (
	"testing"

	"github.com/google/go-cmp/cmp"
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