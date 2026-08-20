package server

import (
	"errors"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDispatcher_RegisterAndLookup(t *testing.T){
	dispatcher := NewDispatcher()

	var called = false

	dispatcher.Register("Greeter.SayHello", func(b []byte) ([]byte, error) {
		called = true
		return b, nil
	})
	got, ok := dispatcher.Lookup("Greeter.SayHello")

	if !ok {
		t.Fatal("expected handler to be found")
	}

	got([]byte("Hello"))

	if called == false {
		t.Errorf("expected handler to be called")
	}
}

func TestDispatcher_LookupUnknownMethod(t *testing.T) {
	dispatcher := NewDispatcher()
	_, ok := dispatcher.Lookup("Greeter.SayHello")

	if ok {
		t.Error("expected method to not be found")
	}
}


func TestDispatcher_RegisterDuplicateMethod(t *testing.T) {
	dispatcher := NewDispatcher()

	var called string

	dispatcher.Register("Greeter.SayHello", func(b []byte) ([]byte, error) {
		called = "handlerA"
		return b, nil
	})
	dispatcher.Register("Greeter.SayHello", func(b []byte) ([]byte, error) {
		called = "handlerB"
		return b, nil
	})
	got, ok := dispatcher.Lookup("Greeter.SayHello")

	if !ok {
		t.Fatal("expected handler to be found")
	}

	got([]byte("Hello"))

	if called != "handlerB" {
		t.Errorf("expected handlerB to be called, got %q", called)
	}
}

func TestDispatcher_ConcurrentRegisterAndLookup(t *testing.T) {
	dispatcher := NewDispatcher()

	var wg sync.WaitGroup
	start := make(chan struct{})

	workerCount := 100

	wg.Add(2 * workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()

			<-start

			dispatcher.Register("Greeter.SayHello", func(b []byte) ([]byte, error) {
				return b, nil
			})
		}()

		go func() {
			defer wg.Done()

			<-start

			dispatcher.Lookup("Greeter.SayHello")
		}()
	}

	close(start)

	wg.Wait()
}

func TestDispatcher_HandlerReceivesRequest(t *testing.T) {
	dispatcher := NewDispatcher()
	request := []byte("Hello Hasan")
	var received []byte
	
	handler := func(payload []byte) ([]byte, error) {
		received = payload
		return nil, nil
	}

	dispatcher.Register("Greeter.SayHello", handler)
	_, err := dispatcher.Dispatch("Greeter.SayHello", request)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if diff := cmp.Diff(request, received); diff != "" {
		t.Errorf("mismatch (-want, +got):\n%s", diff)
	}
}

func TestDispatcher_DispatchUnknownMethod(t *testing.T) {
	dispatcher := NewDispatcher()

	request := []byte("Hello Hasan")

	_, err := dispatcher.Dispatch("Greeter.Unknown", request)

	if err == nil {
		t.Fatal("expected error for unknown method, got nil")
	}
}

func TestDispatcher_ReturnsHandlerResponse(t *testing.T) {
	dispatcher := NewDispatcher()

	request := []byte("Hello")
	expectedResponse := []byte("Hello Hasan")

	handler := func(payload []byte) ([]byte, error) {
		return expectedResponse, nil
	}

	dispatcher.Register("Greeter.SayHello", handler)

	got, err := dispatcher.Dispatch("Greeter.SayHello", request)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if diff := cmp.Diff(expectedResponse, got); diff != "" {
		t.Errorf("mismatch (-want, +got):\n%s", diff)
	}
}

func TestDispatcher_PropagatesHandlerError(t *testing.T) {
	dispatcher := NewDispatcher()

	expectedErr := errors.New("something went wrong")

	handler := func(payload []byte) ([]byte, error) {
		return nil, expectedErr
	}

	dispatcher.Register("Greeter.SayHello", handler)

	_, err := dispatcher.Dispatch("Greeter.SayHello", []byte("Hello"))

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestDispatcher_RegisterAndLookupStream(t *testing.T) {
	dispatcher := NewDispatcher()

	dispatcher.RegisterStream("Greeter.SayHelloStream", func(payload []byte, send func([]byte) error) error {
		return send([]byte("chunk"))
	})

	got, ok := dispatcher.LookupStream("Greeter.SayHelloStream")
	if !ok {
		t.Fatal("expected stream handler to be found")
	}

	var received []byte
	fakeSend := func(b []byte) error {
		received = b
		return nil
	}

	err := got([]byte("request"), fakeSend)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if string(received) != "chunk" {
		t.Errorf("expected send to receive %q, got %q", "chunk", received)
	}
}

func TestDispatcher_LookupStreamUnknownMethod(t *testing.T) {
	dispatcher := NewDispatcher()

	_, ok := dispatcher.LookupStream("Greeter.Unknown")
	if ok {
		t.Error("expected stream method to not be found")
	}
}

func TestDispatcher_UnaryAndStreamRegistriesAreIndependent(t *testing.T) {
	dispatcher := NewDispatcher()

	dispatcher.Register("Greeter.SayHello", func(b []byte) ([]byte, error) {
		return []byte("unary"), nil
	})

	dispatcher.RegisterStream("Greeter.SayHello", func(payload []byte, send func([]byte) error) error {
		return send([]byte("stream"))
	})

	unaryHandler, ok := dispatcher.Lookup("Greeter.SayHello")
	if !ok {
		t.Fatal("expected unary handler to be found")
	}

	unaryResp, err := unaryHandler([]byte("req"))
	if err != nil {
		t.Fatalf("unary handler failed: %v", err)
	}
	if string(unaryResp) != "unary" {
		t.Errorf("expected %q, got %q", "unary", unaryResp)
	}

	streamHandler, ok := dispatcher.LookupStream("Greeter.SayHello")
	if !ok {
		t.Fatal("expected stream handler to be found")
	}

	var received []byte
	err = streamHandler([]byte("req"), func(b []byte) error {
		received = b
		return nil
	})
	if err != nil {
		t.Fatalf("stream handler failed: %v", err)
	}
	if string(received) != "stream" {
		t.Errorf("expected %q, got %q", "stream", received)
	}
}