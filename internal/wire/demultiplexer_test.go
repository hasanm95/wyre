package wire

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestDispatch(t *testing.T) {
	streamID := uint32(5)
	stream := NewDemultiplexer()
	ch := stream.Register(streamID)

	frame := Frame{
		StreamID: uint32(streamID),
		Type:     1,
		Payload:  []byte("Hello"),
	}

	data := make(chan Frame)

	go func() {
		data <- <-ch
	}()

	stream.Dispatch(frame)

	extractedData := <-data

	if diff := cmp.Diff(frame, extractedData); diff != "" {
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}
}

func TestDispatch_DoesNotBlockWithoutConcurrentReceiver(t *testing.T) {
	stream := NewDemultiplexer()
	stream.Register(5)

	frame1 := Frame{StreamID: 5, Type: 1, Payload: []byte("hello")}
	frame2 := Frame{StreamID: 5, Type: 1, Payload: []byte("world")}

	stream.Dispatch(frame1)
	stream.Dispatch(frame2)

	registry, err := stream.GetRegistry(5)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	extractedData1 := <-registry
	if diff := cmp.Diff(frame1, extractedData1); diff != "" {
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}

	extractedData2 := <-registry
	if diff := cmp.Diff(frame2, extractedData2); diff != "" {
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}
}

func TestDispatch_DoesNotBlockWhenBufferFull(t *testing.T) {
	stream := NewDemultiplexer()
	stream.Register(5)

	frame1 := Frame{StreamID: 5, Type: 1, Payload: []byte("hello")}
	frame2 := Frame{StreamID: 5, Type: 1, Payload: []byte("world")}
	frame3 := Frame{StreamID: 5, Type: 1, Payload: []byte("what is going on")}

	stream.Dispatch(frame1)
	stream.Dispatch(frame2)

	dispatchDone := make(chan struct{})

	go func() {
		stream.Dispatch(frame3)
		close(dispatchDone)
	}()

	select {
	case <-dispatchDone:
		fmt.Println("Success: Dispatch did not block on a full buffer!")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Dispatch did not return in time — it likely blocked because the channel buffer was full and no one was reading")
	}

	registry, _ := stream.GetRegistry(5)

	f1Out := <-registry
	if string(f1Out.Payload) != "hello" {
		t.Errorf("Expected 'hello', got %s", f1Out.Payload)
	}

	f2Out := <-registry
	if string(f2Out.Payload) != "world" {
		t.Errorf("Expected 'world', got %s", f2Out.Payload)
	}

	select {
	case unexpectedFrame := <-registry:
		t.Errorf("frame3 was NOT dropped! Found payload: %s", string(unexpectedFrame.Payload))
	default:
		fmt.Println("Success Assertion: Confirmed that frame3 was dropped!")
	}
}

func TestUnregister(t *testing.T) {
	stream := NewDemultiplexer()
	ch := stream.Register(5)
	stream.Unregister(5)

	_, err := stream.GetRegistry(5)
	if err == nil {
		t.Errorf("expected error (stream should be deleted from map), got nil")
	}

	_, ok := <-ch
	if ok {
		t.Errorf("expected registry to be removed and closed")
	}
}

func TestShutdown_ClosesAllActiveStreams(t *testing.T) {
	s := NewDemultiplexer()
	ch5 := s.Register(5)
	ch10 := s.Register(10)

	s.Shutdown()

	if _, ok := <-ch5; ok {
		t.Errorf("expected stream 5 channel to be closed")
	}
	if _, ok := <-ch10; ok {
		t.Errorf("expected stream 10 channel to be closed")
	}
}

func TestShutdown_IsIdempotent(t *testing.T) {
	s := NewDemultiplexer()
	s.Register(5)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("server panicked on double shutdown! Error: %v", r)
		}
	}()

	s.Shutdown()
	s.Shutdown()
}

func TestShutdown_RejectsTrafficPostShutdown(t *testing.T) {
	s := NewDemultiplexer()
	ch5 := s.Register(5)

	s.Shutdown()

	newCh := s.Register(99)
	if newCh != nil {
		t.Errorf("expected Register to return nil after shutdown")
	}

	frame := Frame{StreamID: 5, Type: 1, Payload: []byte("post-shutdown data")}
	s.Dispatch(frame)

	select {
	case _, ok := <-ch5:
		if ok {
			t.Errorf("stream accepted a frame after shutdown!")
		}
	default:
	}
}

func TestShutdown_ConcurrentSafety(t *testing.T) {
	s := NewDemultiplexer()
	s.Register(1)

	var wg sync.WaitGroup
	workersCount := 100

	for i := 0; i < workersCount; i++ {
		wg.Add(2)

		go func() {
			defer wg.Done()
			f := Frame{StreamID: 1, Type: 1, Payload: []byte("data")}
			s.Dispatch(f)
		}()

		go func(id int) {
			defer wg.Done()
			_ = s.Register(uint32(id))
		}(i + 10)
	}

	time.Sleep(1 * time.Millisecond)
	s.Shutdown()

	wg.Wait()
}
