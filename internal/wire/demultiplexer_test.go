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

func TestDemultiplexer_DispatchesInterleavedFramesToCorrectStreams(t *testing.T) {
	s := NewDemultiplexer()
	s.Register(1)
	s.Register(2)

	frameA := Frame{
		StreamID: 1,
		Type: 1,
		Payload: []byte("Frame A"),
	}

	frameB := Frame{
		StreamID: 2,
		Type: 1,
		Payload: []byte("Frame B"),
	}

	frameC := Frame{
		StreamID: 1,
		Type: 1,
		Payload: []byte("Frame C"),
	}

	frameD := Frame{
		StreamID: 2,
		Type: 1,
		Payload: []byte("Frame D"),
	}

	s.Dispatch(frameA)
	s.Dispatch(frameB)
	s.Dispatch(frameC)
	s.Dispatch(frameD)

	stream1FrameChan, _ := s.GetRegistry(1)
	stream2FrameChan, _ := s.GetRegistry(2)

	stream1DataA := <-stream1FrameChan
	stream1DataC := <-stream1FrameChan

	stream2DataB := <-stream2FrameChan
	stream2DataD := <-stream2FrameChan

	if diff := cmp.Diff(frameA, stream1DataA); diff != "" {
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}

	if diff := cmp.Diff(frameC, stream1DataC); diff != "" {
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}

	if diff := cmp.Diff(frameB, stream2DataB); diff != "" {
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}
	if diff := cmp.Diff(frameD, stream2DataD); diff != "" {
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}
}

func TestDemultiplexer_DropsFrameForUnknownStream(t *testing.T){
	s := NewDemultiplexer()
	s.Register(1)
	
	frame := Frame {
		StreamID: 99,
		Type: 1,
		Payload: []byte("Hello"),
	}

	s.Dispatch(frame)

	_, err := s.GetRegistry(99)

	if err == nil {
		t.Errorf("expected error, got nil")
	}

	frameChan, err := s.GetRegistry(1)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	select {
	case unexpectedFrame := <-frameChan:
		t.Errorf("frame was NOT dropped! Found payload: %s", string(unexpectedFrame.Payload))
	default:
	}
}

func TestDemultiplexer_DropsFrameAfterStreamUnregister(t *testing.T) {
	s := NewDemultiplexer()
	stream1 := s.Register(1)
	stream2 := s.Register(2)

	s.Unregister(1)

	frame := Frame {
		StreamID: 1,
		Type: 1,
		Payload: []byte("Hello"),
	}

	s.Dispatch(frame)

	_, err := s.GetRegistry(1)
	if err == nil {
		t.Errorf("expected error (stream should be deleted from map), got nil")
	}

	_, ok := <-stream1
	if ok {
		t.Errorf("expected registry to be removed and closed")
	}

	select{
	case unexpectedFrame := <- stream2:
		t.Errorf("frame was NOT dropped! Found payload: %s", string(unexpectedFrame.Payload))
	default:
	}
}

func TestDemultiplexer_ConcurrentDispatchAndUnregister(t *testing.T) {
	s := NewDemultiplexer()

	stream1 := s.Register(1)
	stream2 := s.Register(2)

	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(2)

	go func() {
		defer wg.Done()

		<-start

		frame := Frame{
			StreamID: 1,
			Type:     1,
			Payload:  []byte("data"),
		}

		s.Dispatch(frame)
	}()

	go func() {
		defer wg.Done()

		<-start

		s.Unregister(1)
	}()

	// Start both goroutines.
	close(start)

	// Wait until both operations have finished.
	wg.Wait()

	// Stream 1 must eventually be closed.
	select {
	case _, ok := <-stream1:
		if ok {
			// Dispatch happened before Unregister, so the
			// frame was buffered. That's a valid outcome.
			t.Log("stream 1 received a frame before it was closed")

			// The channel should now eventually close.
			select {
			case _, ok := <-stream1:
				if ok {
					t.Error("expected stream 1 channel to be closed")
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("stream 1 channel was not closed")
			}
		}

	case <-time.After(100 * time.Millisecond):
		t.Fatal("stream 1 channel did not close in time")
	}

	// Stream 2 must not receive stream 1's frame.
	select {
	case unexpectedFrame := <-stream2:
		t.Errorf(
			"stream 2 received an unexpected frame: %s",
			string(unexpectedFrame.Payload),
		)
	default:
	}
}

func TestDemultiplexer_PreservesFrameOrder(t *testing.T){
	s := NewDemultiplexer()
	stream1 := s.Register(1)

	frameA := Frame{
		StreamID: 1,
		Type: 1,
		Payload: []byte("Frame A"),
	}

	frameB := Frame{
		StreamID: 1,
		Type: 1,
		Payload: []byte("Frame B"),
	}

	s.Dispatch(frameA)
	s.Dispatch(frameB)

	dataA := <- stream1
	dataB := <- stream1

	if diff := cmp.Diff(frameA, dataA); diff != ""{
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}

	if diff := cmp.Diff(frameB, dataB); diff != ""{
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}
}

func TestDemultiplexer_BufferedFramesSurviveUnregister(t *testing.T) {
	s := NewDemultiplexer()
	stream := s.Register(1)

	frameA := Frame{
		StreamID: 1,
		Type: 1,
		Payload: []byte("Frame A"),
	}

	frameB := Frame{
		StreamID: 1,
		Type: 1,
		Payload: []byte("Frame B"),
	}

	s.Dispatch(frameA)
	s.Dispatch(frameB)

	s.Unregister(1)

	dataA := <-stream
	dataB := <-stream

	if diff := cmp.Diff(frameA, dataA); diff != ""{
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}

	if diff := cmp.Diff(frameB, dataB); diff != ""{
		t.Errorf("mismatch (-want, +got)\n%s", diff)
	}

	_, ok := <-stream
	if ok {
		t.Error("expected stream channel to be closed")
	}
}

func TestDemultiplexer_OnUnregistered_InvokedForUnknownStream(t *testing.T) {
	s := NewDemultiplexer()

	var received Frame
	called := make(chan struct{})

	s.OnUnregistered(func(f Frame) {
		received = f
		close(called)
	})

	frame := Frame{
		StreamID: 99,
		Type:     FrameTypeHeader,
		Payload:  []byte("Greeter.SayHello"),
	}

	s.Dispatch(frame)

	select {
	case <-called:
		if diff := cmp.Diff(frame, received); diff != "" {
			t.Errorf("mismatch (-want, +got):\n%s", diff)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("handler was not called in time")
	}
}

func TestDemultiplexer_OnUnregistered_NotCalledForRegisteredStream(t *testing.T) {
	s := NewDemultiplexer()
	stream := s.Register(5)

	called := false
	s.OnUnregistered(func(f Frame) {
		called = true
	})

	frame := Frame{StreamID: 5, Type: FrameTypeData, Payload: []byte("hello")}
	s.Dispatch(frame)

	<-stream 

	if called {
		t.Error("unregistered handler should NOT fire for a registered stream")
	}
}

func TestDemultiplexer_OnUnregistered_HandlerCanRegisterWithoutDeadlock(t *testing.T) {
	s := NewDemultiplexer()

	done := make(chan struct{})

	s.OnUnregistered(func(f Frame) {
		s.Register(f.StreamID)
		close(done)
	})

	frame := Frame{StreamID: 7, Type: FrameTypeHeader, Payload: []byte("Greeter.SayHello")}
	s.Dispatch(frame)

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("handler blocked — likely a deadlock from calling Register() inside the handler")
	}
}