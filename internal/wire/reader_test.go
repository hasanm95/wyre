package wire

import (
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestReader_DispatchesFrameToDemultiplexer(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	demux := NewDemultiplexer()
	stream := demux.Register(1)

	expected := Frame{
		StreamID: 1,
		Type:     1,
		Payload:  []byte("Hello"),
	}

	// Start the reader. It should:
	// 1. Read a frame from c2
	// 2. Decode it
	// 3. Dispatch it to the demultiplexer
	reader := NewReader(c2, demux)

	done := make(chan error, 1)

	go func() {
		done <- reader.Read()
	}()

	// Send the encoded frame through the connection.
	go func() {
		_, err := c1.Write(EncodeFrame(expected))
		if err != nil {
			t.Errorf("failed to write frame: %v", err)
		}
	}()

	// The reader should eventually dispatch the frame to stream 1.
	got := <-stream

	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("mismatch (-want, +got):\n%s", diff)
	}

	// The reader should not have returned an error while processing
	// the frame.
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("reader returned unexpected error: %v", err)
		}
	default:
		// Reader is still running, which is expected because the
		// connection is still open.
	}
}

func TestReader(t *testing.T){
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	demux := NewDemultiplexer()
	stream := demux.Register(1)
	reader := NewReader(c2, demux)
		
	frame := Frame{
		StreamID: 1,
		Type:     1,
		Payload:  []byte("Hello"),
	}

	frameByte := EncodeFrame(frame)

	go reader.Read()
	c1.Write(frameByte)
	

	select {
	case got := <-stream:
		if diff := cmp.Diff(frame, got); diff != "" {
			t.Errorf("mismatch (-want, +got):\n%s", diff)
		}
	case <-time.After(200 * time.Millisecond): 
		t.Fatal("not return on time")
	}
}

func TestReader_ConnClose(t *testing.T) {
	c1, c2 := net.Pipe()

	demux := NewDemultiplexer()
	demux.Register(1)
	reader := NewReader(c2, demux)

	done := make(chan error, 1)

	go func ()  {
		err := reader.Read()
		done <- err
	}()

	c1.Close()

	select {
	case err := <- done:
		if err == nil {
			t.Errorf("reader returned unexpected error: %v", err)
		}
	case <-time.After(200 * time.Millisecond): 
		t.Fatal("not return on time")
	}
}

func TestReader_ReadsMultipleFrames(t *testing.T){
	c1, c2 := net.Pipe()

	demux := NewDemultiplexer()
	stream := demux.Register(1)
	reader := NewReader(c2, demux)

	frameA := Frame{
		StreamID: 1,
		Type:     1,
		Payload:  []byte("Frame A"),
	}

	frameB := Frame{
		StreamID: 1,
		Type:     1,
		Payload:  []byte("Frame B"),
	}

	frameAByte := EncodeFrame(frameA)
	frameBByte := EncodeFrame(frameB)

	go reader.Read()

	c1.Write(frameAByte)
	c1.Write(frameBByte)

	gotA := <- stream
	gotB := <- stream

	if diff := cmp.Diff(frameA, gotA); diff != "" {
		t.Errorf("mismatch A (-want, +got):\n%s", diff)
	}
	if diff := cmp.Diff(frameB, gotB); diff != "" {
		t.Errorf("mismatch B (-want, +got):\n%s", diff)
	}
}

func TestReader_InterleavedFramesToMultipleStreams(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		c1.Close()
		c2.Close()
	})

	demux := NewDemultiplexer()

	stream1 := demux.Register(1)
	stream2 := demux.Register(2)

	reader := NewReader(c2, demux)

	frameA := Frame{
		StreamID: 1,
		Type:     1,
		Payload:  []byte("Frame A"),
	}

	frameB := Frame{
		StreamID: 2,
		Type:     1,
		Payload:  []byte("Frame B"),
	}

	frameC := Frame{
		StreamID: 1,
		Type:     1,
		Payload:  []byte("Frame C"),
	}

	frameD := Frame{
		StreamID: 2,
		Type:     1,
		Payload:  []byte("Frame D"),
	}

	go reader.Read()

	_, err := c1.Write(EncodeFrame(frameA))
	if err != nil {
		t.Fatalf("failed to write frame A: %v", err)
	}

	_, err = c1.Write(EncodeFrame(frameB))
	if err != nil {
		t.Fatalf("failed to write frame B: %v", err)
	}

	_, err = c1.Write(EncodeFrame(frameC))
	if err != nil {
		t.Fatalf("failed to write frame C: %v", err)
	}

	_, err = c1.Write(EncodeFrame(frameD))
	if err != nil {
		t.Fatalf("failed to write frame D: %v", err)
	}

	gotA := <-stream1
	gotC := <-stream1

	gotB := <-stream2
	gotD := <-stream2

	if diff := cmp.Diff(frameA, gotA); diff != "" {
		t.Errorf("stream 1 frame A mismatch (-want, +got):\n%s", diff)
	}

	if diff := cmp.Diff(frameC, gotC); diff != "" {
		t.Errorf("stream 1 frame C mismatch (-want, +got):\n%s", diff)
	}

	if diff := cmp.Diff(frameB, gotB); diff != "" {
		t.Errorf("stream 2 frame B mismatch (-want, +got):\n%s", diff)
	}

	if diff := cmp.Diff(frameD, gotD); diff != "" {
		t.Errorf("stream 2 frame D mismatch (-want, +got):\n%s", diff)
	}
}