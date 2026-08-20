package wire

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestEncodeDecodeFrame(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{
			name: "normal payload",
			frame: Frame{
				StreamID: 1,
				Type:     2,
				Payload:  []byte("hello"),
			},
		},
		{
			name: "empty payload",
			frame: Frame{
				StreamID: 42,
				Type:     1,
				Payload:  []byte{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeFrame(tt.frame)

			decoded, err := DecodeFrame(data)
			if err != nil {
				t.Fatalf("failed to decode frame: %v", err)
			}

			if diff := cmp.Diff(tt.frame, decoded); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDecodeFrame_HeaderTooShort(t *testing.T) {
	data := []byte{0, 0, 0, 1, 2}

	_, err := DecodeFrame(data)
	if err == nil {
		t.Fatal("expected an error for a too-short header, got nil")
	}
}


func TestDecodeFrame_TruncatedPayload(t *testing.T) {
	frame := Frame{
		StreamID: 1,
		Type:     2,
		Payload:  []byte("hello world"),
	}

	data := EncodeFrame(frame)
	truncated := data[:len(data)-3]

	_, err := DecodeFrame(truncated)
	if err == nil {
		t.Fatal("expected an error for truncated payload, got nil")
	}
}

func TestFrame(t *testing.T) {
	frame := Frame{
		StreamID: 1,
		Type:     2,
		Payload:  []byte("hello"),
	}

	encodedData := EncodeFrame(frame)

	encodedDataReader := bytes.NewReader(encodedData)
	framedData, err := ReadFrame(encodedDataReader)

	if err != nil {
		t.Fatalf("failed to read frame: %v", err)
	}

	if diff := cmp.Diff(framedData, frame); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestFrame_Sequential(t *testing.T){
	frameA := Frame{
		StreamID: 1,
		Type:     2,
		Payload:  []byte("hello"),
	}

	frameB := Frame{
		StreamID: 42,
		Type:     1,
		Payload:  []byte("hello world"),
	}

	frameAEncodedData := EncodeFrame(frameA)
	frameBEncodedData := EncodeFrame(frameB)

	enodedData := append(frameAEncodedData, frameBEncodedData...)
	encodedDataReader := bytes.NewReader(enodedData)

	frameAData, err := ReadFrame(encodedDataReader)

	if err != nil {
		t.Errorf("failed to read frame A: %v", err)
	}

	frameBData, err := ReadFrame(encodedDataReader)

	if err != nil {
		t.Errorf("failed to read frame B: %v", err)
	}

	if diff := cmp.Diff(frameA, frameAData); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}

	if diff := cmp.Diff(frameB, frameBData); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestFrame_HeaderTooShort(t *testing.T) {
	data := []byte{0, 0, 0, 1, 2}

	dataReader := bytes.NewReader(data)

	_, err := ReadFrame(dataReader)

	if err == nil {
		t.Fatal("expected unexpected EOF error, got nil")
	}
}

func TestReadFrame_RejectsOversizedPayloadBeforeReading(t *testing.T) {
	pr, pw := io.Pipe()
	t.Cleanup(func() {
		pr.Close()
		pw.Close()
	})

	// Hand-build a header that LIES about payload size — claims more than
	// maxPayloadLen, without ever actually sending that much data.
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[0:4], 1)          
	header[4] = 1                                       
	binary.BigEndian.PutUint32(header[5:9], maxPayloadLen+1) // Length — a lie

	go func() {
		pw.Write(header)
		// Deliberately never write payload bytes. If ReadFrame tries to
		// read them, this goroutine just sits here forever — and so would
		// the read on the other end, if the size check weren't in the
		// right place.
	}()

	type result struct {
		frame Frame
		err   error
	}
	done := make(chan result, 1)

	go func() {
		f, err := ReadFrame(pr)
		done <- result{f, err}
	}()

	select {
	case res := <-done:
		if res.err == nil {
			t.Fatal("expected an error for oversized payload length, got nil")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ReadFrame did not return in time — it likely blocked waiting for payload bytes that were never sent")
	}
}

func TestFrame_TypeConstants(t *testing.T) {
	tests := []struct {
		name string
		typ  uint8
	}{
		{name: "header frame", typ: FrameTypeHeader},
		{name: "data frame", typ: FrameTypeData},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := Frame{
				StreamID: 1,
				Type:     tt.typ,
				Payload:  []byte("Greeter.SayHello"),
			}

			data := EncodeFrame(frame)

			decoded, err := DecodeFrame(data)
			if err != nil {
				t.Fatalf("failed to decode frame: %v", err)
			}

			if diff := cmp.Diff(frame, decoded); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEncodeDecodeFrame_End(t *testing.T) {
	frame := Frame{
		StreamID: 1,
		Type:     FrameTypeEnd,
		Payload:  []byte(""),
	}
	frameEncodedData := EncodeFrame(frame)
	decodedFrame, err := DecodeFrame(frameEncodedData)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if decodedFrame.StreamID != 1 {
		t.Errorf("expected stream id 1, got %d", decodedFrame.StreamID)
	}

	if decodedFrame.Type != FrameTypeEnd {
		t.Errorf("expected type %d, got %d", FrameTypeEnd, decodedFrame.Type)
	}
}