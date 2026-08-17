package wire

import (
	"bytes"
	"testing"

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