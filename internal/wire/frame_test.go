package wire

import (
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