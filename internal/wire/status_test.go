package wire

import (
	"testing"
)

func TestEncodeDecodeStatus(t *testing.T) {
	tests := []struct {
		name    string
		code    StatusCode
		message string
	}{
		{name: "ok, empty message", code: StatusOK, message: ""},
		{name: "not found", code: StatusNotFound, message: "method not found"},
		{name: "internal error", code: StatusInternal, message: "handler panicked"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeStatus(tt.code, tt.message)

			gotCode, gotMessage, err := DecodeStatus(data)
			if err != nil {
				t.Fatalf("failed to decode status: %v", err)
			}

			if gotCode != tt.code {
				t.Errorf("expected code %v, got %v", tt.code, gotCode)
			}

			if gotMessage != tt.message {
				t.Errorf("expected message %q, got %q", tt.message, gotMessage)
			}
		})
	}
}

func TestDecodeStatus_EmptyPayload(t *testing.T) {
	_, _, err := DecodeStatus([]byte{})
	if err == nil {
		t.Fatal("expected an error for empty payload, got nil")
	}
}

func TestDecodeStatus_UnknownCode(t *testing.T) {
	data := []byte{99}

	_, _, err := DecodeStatus(data)
	if err == nil {
		t.Fatal("expected an error for unknown status code, got nil")
	}
}
