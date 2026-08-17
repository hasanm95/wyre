package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	headerSize = 9
	maxPayloadLen = 10 * 1024 * 1024 // 10MB
)

type Frame struct {
	StreamID uint32
	Type     uint8
	Payload  []byte
}

// encodeHeader builds the fixed 9-byte header for a frame.
func encodeHeader(f Frame) []byte {
	buf := make([]byte, headerSize)
	binary.BigEndian.PutUint32(buf[0:4], f.StreamID)
	buf[4] = f.Type
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(f.Payload)))
	return buf
}

// decodeHeader parses a header that has ALREADY been confirmed to be
// exactly headerSize bytes. Callers are responsible for that guarantee.
func decodeHeader(header []byte) (streamID uint32, msgType uint8, payloadLen uint32) {
	streamID = binary.BigEndian.Uint32(header[0:4])
	msgType = header[4]
	payloadLen = binary.BigEndian.Uint32(header[5:9])
	return
}

func EncodeFrame(f Frame) []byte {
	header := encodeHeader(f)
	return append(header, f.Payload...)
}

// DecodeFrame parses a frame from a byte slice you already hold in full.
func DecodeFrame(data []byte) (Frame, error) {
	if len(data) < headerSize {
		return Frame{}, fmt.Errorf("invalid frame: header too short, got %d bytes, need %d", len(data), headerSize)
	}

	streamID, msgType, payloadLen := decodeHeader(data[:headerSize])

	if payloadLen > maxPayloadLen {
		return Frame{}, fmt.Errorf("invalid frame: declared payload length %d exceeds max allowed %d", payloadLen, maxPayloadLen)
	}

	payloadStart := headerSize
	payloadEnd := payloadStart + int(payloadLen)

	if payloadEnd > len(data) {
		return Frame{}, fmt.Errorf("invalid frame: declared payload length %d exceeds available data %d", payloadLen, len(data)-payloadStart)
	}

	return Frame{
		StreamID: streamID,
		Type:     msgType,
		Payload:  data[payloadStart:payloadEnd],
	}, nil
}

// ReadFrame reads exactly one frame off a live stream (e.g. a net.Conn).
func ReadFrame(r io.Reader) (Frame, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Frame{}, fmt.Errorf("failed to read frame header: %w", err)
	}

	streamID, msgType, payloadLen := decodeHeader(header)

	// Reject BEFORE allocating anything sized by an untrusted number.
	if payloadLen > maxPayloadLen {
		return Frame{}, fmt.Errorf("invalid frame: declared payload length %d exceeds max allowed %d", payloadLen, maxPayloadLen)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, fmt.Errorf("failed to read frame payload: %w", err)
	}

	return Frame{
		StreamID: streamID,
		Type:     msgType,
		Payload:  payload,
	}, nil
}