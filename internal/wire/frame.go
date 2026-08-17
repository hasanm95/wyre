package wire

import (
	"encoding/binary"
	"fmt"
)


type Frame struct {
	StreamID uint32
	Type uint8
	Payload []byte
}

func EncodeFrame(f Frame) []byte {
	buf := make([]byte, 9)

	binary.BigEndian.PutUint32(buf[0:4], f.StreamID)

	buf[4] = f.Type

	payloadLength := uint32(len(f.Payload))
	binary.BigEndian.PutUint32(buf[5:9], payloadLength)

	finalFrame := append(buf, f.Payload...)

	return finalFrame
}

func DecodeFrame(data []byte) (Frame, error) {
	dataLen := len(data)
	if dataLen < 9 {
		return Frame{}, fmt.Errorf("invalid frame lenght %d", dataLen)
	}
	streamID := binary.BigEndian.Uint32(data[0:4])
	msgType := uint8(data[4])
	payloadLen := binary.BigEndian.Uint32(data[5:9])

	payloadStart := 9
	payloadEnd := payloadStart + int(payloadLen)

	if payloadEnd > len(data) {
		return Frame{}, fmt.Errorf("invalid frame: declared payload length %d exceeds available data %d", payloadLen, len(data)-payloadStart)
	}

	payload := data[payloadStart:payloadEnd]

	return Frame{
		StreamID: streamID,
		Type: msgType,
		Payload: payload,
	}, nil
}