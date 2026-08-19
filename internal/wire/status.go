package wire

import "fmt"

const FrameTypeStatus uint8 = 3

type StatusCode uint8

const (
	StatusOK StatusCode = 0
	StatusNotFound StatusCode = 1
	StatusInternal StatusCode = 2
)

func EncodeStatus(code StatusCode, message string) []byte {
	buf := make([]byte, 1, 1+len(message))
	buf[0] = byte(code)

	return append(buf, message...)
}

func DecodeStatus(data []byte) (StatusCode, string, error) {
	if len(data) < 1 {
		return StatusInternal, "", fmt.Errorf("invalid status: got %d bytes, need %d", len(data), 1)
	}
	code := StatusCode(data[0])

	if code > StatusInternal {
		return StatusInternal, "", fmt.Errorf("unknown status code: %d", code)
	}

	message := string(data[1:])

	return code, message, nil
}