package wire

import (
	"fmt"
	"net"
)

type Reader struct {
	conn net.Conn
	demux *Demultiplexer
}

func NewReader(conn net.Conn, demux *Demultiplexer) *Reader{
	return &Reader{
		conn: conn,
		demux: demux,
	}
}

func (r *Reader) Read() error{
	for {
		frame, err := ReadFrame(r.conn)

		if err != nil {
			return fmt.Errorf("failed to read connection frame: %w", err)
		}

		r.demux.Dispatch(frame)
	}
}