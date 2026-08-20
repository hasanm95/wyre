package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/hasanm95/wyre/internal/wire"
)

type ConnHandler struct {
	conn       net.Conn
	demux      *wire.Demultiplexer
	dispatcher *Dispatcher
	writeMu    sync.Mutex
}

func NewConnHandler(conn net.Conn, dispatcher *Dispatcher) *ConnHandler {
	return &ConnHandler{
		conn:       conn,
		demux:      wire.NewDemultiplexer(),
		dispatcher: dispatcher,
	}
}

func (h *ConnHandler) Serve() error {
	h.demux.OnUnregistered(h.handleNewStream)

	reader := wire.NewReader(h.conn, h.demux)
	err := reader.Read()

	h.demux.Shutdown()

	return err
}

func (h *ConnHandler) handleNewStream(f wire.Frame) {
	if f.Type != wire.FrameTypeHeader {
		return
	}

	methodName := string(f.Payload)
	ch := h.demux.Register(f.StreamID)

	if streamHandler, ok := h.dispatcher.LookupStream(methodName); ok {
		go h.runStreamCall(f.StreamID, streamHandler, ch)
		return
	}

	go h.runCall(f.StreamID, methodName, ch)
}

func (h *ConnHandler) runCall(streamID uint32, methodName string, ch <-chan wire.Frame) {
	defer h.demux.Unregister(streamID)

	dataFrame, ok := <-ch
	if !ok {
		return
	}
	if dataFrame.Type != wire.FrameTypeData {
		return
	}

	response, err := h.dispatcher.Dispatch(methodName, dataFrame.Payload)
	if err != nil {
		log.Printf("dispatch error for stream %d, method %q: %v", streamID, methodName, err)

		statusCode := wire.StatusInternal
		if errors.Is(err, ErrMethodNotFound) {
			statusCode = wire.StatusNotFound
		}

		statusFrame := wire.Frame {
			StreamID: streamID,
			Type: wire.FrameTypeStatus,
			Payload: wire.EncodeStatus(statusCode, err.Error()),
		}
		if err := h.writeFrame(statusFrame); err != nil {
			log.Printf("failed to write status for stream %d: %v", streamID, err)
		}
		return
	}

	if err := h.writeFrame(wire.Frame{
		StreamID: streamID,
		Type:     wire.FrameTypeData,
		Payload:  response,
	}); err != nil {
		log.Printf("failed to write response for stream %d: %v", streamID, err)
	}

	if err := h.writeFrame(wire.Frame{
		StreamID: streamID,
		Type:     wire.FrameTypeStatus,
		Payload:  wire.EncodeStatus(wire.StatusOK, ""),
	}); err != nil {
		log.Printf("failed to write status frame for stream %d: %v", streamID, err)
	}
}

func (h *ConnHandler) runStreamCall(streamID uint32, handler StreamHandler, ch <-chan wire.Frame) {
	defer h.demux.Unregister(streamID)

	dataFrame, ok := <-ch
	if !ok {
		return
	}
	if dataFrame.Type != wire.FrameTypeData {
		return
	}

   send := func(payload []byte) error {
       return h.writeFrame(wire.Frame{
           StreamID: streamID,
           Type:     wire.FrameTypeData,
           Payload:  payload,
       })
   }

   err := handler(dataFrame.Payload, send)

   if err != nil {
		statusCode := wire.StatusInternal
		if errors.Is(err, ErrMethodNotFound) {
			statusCode = wire.StatusNotFound
		}

		statusFrame := wire.Frame {
			StreamID: streamID,
			Type: wire.FrameTypeStatus,
			Payload: wire.EncodeStatus(statusCode, err.Error()),
		}
		if err := h.writeFrame(statusFrame); err != nil {
			log.Printf("failed to write status for stream %d: %v", streamID, err)
		}
		return
   }

   	if err := h.writeFrame(wire.Frame{
		StreamID: streamID,
		Type:     wire.FrameTypeEnd,
		Payload:  []byte(""),
	}); err != nil {
		log.Printf("failed to write response for stream %d: %v", streamID, err)
	}
}

func (h *ConnHandler) writeFrame(f wire.Frame) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	if _, err := h.conn.Write(wire.EncodeFrame(f)); err != nil {
		return fmt.Errorf("failed to write frame: %w", err)
	}
	return nil
}