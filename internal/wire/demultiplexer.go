package wire

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

// streamChannel wraps individual channels to protect against write-vs-close races
type streamChannel struct {
	mu       sync.Mutex
	ch       chan Frame
	isClosed bool
}

type Demultiplexer struct {
	mu        sync.Mutex
	registry  map[uint32]*streamChannel
	isClosed  bool
	closeChan chan struct{}
	closeOnce sync.Once
}

const chanSize = 2

var errSystemShutdown = errors.New("demultiplexer is shut down")

func NewDemultiplexer() *Demultiplexer {
	return &Demultiplexer{
		registry:  map[uint32]*streamChannel{},
		closeChan: make(chan struct{}),
	}
}

func (s *Demultiplexer) GetRegistry(streamID uint32) (<-chan Frame, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return nil, errSystemShutdown
	}

	stream, ok := s.registry[streamID]
	if !ok {
		return nil, fmt.Errorf("failed to read registry")
	}
	return stream.ch, nil
}

func (s *Demultiplexer) Register(streamID uint32) <-chan Frame {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return nil
	}

	stream := &streamChannel{
		ch: make(chan Frame, chanSize),
	}
	s.registry[streamID] = stream

	return stream.ch
}

func (s *Demultiplexer) Dispatch(f Frame) {
	select {
	case <-s.closeChan:
		return 
	default:
	}

	s.mu.Lock()
	stream, exists := s.registry[f.StreamID]
	s.mu.Unlock()

	if !exists {
		return
	}

	stream.mu.Lock()
	if stream.isClosed {
		stream.mu.Unlock()
		return
	}

	select {
	case <-s.closeChan:
		stream.mu.Unlock()
		return
	case stream.ch <- f:
		// Success
	default:
		log.Printf("Stream %d buffer is full! Dropping frame.", f.StreamID)
	}
	stream.mu.Unlock()
}

func (s *Demultiplexer) Unregister(streamID uint32) {
	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		return
	}

	stream, ok := s.registry[streamID]
	if !ok {
		s.mu.Unlock()
		return
	}

	delete(s.registry, streamID)
	s.mu.Unlock()

	stream.mu.Lock()
	if !stream.isClosed {
		stream.isClosed = true
		close(stream.ch)
	}
	stream.mu.Unlock()
}

func (s *Demultiplexer) Shutdown() {
	s.closeOnce.Do(func() {
		close(s.closeChan)

		s.mu.Lock()
		s.isClosed = true
		activeStreams := make([]*streamChannel, 0, len(s.registry))
		for streamID, stream := range s.registry {
			activeStreams = append(activeStreams, stream)
			delete(s.registry, streamID)
		}
		s.mu.Unlock()

		for _, stream := range activeStreams {
			stream.mu.Lock()
			if !stream.isClosed {
				stream.isClosed = true
				close(stream.ch)
			}
			stream.mu.Unlock()
		}
		log.Println("System shutdown completed successfully.")
	})
}
