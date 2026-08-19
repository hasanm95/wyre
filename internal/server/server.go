package server

import (
	"fmt"
	"net"
)

type Server struct {
   dispatcher *Dispatcher
   listener net.Listener
}

func NewServer(dispatcher *Dispatcher) *Server {
    return &Server{
		dispatcher: dispatcher,  
	}
}

func (s *Server) Dispatch(methodName string, request []byte) ([]byte, error) {
	return s.dispatcher.Dispatch(methodName, request)
}

func (s *Server) Listen(address string) error {
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return fmt.Errorf("failed to list address: %v", err)
	}

	s.listener = listener
	return nil
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) Serve() error {
	for {
		conn, err := s.listener.Accept()
		
		if err != nil {
            return err
        }

		go func() {
			handler := NewConnHandler(conn, s.dispatcher)
			handler.Serve()
		}()
	}
}

func (s *Server) Close() error {
    return s.listener.Close()
}