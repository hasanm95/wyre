package server

type Server struct {
   dispatcher *Dispatcher
}

func NewServer(dispatcher *Dispatcher) *Server {
    return &Server{
		dispatcher: dispatcher,
	}
}

func (s *Server) Dispatch(methodName string, request []byte) ([]byte, error) {
	return s.dispatcher.Dispatch(methodName, request)
}