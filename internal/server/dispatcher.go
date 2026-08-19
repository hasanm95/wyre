package server

import (
	"errors"
	"fmt"
	"sync"
)


type Handler func([]byte) ([]byte, error)

type Dispatcher struct {
	mu sync.Mutex
	methods map[string]Handler
}

var ErrMethodNotFound = errors.New("method not found")

func NewDispatcher() *Dispatcher  {
	return &Dispatcher{
		methods: make(map[string]Handler),
	}
}

func (d *Dispatcher) Register(methodName string, handler Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.methods[methodName] = handler
}

func (d *Dispatcher) Lookup(methodName string) (Handler, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	medthod, ok := d.methods[methodName]

	return medthod, ok
}

func (d *Dispatcher) Dispatch(methodName string, request []byte)([]byte, error) {
	handler, ok := d.Lookup(methodName)

	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrMethodNotFound, methodName)
	}

	response, err := handler(request)

	return response, err
}