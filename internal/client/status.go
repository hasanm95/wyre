package client

import (
	"github.com/hasanm95/wyre/internal/wire"
)

type StatusError struct {
	Code wire.StatusCode
	Message string
}

func (e *StatusError) Error() string {
	return e.Message
}