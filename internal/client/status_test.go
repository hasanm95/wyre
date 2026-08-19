package client

import (
	"testing"

	"github.com/hasanm95/wyre/internal/wire"
)

func TestStatusError_Error(t *testing.T) {
	err := &StatusError{
		Code:    wire.StatusNotFound,
		Message: "method not found",
	}

	if err.Error() != "method not found" {
		t.Errorf("expected %q, got %q", "method not found", err.Error())
	}
}