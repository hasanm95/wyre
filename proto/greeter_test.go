package proto

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)


func TestHeloRequest(t *testing.T) {
	m := HelloRequest{
		Name: "Hasan",
	}

	data, err := proto.Marshal(&m)

	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	p := HelloRequest{}

	err = proto.Unmarshal(data, &p)

	if err != nil {
		t.Fatalf("failed to unmarshal request: %v", err)
	}

	diff := cmp.Diff(&m, &p, protocmp.Transform())
	if diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestHeloResponse(t *testing.T) {
	m := &HelloResponse{
		Message: "Hello",
	}

	data, err := proto.Marshal(m)

	if err != nil {
		t.Fatalf("failed to marshal response: %v", err)
	}

	p := &HelloResponse{}

	err = proto.Unmarshal(data, p)

	if err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	diff := cmp.Diff(&m, &p, protocmp.Transform())
	if diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}