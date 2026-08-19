package client

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hasanm95/wyre/internal/server"
)

func TestDial_ConnectsSuccessfully(t *testing.T) {
	dispatcher := server.NewDispatcher()
	srv := server.NewServer(dispatcher)

	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	go srv.Serve()

	c, err := Dial(srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	if c == nil {
		t.Fatal("expected a non-nil client")
	}
}

func TestClient_NewStreamID_ReturnsUniqueIncreasingValues(t *testing.T) {
	c := &Client{}

	id1 := c.newStreamID()
	id2 := c.newStreamID()

	if id1 == id2 {
		t.Errorf("expected unique stream IDs, got %d twice", id1)
	}
	if id2 <= id1 {
		t.Errorf("expected increasing values, got %d then %d", id1, id2)
	}
}

func TestClient_Call_ReturnsServerResponse(t *testing.T) {
	dispatcher := server.NewDispatcher()
	dispatcher.Register("Greeter.SayHello", func(payload []byte) ([]byte, error) {
		return []byte("Hello, " + string(payload)), nil
	})

	srv := server.NewServer(dispatcher)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	go srv.Serve()

	c, err := Dial(srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	resp, err := c.Call("Greeter.SayHello", []byte("Hasan"))
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}

	if string(resp) != "Hello, Hasan" {
		t.Errorf("expected 'Hello, Hasan', got %q", resp)
	}
}

func TestClient_Call_ConcurrentCallsGetCorrectResponses(t *testing.T) {
	dispatcher := server.NewDispatcher()
	dispatcher.Register("Echo", func(payload []byte) ([]byte, error) {
		return payload, nil
	})

	srv := server.NewServer(dispatcher)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	go srv.Serve()

	c, err := Dial(srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			req := fmt.Sprintf("req-%d", i)
			resp, err := c.Call("Echo", []byte(req))
			if err != nil {
				t.Errorf("call %d failed: %v", i, err)
				return
			}
			if string(resp) != req {
				t.Errorf("call %d: expected %q, got %q", i, req, resp)
			}
		}(i)
	}

	wg.Wait()
}

func TestClient_Call_TimesOutWhenNoResponseArrives(t *testing.T) {
	dispatcher := server.NewDispatcher()
	dispatcher.Register("Slow", func(payload []byte) ([]byte, error) {
		select {} // never returns, simulates a handler that hangs forever
	})

	srv := server.NewServer(dispatcher)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	go srv.Serve()

	c, err := Dial(srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	_, err = c.Call("Slow", []byte("hi"))
	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
}

func TestClient_DetectsConnectionClose(t *testing.T) {
	dispatcher := server.NewDispatcher()
	srv := server.NewServer(dispatcher)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	go srv.Serve()

	c, err := Dial(srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	c.conn.Close() // simulate the connection dying

	select {
	case <-c.Closed():
		// expected
	case <-time.After(time.Second):
		t.Fatal("expected Closed() channel to close after connection dies")
	}
}

func TestClient_Reconnect_RestoresWorkingConnection(t *testing.T) {
	dispatcher := server.NewDispatcher()
	dispatcher.Register("Echo", func(payload []byte) ([]byte, error) {
		return payload, nil
	})

	srv := server.NewServer(dispatcher)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	go srv.Serve()

	c, err := Dial(srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	c.conn.Close() // kill the connection
	<-c.Closed()   // wait for the client to notice

	if err := c.connect(); err != nil {
		t.Fatalf("failed to reconnect: %v", err)
	}

	resp, err := c.Call("Echo", []byte("hello again"))
	if err != nil {
		t.Fatalf("call after reconnect failed: %v", err)
	}
	if string(resp) != "hello again" {
		t.Errorf("expected %q, got %q", "hello again", resp)
	}
}
