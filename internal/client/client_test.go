package client

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hasanm95/wyre/internal/server"
	wyreproto "github.com/hasanm95/wyre/proto"
	"google.golang.org/protobuf/proto"
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

	requestData, err := proto.Marshal(&wyreproto.HelloRequest{
		Name: "Hasan",
	})
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	dispatcher.Register("Greeter.SayHello", func(payload []byte) ([]byte, error) {
		request := &wyreproto.HelloRequest{}
		if err := proto.Unmarshal(payload, request); err != nil {
			return nil, err
		}

		response := &wyreproto.HelloResponse{
			Message: "Hello, " + request.Name,
		}

		return proto.Marshal(response)
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

	resp, err := c.Call(t.Context(), "Greeter.SayHello", requestData)
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}

	response := &wyreproto.HelloResponse{}

	if err := proto.Unmarshal(resp, response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if response.Message != "Hello, Hasan" {
		t.Errorf("expected %q, got %q", "Hello, Hasan", response.Message)
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
			resp, err := c.Call(t.Context(), "Echo", []byte(req))
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

	_, err = c.Call(t.Context(), "Slow", []byte("hi"))
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

	resp, err := c.Call(t.Context(), "Echo", []byte("hello again"))
	if err != nil {
		t.Fatalf("call after reconnect failed: %v", err)
	}
	if string(resp) != "hello again" {
		t.Errorf("expected %q, got %q", "hello again", resp)
	}
}

func TestClient_Call_AutomaticallyReconnectsAfterConnectionDies(t *testing.T) {
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

	c.conn.Close()
	<-c.Closed()

	resp, err := c.Call(t.Context(), "Echo", []byte("automatic"))
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
	if string(resp) != "automatic" {
		t.Errorf("expected %q, got %q", "automatic", resp)
	}
}

func TestClient_Call_FailsCleanlyWhenServerDiesMidCall(t *testing.T) {
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

	type callResult struct {
		data []byte
		err error 
	}

	done := make(chan callResult, 1)

	go func ()  {
		data, err := c.Call(t.Context(), "Slow", []byte("hi"))
		done <- callResult{data, err}
	}()

	time.Sleep(20 * time.Millisecond)

	c.conn.Close()

	select {
	case res := <- done:
		if res.err == nil {
			t.Errorf("expected an error, got nil (response: %v)", res.data)
		}
	case <-time.After(200 * time.Millisecond):
		t.Errorf("expected fast failure, but call was still hanging")
	}
}

func TestClient_EnsureConnected_BacksOffBetweenFailedAttempts(t *testing.T) {
	srv := server.NewServer(server.NewDispatcher())
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	addr := srv.Addr().String()
	srv.Close() // nobody is listening on addr now

	c := &Client{address: addr}
	closed := make(chan struct{})
	close(closed)

	c.mu.Lock()
	c.closed = closed
	c.mu.Unlock()

	start := time.Now()
	err := c.ensureConnected(t.Context())
	elapsed := time.Since(start)

	if err == nil {
		t.Errorf("expected an error since nobody is listening on %s, got nil", addr)
	}

	if elapsed <= 140*time.Millisecond {
		t.Errorf("expected reconnect to back off for at least ~150ms across attempts, only took %v", elapsed)
	}
}