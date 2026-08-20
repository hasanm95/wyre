package benchmark

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hasanm95/wyre/internal/client"
	"github.com/hasanm95/wyre/internal/server"
)

func BenchmarkWyreCall(b *testing.B) {
	dispatcher := server.NewDispatcher()
	dispatcher.Register("Greeter.SayHello", func(payload []byte) ([]byte, error) {
		return []byte("Hello, " + string(payload)), nil
	})

	server := server.NewServer(dispatcher)

	if err := server.Listen("127.0.0.1:0"); err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	b.Cleanup(func() { server.Close() })

	go server.Serve()

	c, err := client.Dial(server.Addr().String())
	if err != nil {
		b.Fatalf("failed to dial: %v", err)
	}
	
	b.ResetTimer()

	for i := 0; i < b.N; i++ { 
		c.Call(b.Context(), "Greeter.SayHello", []byte("")) 
	}
}

func BenchmarkHTTPJSONCall(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		if !json.Valid(body) {
			http.Error(w, "invalid JSON payload", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	b.Cleanup(func() { srv.Close() })

	client := &http.Client{}
	payload := []byte(`{"message":"hello stream"}`)
	
	b.ResetTimer() 

	for i := 0; i < b.N; i++ { 
		req, err := http.NewRequestWithContext(b.Context(), "POST", srv.URL, bytes.NewReader(payload))
		if err != nil {
			b.Fatalf("failed to build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("HTTP call failed at iteration %d: %v", i, err)
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}