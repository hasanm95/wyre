# Wyre

A simplified-but-accurate gRPC clone written in Go, built as a learning project to understand how gRPC actually works under the hood — real Protobuf serialization over a hand-built, stream-multiplexed transport, instead of relying on `protoc-gen-go-grpc` to generate it all for you.

> Working title. Rename freely — same naming pattern as Go-Redis, Dingo, Gonx.

## Why this exists

Plain HTTP has too much overhead for internal service-to-service calls. gRPC solves that with HTTP/2 transport + Protobuf serialization + contract-first design. Rather than build a toy protocol *inspired by* gRPC, this project builds a **smaller, self-made version of gRPC itself** — keeping the core mechanics genuinely accurate. Finishing it means actually understanding how gRPC works, not just knowing that it exists.

## What "accurate" means here vs. real gRPC

| Piece | This project's approach | How close to real gRPC |
|---|---|---|
| Serialization | Real Protobuf — `protoc` + `protobuf-go`, `proto.Marshal`/`proto.Unmarshal` | Identical — this is literally what gRPC uses |
| Contract | Real `.proto` files, real codegen for message types | Identical for the IDL/message part. `protoc-gen-go-grpc` is intentionally skipped — the transport is hand-built |
| Transport framing / multiplexing | Custom stream-ID based frame format (`stream_id` + `frame_type` + `length` + `payload`) over one shared TCP connection | Same idea as HTTP/2 (multiple concurrent calls sharing one connection, frame-interleaved) — not literal HTTP/2 (no HPACK, no flow-control windows, no full h2 state machine) |
| Method dispatch | Service/method registry + routing by name, mirroring grpc-go's handler registration | Same idea, hand-built instead of codegen'd |

## Non-goals

- TLS / encryption
- Real HTTP/2 spec compliance (HPACK, per-stream flow control windows)
- Full h2 state machine
- Load balancing / service discovery

## Architecture

```
              ONE persistent TCP connection
 Client ───────────────────────────────────────▶ Server
   │  Call(ctx, "Greeter.SayHello", req)           │
   │  stream_id=1 ─┐                                │
   │  stream_id=2 ─┼─▶ multiplexed on the same       │
   │  stream_id=3 ─┘   socket, frames interleaved    │
   │                                                │
   │        [ Frame codec (custom) ]                 │
   │        [ Protobuf payload (real) ]              │
   │        [ Method dispatcher ]                    │
```

### Wire frame format

Every frame on the wire has a fixed 9-byte header followed by the payload:

```
[ stream_id: 4 bytes ][ type: 1 byte ][ length: 4 bytes ][ payload: N bytes ]
```

Frame types:

| Type | Meaning |
|---|---|
| `FrameTypeHeader` | Opens a call — payload is the method name (e.g. `"Greeter.SayHello"`) |
| `FrameTypeData` | Carries a request or response payload (may appear multiple times per stream, for streaming RPCs) |
| `FrameTypeStatus` | Terminal status for a unary call or a failed stream — carries a status code + message |
| `FrameTypeEnd` | Marks a successful streaming RPC as complete |

A single TCP connection carries many concurrent logical streams, each identified by `stream_id`. A `Demultiplexer` on both ends reads frames off the connection and routes each one to the goroutine handling that stream, so many RPCs can be in flight on one connection at once without their bytes getting mixed up.

### Status codes

Modeled loosely on gRPC's status codes, kept to a minimal set:

| Code | Meaning |
|---|---|
| `StatusOK` | Call succeeded |
| `StatusNotFound` | Unknown method |
| `StatusInternal` | Handler returned an error |

Unary calls always end with a status frame (`OK` on success, `NotFound`/`Internal` on failure) instead of leaving the caller hanging. Streaming calls end with either `FrameTypeEnd` (success) or a status frame (failure).

## Project layout

```
proto/              greeter.proto + generated message types (HelloRequest, HelloResponse)
internal/wire/       frame encode/decode, status encode/decode, the Demultiplexer, the connection Reader
internal/server/     Dispatcher (unary + streaming method registries), ConnHandler, Server
internal/client/      Client (Dial, Call, Stream), reconnect logic, StatusError
internal/benchmark/   Wyre vs. plain HTTP+JSON benchmark
```

## Features

- **Real Protobuf messages**, generated via `protoc` + `protoc-gen-go` from a normal `.proto` file
- **Custom multiplexed framing** over a single TCP connection — many concurrent calls, no head-of-line blocking across streams
- **Unary RPCs**: `client.Call(ctx, method, request)`
- **Streaming RPCs**: `client.Stream(ctx, method, request)` returning a `*ClientStream` with `Recv()`, terminated by `io.EOF`
- **Status codes**: failed calls surface a typed `*StatusError` with a code and message, instead of an opaque error or a silent hang
- **Automatic reconnect** with exponential backoff + jitter when the connection dies
- **In-flight call failure detection** — a call fails fast with a clear error if the connection dies mid-call, rather than hanging until timeout
- **Context-aware everywhere** — `Call` and `Stream` both take a `context.Context`, and respect cancellation both while connecting and while waiting for a response
- **Concurrency-safe**: many goroutines can call `Call`/`Stream` on the same client concurrently; verified clean under `go test -race`

## Usage

### Server

```go
dispatcher := server.NewDispatcher()

dispatcher.Register("Greeter.SayHello", func(payload []byte) ([]byte, error) {
    req := &pb.HelloRequest{}
    proto.Unmarshal(payload, req)
    resp := &pb.HelloResponse{Message: "Hello, " + req.Name}
    return proto.Marshal(resp)
})

dispatcher.RegisterStream("Greeter.SayHelloStream", func(payload []byte, send func([]byte) error) error {
    for i := 0; i < 3; i++ {
        if err := send([]byte(fmt.Sprintf("chunk-%d", i))); err != nil {
            return err
        }
    }
    return nil
})

srv := server.NewServer(dispatcher)
srv.Listen("127.0.0.1:0")
srv.Serve()
```

### Client — unary call

```go
c, _ := client.Dial(addr)

reqData, _ := proto.Marshal(&pb.HelloRequest{Name: "Hasan"})
respData, err := c.Call(ctx, "Greeter.SayHello", reqData)
if err != nil {
    var statusErr *client.StatusError
    if errors.As(err, &statusErr) {
        // statusErr.Code, statusErr.Message
    }
}
```

### Client — streaming call

```go
stream, err := c.Stream(ctx, "Greeter.SayHelloStream", []byte("request"))
for {
    data, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        // handle error
    }
    // use data
}
```

## Development approach

Built test-first, phase by phase:

0. Project setup
1. Contract-first `.proto` IDL
2. Real Protobuf marshal/unmarshal, verified with round-trip tests
3. Wire frame format — `EncodeFrame`/`DecodeFrame`, with round-trip, partial-read, and malformed-frame test cases
4. Stream demultiplexer — interleaved frames from multiple streams routed correctly, in order
5. Method dispatcher (unary + later, streaming)
6. Server library — accept loop, one demultiplexer + dispatcher per connection
7. Client library — `Dial`, `Call`, correlation via `map[stream_id]chan Frame`
8. Connection lifecycle — reconnect with exponential backoff + jitter, clean failure of in-flight calls on disconnect
9. Concurrency correctness — verified under `go test -race`
10. Status codes and streaming RPCs (client + server), including an end-to-end test that caught and led to fixing a real bug (see below)
11. Benchmark — Wyre vs. plain HTTP+JSON

Every table-driven or interleaved-frame test in the `wire` package uses `net.Pipe()` for fast in-memory testing; real `net.Listen`/`net.Dial` is used for true end-to-end integration tests.

### A bug the tests caught

The end-to-end streaming test (a real client talking to a real server, not fake channels or hand-written frames) exposed a real concurrency bug: `Demultiplexer.Dispatch` used to **silently drop** a frame if a stream's channel buffer was full. Unary calls never hit this, since they only ever send two frames per stream (data + status), which just barely fit the buffer. Streaming calls, which can send many chunks in quick succession, overflowed it — and a dropped `FrameTypeEnd` frame meant `Recv()` would hang forever waiting for a frame that was never coming.

The fix: `Dispatch` now **blocks** until the consumer has room, instead of dropping the frame — real (if simple) backpressure, instead of silently losing data. A test (`TestDispatch_BlocksWhenBufferFull`) locks in the new behavior.

## Benchmark results

Wyre vs. plain HTTP+JSON, same echo-style payload, measured with `go test -bench=. -benchmem`:

| | ns/op | B/op | allocs/op |
|---|---|---|---|
| **Wyre** | 24,298 | 1,084 | 28 |
| **HTTP+JSON** | 31,397 | 7,778 | 83 |

Wyre is roughly **23% faster**, uses **~7x less memory per call**, and does **~3x fewer allocations**. This lines up with expectations: `encoding/json` relies on reflection and is allocation-heavy, and plain HTTP carries text-protocol overhead (headers, status line parsing) that a 9-byte binary frame header + real Protobuf payload avoids entirely.

## Running tests

```
go test ./...                              # full suite
go test -race -count=3 ./internal/client/...  # concurrency correctness
go test -bench=. -benchmem ./internal/benchmark/...  # benchmark
```

## Status

All planned phases (0 through 11) are complete, including both stretch goals (streaming RPCs + status codes, and the benchmark). Not implemented, by design: TLS, real HTTP/2 flow control (HPACK, per-stream windows), load balancing, and service discovery — see Non-goals above.