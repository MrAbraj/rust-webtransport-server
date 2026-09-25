# Rust WebTransport Server

A performance-oriented, low-latency WebTransport server written in Rust for real-time collaborative applications.

This server powers the real-time communication layer for the companion React/TypeScript collaborative canvas.

## Companion Client

The easiest way to understand how this server is consumed is to look at the client repository:

**Client:** [realtime-collaborative-canvas](https://github.com/MrAbraj/realtime-collaborative-canvas)

The client demonstrates:

* Establishing a WebTransport connection
* Joining a collaborative room
* Receiving the initial room snapshot
* Receiving real-time room events
* Sending canvas element updates
* Sending and receiving cursor updates
* Using MessagePack for the communication protocol

**Server:** [rust-webtransport-server](https://github.com/MrAbraj/rust-webtransport-server)

Together, the two repositories demonstrate the complete real-time flow:

```text
React / TypeScript Client
        │
        │ WebTransport + MessagePack
        ▼
Rust WebTransport Server
        │
        ├── Room state
        ├── Element updates
        ├── User events
        └── Cursor datagrams
```

## Features

* WebTransport over QUIC
* Async networking with Tokio
* MessagePack binary serialization
* Per-room state management
* Real-time element updates
* Real-time cursor updates using datagrams
* User join/leave events
* Initial room state synchronization
* Concurrent room access using DashMap
* Separate processing paths for reliable events and cursor updates

## Architecture

Each collaborative canvas is organized into a **room**.

When a client joins a room:

1. The server creates the room if it does not already exist.
2. The client receives the current canvas snapshot.
3. Existing room members are sent to the client.
4. Other clients receive a `UserJoined` event.
5. Element changes are broadcast to the room.
6. Cursor movements are sent separately using WebTransport datagrams.

The server keeps room state in memory and processes room commands asynchronously.

### Room Architecture

Each room has:

* Canvas element state
* Connected members
* Reliable event channel
* Cursor datagram channel
* A dedicated room actor
* A dedicated cursor actor

The room registry uses `DashMap` so different rooms can be accessed concurrently without requiring one global lock.

## Message Types

### Element

Canvas elements contain an ID, version and additional properties.

```rust
pub struct Element {
    pub id: String,
    pub version: u32,

    #[serde(flatten)]
    pub extra_properties: HashMap<String, rmpv::Value>,
}
```

Additional properties are kept flexible so the server does not need to know every canvas element field.

### CursorUpdate

Cursor movements contain:

```rust
pub struct CursorUpdate {
    pub user_id: String,
    pub x: f32,
    pub y: f32,
}
```

Cursor updates are treated differently from reliable room events because cursor movement is high-frequency and the newest position is generally more useful than retransmitting every intermediate position.

### RoomSnapshot

When a user joins, the server sends the current room state:

```rust
pub struct RoomSnapshot {
    pub user_id: String,
    pub elements: Vec<Element>,
}
```

### Room Events

The server broadcasts events such as:

* `UserJoined`
* `UserLeft`
* `ElementUpdated`

## Reliable vs. Unreliable Data

The server uses different WebTransport mechanisms depending on the type of data.

### Reliable streams

Used for data where ordering and delivery matter:

* Initial room snapshot
* User join/leave events
* Canvas element updates

### Datagrams

Used for high-frequency cursor updates.

This allows cursor traffic to remain lightweight without forcing every cursor position through the reliable event path.

## Serialization

The application uses **MessagePack** instead of JSON for the real-time protocol.

The Rust server uses:

* `serde`
* `rmp-serde`
* `rmpv`

MessagePack keeps the wire format compact while allowing application data structures to be serialized/deserialized directly.

## Concurrency

The server is built around Tokio's asynchronous runtime.

Each room gets its own asynchronous actor that processes commands through a Tokio channel.

This keeps room state owned by the room actor instead of requiring multiple tasks to directly mutate the same state.

The room registry uses:

```text
DashMap<String, RoomHandle>
```

and creates room actors on demand.

This allows independent rooms to progress concurrently.

## Performance-oriented Design

The server is designed for low-latency real-time communication.

Key design choices include:

* Rust for efficient server-side execution
* Tokio for asynchronous I/O
* WebTransport/QUIC for multiplexed communication
* MessagePack for compact binary serialization
* DashMap for concurrent room lookup
* In-memory room state for fast access
* Dedicated room actors for isolated state management
* Separate cursor processing from reliable room events
* Tokio channels for asynchronous message passing
* WebTransport datagrams for high-frequency cursor updates

The implementation is **performance-oriented**, but benchmark results are not currently included, so no specific latency or throughput numbers are claimed.

## Running Locally

```bash
cargo run
```

The companion client can then connect to the local WebTransport endpoint.

## Related Project

**Client:** [realtime-collaborative-canvas](https://github.com/MrAbraj/realtime-collaborative-canvas)

**Server:** [rust-webtransport-server](https://github.com/MrAbraj/rust-webtransport-server)

The client repository is useful for understanding the actual protocol consumption and how the Rust server integrates with a real React/TypeScript application.
