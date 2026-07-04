# mcpsocat

`mcpsocat` is a specialized, lightweight bridge tool written in Go that connects standard input/output (stdio) to a Model Context Protocol (MCP) server over a UNIX domain socket. 

It acts as a robust replacement for `socat`, specifically designed to handle MCP session lifecycle and automatic reconnections seamlessly.

## Why not just `socat`?

When running an AI agent inside a Guest VM that connects to a Host MCP server via a forwarded UNIX socket using standard `socat`:

```bash
# Typical setup without mcpsocat
socat STDIO UNIX-CONNECT:/path/to/mcp.sock
```

If the Host MCP server restarts or the connection drops, `socat` exits. Even if it reconnects, the MCP protocol requires the client to send an `initialize` request for the server to accept new operations. Standard `socat` is just a byte pipe and doesn't understand this.

**`mcpsocat` solves this by understanding the MCP protocol layer:**
1. It intercepts and caches the initial `initialize` request and `notifications/initialized` from the client.
2. If the socket connection drops, it automatically buffers incoming client messages and continuously tries to reconnect.
3. Upon reconnection, it automatically replays the cached initialization handshake to restore the session state behind the scenes.
4. It safely drops the redundant initialization response from the new server instance so the client agent never gets confused.

## Installation

```bash
go build -o mcpsocat
# or install directly
go install github.com/nizyu/mcpsocat@latest
```

## Usage

Simply replace your `socat` command with `mcpsocat`, passing the path to the UNIX domain socket.

```bash
mcpsocat /path/to/my-mcp-server.sock
```

### In a client configuration (e.g., Claude Desktop, Cursor, Custom Agent)

Configure your MCP client to use `mcpsocat` as the command:

```json
{
  "mcpServers": {
    "my-host-server": {
      "command": "mcpsocat",
      "args": ["/path/to/my-mcp-server.sock"]
    }
  }
}
```

## Features

- **Single Binary**: No dependencies, making it easy to deploy in any environment.
- **Automatic Reconnection**: Exponential backoff for reconnecting to dropped sockets.
- **Transparent Session Recovery**: Caches and replays MCP initialization handshakes.
- **Message Buffering**: Buffers client requests while the server is disconnected instead of crashing the client.
- **Low Footprint**: Minimal memory and CPU usage.

## How it works

1. **Client to Server (`stdin` -> `socket`)**: Reads JSON-RPC messages from `stdin`. If it spots an `initialize` or `notifications/initialized` method, it saves them. All messages are forwarded to the socket. If disconnected, messages are queued.
2. **Server to Client (`socket` -> `stdout`)**: Reads responses from the socket and writes them to `stdout`.
3. **Reconnection**: When the socket breaks, `mcpsocat` attempts to reconnect. Once connected, it fires the saved `initialize` and `initialized` messages automatically. It drops the very first response from the server (which is the reply to the replayed `initialize`) to maintain protocol consistency with the client.
