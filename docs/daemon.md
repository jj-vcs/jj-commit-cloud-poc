# Local Daemon (`jj-cc-daemon`)

Short-lived `jj` CLI commands incur TCP/TLS connection setup latency on every invocation. `jj-cc-daemon` eliminates this overhead by listening on a local Unix Domain Socket (UDS) and multiplexing all client RPCs over a single persistent HTTP/2 channel to the remote `jj-cc-server`.

---

## Daemon Components (`daemon/src/`)

| Component | Role |
| :--- | :--- |
| **`UnixListenerStream`** | Listens on `/tmp/jj-cc-daemon-<user>-<hash>.sock` (where `<hash>` is derived from `server_url`). |
| **`SocketGuard`** | Automatically cleans up the Unix socket file when the daemon process exits or shuts down. |
| **`RemoteConnectionManager`** | Lazily initializes and maintains a single shared HTTP/2 `Channel` to the remote server. |
| **`DaemonBackendProxy`** | Implements `BackendServiceServer`, forwarding object reads/writes over the shared channel. |
| **`DaemonOpStoreProxy`** | Implements `OpStoreServiceServer`, forwarding operation log and op head requests. |
| **`DaemonWorkspaceProxy`** | Implements `WorkspaceServiceServer`, forwarding workspace state queries and updates. |

---

## Auto-Spawning & Fallback Behavior

When `use_daemon = true` in `config.toml`:
1. **Connect via UDS**: The client (`lib/src/client.rs`) attempts to connect to `/tmp/jj-cc-daemon-<user>-<hash>.sock`.
2. **Auto-Spawn on Demand**: If the socket does not exist or refuses connection, `cc-lib` automatically spawns `jj-cc-daemon --server <URL> --socket <PATH>` as a detached background process and polls until the socket is bound (up to 3 seconds).
3. **Graceful TCP Fallback**: If the daemon binary cannot be found or fails to start, the client logs a warning and falls back to connecting directly to `server_url` over TCP HTTP/2.

---

## Daemon CLI Management

```bash
# Start daemon manually
jj cc daemon start --server http://127.0.0.1:8080

# Check daemon status
jj cc daemon status

# Stop running daemon and remove UDS socket
jj cc daemon stop
```
