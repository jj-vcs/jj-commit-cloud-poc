# Local Daemon (`jj-cc-daemon`)

Short-lived `jj` CLI commands incur TCP/TLS connection setup latency on every invocation. `jj-cc-daemon` eliminates this overhead by listening on a local Unix Domain Socket (UDS) and multiplexing all client RPCs over a single persistent HTTP/2 channel.

---

## Without Daemon vs. With Daemon

```mermaid
flowchart LR
    subgraph Direct Mode ["use_daemon = false (New TCP handshake per command)"]
        CLI1["jj log"] -- "TCP + TLS Handshake" --> S1["jj-cc-server"]
        CLI2["jj status"] -- "TCP + TLS Handshake" --> S1
        CLI3["jj new"] -- "TCP + TLS Handshake" --> S1
    end

    subgraph Daemon Mode ["use_daemon = true (Instant Local IPC + Warm Connection Pool)"]
        D_CLI1["jj log"] -- "Local UDS" --> Daemon["jj-cc-daemon"]
        D_CLI2["jj status"] -- "Local UDS" --> Daemon
        D_CLI3["jj new"] -- "Local UDS" --> Daemon
        Daemon == "Single Warm HTTP/2 Channel" ==> S2["jj-cc-server"]
    end
```

---

## Daemon Proxy Architecture

```mermaid
classDiagram
    class UnixListenerStream {
        +/tmp/jj-cc-daemon-USER-HASH.sock
    }
    class RemoteConnectionManager {
        -server_url: String
        -channel: RwLock~Option~Channel~~
        +get_backend_client()
        +get_op_store_client()
        +get_workspace_client()
    }
    class DaemonBackendProxy {
        +ReadCommit() / WriteCommit()
        +ReadTree() / WriteTree()
        +ReadFile() / WriteFile()
    }
    class DaemonOpStoreProxy {
        +ReadOperation() / WriteOperation()
        +GetOpHeads() / UpdateOpHeads()
        +ReconcileOpHeads()
    }
    class DaemonWorkspaceProxy {
        +GetWorkspace() / UpdateWorkspace()
    }

    UnixListenerStream --> DaemonBackendProxy
    UnixListenerStream --> DaemonOpStoreProxy
    UnixListenerStream --> DaemonWorkspaceProxy
    DaemonBackendProxy --> RemoteConnectionManager
    DaemonOpStoreProxy --> RemoteConnectionManager
    DaemonWorkspaceProxy --> RemoteConnectionManager
```

---

## Auto-Spawn & Connection Fallback Flow

When `use_daemon = true`, `cc-lib` automatically starts `jj-cc-daemon` on demand:

```mermaid
sequenceDiagram
    participant CLI as jj CLI (cc-lib)
    participant FS as /tmp Socket File
    participant Daemon as jj-cc-daemon Process
    participant Server as Remote jj-cc-server

    CLI->>FS: Connect to /tmp/jj-cc-daemon-{user}-{hash}.sock
    alt Socket Active
        FS-->>CLI: Connected via UDS
        CLI->>Daemon: Forward gRPC Request
        Daemon->>Server: Multiplex over warm HTTP/2 Channel
        Server-->>CLI: gRPC Response
    else Socket Missing / Stale
        FS-->>CLI: Connection Refused
        CLI->>Daemon: Spawn background process (jj-cc-daemon --server URL --socket PATH)
        loop Poll up to 3s
            CLI->>FS: Retry UDS connect every 15ms
        end
        alt Daemon Ready
            FS-->>CLI: Connected via UDS
            CLI->>Daemon: Forward gRPC Request
        else Spawn Failed / Timeout
            Note over CLI,Server: Graceful Fallback
            CLI->>Server: Connect directly over TCP HTTP/2
        end
    end
```

---

## Daemon CLI Management

```bash
# Start daemon manually in foreground or background
jj cc daemon start --server http://127.0.0.1:8080

# Check daemon connection status
jj cc daemon status

# Stop running daemon and clean up UDS socket
jj cc daemon stop
```
