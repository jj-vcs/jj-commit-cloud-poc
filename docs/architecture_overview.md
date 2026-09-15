# Architecture Overview

Commit Cloud decouples Jujutsu (`jj`) storage from the local filesystem into a remote gRPC server (`jj-cc-server`) with optional local Unix Domain Socket (UDS) connection pooling via `jj-cc-daemon`.

---

## End-to-End System Flow

```mermaid
flowchart LR
    CLI["jj CLI\n(cli crate)"] --> Lib["cc-lib\n(lib crate)"]
    Lib -- "use_daemon = true\n(UDS gRPC)" --> Daemon["jj-cc-daemon\n(daemon crate)"]
    Lib -- "use_daemon = false\n(Direct HTTP/2 gRPC)" --> Server["jj-cc-server\n(server crate)"]
    Daemon -- "Persistent HTTP/2 Pool" --> Server
    Server --> Store{"Store Trait"}
    Store --> Mem["MemoryStore"]
    Store --> Sqlite["SqliteStore"]
    Store --> Spanner["SpannerStore"]
```

---

## Workspace Package Dependency Graph

The workspace consists of 6 Rust packages. Arrows indicate compile-time, dev, and build-script relationships:

```mermaid
flowchart TD
    CLI["cli\n(bin: jj)"]
    Lib["lib\n(cc-lib)"]
    Common["common\n(cc-common)"]
    Daemon["daemon\n(bin: jj-cc-daemon)"]
    Server["server\n(bin: jj-cc-server)"]
    TestUtils["testutils"]

    %% Normal dependencies
    CLI -- "depends on" --> Lib
    Lib -- "depends on" --> Common
    Daemon -- "depends on" --> Common
    Server -- "depends on" --> Common

    %% Runtime process spawning
    Lib -. "spawns at runtime via UDS" .-> Daemon

    %% Dev dependencies
    CLI -- "dev-dependency" --> TestUtils
    CLI -- "dev-dependency" --> Common
    Server -- "dev-dependency" --> TestUtils

    %% Build script compilation
    TestUtils == "build.rs compiles binary" ==> Server
    TestUtils == "build.rs compiles binary" ==> Daemon
```

| Package | Binary / Lib | Role |
| :--- | :--- | :--- |
| **`common`** | `cc-common` | Protobuf schemas (`backend.proto`, `op_store.proto`, `workspace.proto`) & `jj-lib` $\leftrightarrow$ Proto conversions. |
| **`lib`** | `cc-lib` | Cloud implementations of `jj-lib` traits (`Backend`, `OpStore`, `OpHeadsStore`, `WorkingCopy`). |
| **`cli`** | `jj` | Custom `jj` binary integrating `cc-lib` and subcommands (`jj cc init`, `jj cc daemon`, `jj cc import-git`). |
| **`daemon`** | `jj-cc-daemon` | Local UDS proxy server that multiplexes CLI calls over a single persistent HTTP/2 connection. |
| **`server`** | `jj-cc-server` | Remote gRPC server implementing object storage, workspace tracking, and op head reconciliation. |
| **`testutils`** | `testutils` | Test harness whose `build.rs` compiles `jj-cc-server` and `jj-cc-daemon` for integration tests. |

---

## gRPC Services

```mermaid
classDiagram
    class BackendService {
        +RegisterRepository()
        +ReadCommit() / WriteCommit()
        +ReadTree() / WriteTree()
        +ReadFile() / WriteFile()
        +ReadSymlink() / WriteSymlink()
    }
    class OpStoreService {
        +ReadOperation() / WriteOperation()
        +ReadView() / WriteView()
        +GetOpHeads() / UpdateOpHeads()
        +ReconcileOpHeads()
    }
    class WorkspaceService {
        +CreateWorkspace()
        +GetWorkspace()
        +UpdateWorkspace()
        +DeleteWorkspace()
        +ListWorkspaces()
    }
```
