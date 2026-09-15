# Architecture Overview

Commit Cloud decouples Jujutsu (`jj`) storage from the local filesystem into a remote gRPC server (`jj-cc-server`) with optional local Unix Domain Socket (UDS) connection pooling via `jj-cc-daemon`.

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

    CLI --> Lib
    Lib --> Common
    Daemon --> Common
    Server --> Common

    CLI -- "dev dependency" --> TestUtils
    CLI -- "dev dependency" --> Common
    Server -- "dev dependency" --> TestUtils

    TestUtils == "compiles binary" ==> Server
    TestUtils == "compiles binary" ==> Daemon
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

* **`BackendService`** (`backend.proto`): Reads and writes content-addressed repository objects (`RegisterRepository`, `ReadCommit`/`WriteCommit`, `ReadTree`/`WriteTree`, `ReadFile`/`WriteFile`, `ReadSymlink`/`WriteSymlink`).
* **`OpStoreService`** (`op_store.proto`): Manages operation log history, views, and head pointers (`ReadOperation`/`WriteOperation`, `ReadView`/`WriteView`, `GetOpHeads`/`UpdateOpHeads`, `ReconcileOpHeads`).
* **`WorkspaceService`** (`workspace.proto`): Manages remote working copy metadata (`CreateWorkspace`, `GetWorkspace`, `UpdateWorkspace`, `DeleteWorkspace`, `ListWorkspaces`).
