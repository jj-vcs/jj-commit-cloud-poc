# Storage Backend & Schema Design

The server persists all Jujutsu repository objects, operation logs, and workspace pointers through an asynchronous, pluggable `Store` trait (`server/src/store/mod.rs`).

---

## Pluggable `Store` Architecture

All gRPC service handlers interact strictly with `Arc<dyn Store>`, enabling interchangeable storage engines without altering business logic.

| Engine | Persistence | Concurrency Model | Target Environment |
| :--- | :--- | :--- | :--- |
| **`MemoryStore`** | Ephemeral RAM | `tokio::sync::RwLock` | Unit tests & rapid prototyping |
| **`SqliteStore`** | Single file DB | `spawn_blocking` + SQLite WAL Transactions | Local single-node server |
| **`SpannerStore`** | Distributed Cloud DB | Spanner Read-Write Transactions | Multi-client Cloud Run production |

### Core Async `Store` Methods
* **Repository Registry:** `is_repo_registered`, `register_repo`
* **Content-Addressed Objects:** `get_commit`/`put_commit`, `get_tree`/`put_tree`, `get_file`/`put_file`, `get_symlink`/`put_symlink`
* **Operation Log & Views:** `get_operation`/`put_operation`, `get_view`/`put_view`
* **Atomic Op Head Updates:**
  * `update_op_heads_append_new_remove_old`: Appends a new operation head and removes parent heads (used by client transactions).
  * `update_op_heads_compare_and_swap`: Atomically replaces the active head set with a single merged head if and only if the active set matches expected heads (used by server-side reconciliation).
* **Workspaces:** `get_workspace`, `put_workspace`, `delete_workspace`, `list_workspaces`

---

## Database Relational Schema (`erDiagram`)

Every entity is scoped to a `repo_id` primary key partition. Content-addressable objects (`commits`, `trees`, `files`, `symlinks`, `operations`, `views`) use their deterministic SHA-1/SHA-256 hash as the second composite primary key column.

```mermaid
erDiagram
    REPOS ||--o{ COMMITS : "contains"
    REPOS ||--o{ TREES : "contains"
    REPOS ||--o{ FILES : "contains"
    REPOS ||--o{ SYMLINKS : "contains"
    REPOS ||--o{ VIEWS : "contains"
    REPOS ||--o{ OPERATIONS : "contains"
    REPOS ||--o{ OP_HEADS : "tracks active"
    REPOS ||--o{ WORKSPACES : "manages"

    OPERATIONS }o--|| VIEWS : "references view_id"
    OP_HEADS }o--|| OPERATIONS : "points to op_id"
    WORKSPACES }o--|| COMMITS : "checked out commit_id"
    WORKSPACES }o--|| OPERATIONS : "at operation_id"
    WORKSPACES }o--|| TREES : "working tree_id"
    COMMITS }o--|| TREES : "root_tree_id (in proto data)"
    TREES }o--o{ FILES : "file_id (in proto data)"
    TREES }o--o{ SYMLINKS : "symlink_id (in proto data)"

    REPOS {
        string repo_id PK "UUID v4"
        string name "Optional repo name"
        timestamp created_at
    }

    COMMITS {
        string repo_id PK, FK
        bytes commit_id PK "Object hash"
        bytes data "Protobuf Commit"
        timestamp created_at
    }

    TREES {
        string repo_id PK, FK
        bytes tree_id PK "Object hash"
        bytes data "Repeated TreeEntry"
        timestamp created_at
    }

    FILES {
        string repo_id PK, FK
        bytes file_id PK "Object hash"
        bytes data "Raw file blob"
        timestamp created_at
    }

    SYMLINKS {
        string repo_id PK, FK
        bytes symlink_id PK "Object hash"
        string target "Symlink target path"
        timestamp created_at
    }

    VIEWS {
        string repo_id PK, FK
        bytes view_id PK "Object hash"
        bytes data "Protobuf View (bookmarks, heads)"
        timestamp created_at
    }

    OPERATIONS {
        string repo_id PK, FK
        bytes op_id PK "Object hash"
        bytes data "Protobuf Operation"
        timestamp created_at
    }

    OP_HEADS {
        string repo_id PK, FK
        bytes op_id PK, FK "Active head operation ID"
        timestamp updated_at
    }

    WORKSPACES {
        string repo_id PK, FK
        string user PK "Username"
        string workspace_name PK "Workspace identifier"
        bytes commit_id FK "Current WC commit"
        bytes operation_id FK "Current WC operation"
        bytes tree_id FK "Current WC tree"
        timestamp updated_at
    }
```
