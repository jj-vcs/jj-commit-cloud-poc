# Storage Backend & Schema Design

The server persists all Jujutsu repository objects, operation logs, and workspace pointers through an asynchronous, pluggable `Store` trait.

---

## Pluggable `Store` Architecture

All gRPC service handlers interact strictly with `Arc<dyn Store>`, enabling interchangeable storage engines without altering business logic.

```mermaid
classDiagram
    class Store {
        <<interface>>
        +async is_repo_registered(repo_id) bool
        +async register_repo(repo_id, name)
        +async get_commit(repo_id, commit_id) Option~Commit~
        +async put_commit(repo_id, commit_id, commit)
        +async get_tree(repo_id, tree_id) Option~Vec~TreeEntry~~
        +async put_tree(repo_id, tree_id, entries)
        +async get_file(repo_id, file_id) Option~Vec~u8~~
        +async put_file(repo_id, file_id, content)
        +async get_symlink(repo_id, symlink_id) Option~String~
        +async put_symlink(repo_id, symlink_id, target)
        +async get_operation(repo_id, op_id) Option~Operation~
        +async put_operation(repo_id, op_id, op)
        +async get_view(repo_id, view_id) Option~View~
        +async put_view(repo_id, view_id, view)
        +async get_op_heads(repo_id) Option~Vec~Vec~u8~~~
        +async update_op_heads_append_new_remove_old(repo_id, remove, add)
        +async update_op_heads_compare_and_swap(repo_id, expected, new)
        +async get_workspace(repo_id, user, name) Option~WorkspaceState~
        +async put_workspace(repo_id, user, name, state)
        +async delete_workspace(repo_id, user, name)
    }

    Store <|.. MemoryStore : In-Memory RwLock Maps
    Store <|.. SqliteStore : Local SQLite (WAL Mode)
    Store <|.. SpannerStore : Google Cloud Spanner
```

| Engine | Persistence | Concurrency Model | Target Environment |
| :--- | :--- | :--- | :--- |
| **`MemoryStore`** | Ephemeral RAM | `tokio::sync::RwLock` | Unit tests & rapid prototyping |
| **`SqliteStore`** | Single file DB | `spawn_blocking` + SQLite Transactions | Local single-node server |
| **`SpannerStore`** | Distributed Cloud DB | Spanner Read-Write Transactions | Multi-client Cloud Run production |

---

## Database Relational Schema (`erDiagram`)

Every entity is scoped to a `repo_id` primary key partition. Content-addressable objects (`commits`, `trees`, `files`, `symlinks`, `operations`, `views`) use their deterministic SHA-256 hash as the second composite primary key column.

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
        bytes commit_id PK "SHA-256 hash"
        bytes data "Protobuf Commit"
        timestamp created_at
    }

    TREES {
        string repo_id PK, FK
        bytes tree_id PK "SHA-256 hash"
        bytes data "Repeated TreeEntry"
        timestamp created_at
    }

    FILES {
        string repo_id PK, FK
        bytes file_id PK "SHA-256 hash"
        bytes data "Raw file blob"
        timestamp created_at
    }

    SYMLINKS {
        string repo_id PK, FK
        bytes symlink_id PK "SHA-256 hash"
        string target "Symlink target path"
        timestamp created_at
    }

    VIEWS {
        string repo_id PK, FK
        bytes view_id PK "SHA-256 hash"
        bytes data "Protobuf View (bookmarks, heads)"
        timestamp created_at
    }

    OPERATIONS {
        string repo_id PK, FK
        bytes op_id PK "SHA-256 hash"
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

---

## Atomic Op Head Concurrency Primitives

To support concurrent clients without losing history, `Store` provides two atomic primitives on `OP_HEADS`:

```mermaid
sequenceDiagram
    participant ClientA as Client A
    participant ClientB as Client B
    participant Store as Store (SQLite / Spanner Transaction)

    Note over ClientA,Store: 1. Client Update (Append & Remove)
    ClientA->>Store: update_op_heads_append_new_remove_old(remove=[Op1], add=Op2)
    Store-->>ClientA: Active Heads: [Op2]

    ClientB->>Store: update_op_heads_append_new_remove_old(remove=[Op1], add=Op3)
    Note right of Store: Op1 already removed, but Op3 is added concurrently
    Store-->>ClientB: Active Heads: [Op2, Op3] (Divergent!)

    Note over ClientA,Store: 2. Server Reconciliation (Strict Compare-And-Swap)
    ClientA->>Store: update_op_heads_compare_and_swap(expected=[Op2, Op3], new=OpMerged)
    Store-->>ClientA: Active Heads: [OpMerged]
```
