# End-to-End Workflows

This guide illustrates the core workflows for running `jj-cc-server`, initializing cloud repositories, synchronizing across workspaces, and importing Git repositories.

---

## 1. Starting the Server

```bash
# In-Memory Backend (ephemeral)
cargo run --bin jj-cc-server -- --port 8080 --storage memory

# SQLite Backend (local persistent file)
cargo run --bin jj-cc-server -- --port 8080 --storage sqlite --database-path /tmp/cc.db

# Google Cloud Spanner Backend (production)
cargo run --bin jj-cc-server -- --port 8080 --storage spanner \
  --spanner-project my-gcp-project \
  --spanner-instance my-instance \
  --spanner-database my-db
```

---

## 2. Repository Initialization (`jj cc init`)

### A. Create a New Cloud Repository (`--create-repo`)
```bash
jj cc init --server http://127.0.0.1:8080 --create-repo my-project
```

```mermaid
sequenceDiagram
    participant User
    participant CLI as jj cc init
    participant Server as jj-cc-server

    User->>CLI: jj cc init --create-repo
    CLI->>Server: RegisterRepository(name)
    Server-->>CLI: Returns new repo_id (UUID)
    CLI->>Server: WriteTree(empty) & WriteCommit(root_wc)
    CLI->>Server: WriteView(initial) & WriteOperation(init_op)
    CLI->>Server: UpdateOpHeads(new_op_head = init_op)
    CLI->>Server: CreateWorkspace(user, "default", root_wc)
    CLI->>CLI: Write .jj/repo/store/commit_cloud/config.toml
    CLI-->>User: Initialized repo <UUID>
```

### B. Join an Existing Cloud Repository (`--repo-id`)
```bash
jj cc init --server http://127.0.0.1:8080 --repo-id <EXISTING_UUID> ./workspace-b
```

```mermaid
sequenceDiagram
    participant User
    participant CLI as jj cc init
    participant Server as jj-cc-server

    User->>CLI: jj cc init --repo-id <UUID>
    CLI->>Server: ReconcileOpHeads(repo_id)
    Server-->>CLI: Returns latest resolved op_head
    CLI->>Server: ReadOperation(op_head) & ReadView()
    CLI->>Server: WriteCommit(new_wc_commit)
    CLI->>Server: CreateWorkspace(user, workspace_name, new_wc_commit)
    CLI->>CLI: Write .jj/repo/store/commit_cloud/config.toml
    CLI-->>User: Workspace attached to repo <UUID>
```

---

## 3. Concurrent Edits & Auto-Reconciliation

When two workspaces push operations concurrently, the next command automatically reconciles divergent operation heads on the server:

```mermaid
sequenceDiagram
    participant WSA as Workspace A
    participant WSB as Workspace B
    participant Server as jj-cc-server (Reconcile Engine)

    WSA->>Server: UpdateOpHeads(old=[Op1], new=OpA)
    WSB->>Server: UpdateOpHeads(old=[Op1], new=OpB)
    Note over Server: Active Op Heads are now divergent: [OpA, OpB]

    WSA->>Server: Next command (e.g. jj log) -> ReconcileOpHeads()
    Server->>Server: Load ServerBackend & ServerOpStore
    Server->>Server: 3-Way Merge Views (OpA + OpB -> OpMerged)
    Server->>Server: Compare-And-Swap OpHeads ([OpA, OpB] -> [OpMerged])
    Server-->>WSA: Returns single merged head [OpMerged]
```

---

## 4. Importing a Git Repository (`jj cc import-git`)

```bash
jj cc import-git --git-dir /path/to/git/repo
```

```mermaid
flowchart LR
    GitRepo[("Local .git Repo")] --> Walk["Traverse Git DAG\n(Blobs -> Trees -> Commits)"]
    Walk --> Upload["Stream to BackendService\n(WriteFile, WriteTree, WriteCommit)"]
    Upload --> Bookmarks["Map Git Branches\nto jj View Bookmarks"]
    Bookmarks --> OpLog["WriteOperation & UpdateOpHeads"]
```
