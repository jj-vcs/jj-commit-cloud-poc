# Cloud Working Copy & Workspaces

`CommitCloudWorkingCopy` decouples workspace state tracking from local state files, storing active workspace checkouts in the remote database via `WorkspaceService`.

---

## Multi-Workspace Cloud Model

Multiple machines or directories can attach to the same `repo_id` while maintaining independent working copy commits:

```mermaid
flowchart TD
    subgraph Cloud Server ["jj-cc-server (Shared Repo ID)"]
        Repo[("Repo: c4b18e92...")]
        WS_Table[("workspaces Table")]
        Commits[("commits / trees / files")]
        Repo --- WS_Table
        Repo --- Commits
    end

    subgraph MachineA ["Laptop (workspace: default)"]
        WCA["CommitCloudWorkingCopy\nuser: alice, name: default"]
    end

    subgraph MachineB ["Cloud Desktop (workspace: secondary)"]
        WCB["CommitCloudWorkingCopy\nuser: alice, name: secondary"]
    end

    WCA <== "commit_id: A1\ntree_id: T1" ==> WS_Table
    WCB <== "commit_id: B2\ntree_id: T2" ==> WS_Table
```

---

## Working Copy Snapshot & Checkout Lifecycle

```mermaid
sequenceDiagram
    participant CLI as jj CLI
    participant WC as CommitCloudWorkingCopy
    participant Backend as BackendService (Server)
    participant WS as WorkspaceService (Server)

    Note over CLI,WS: 1. Snapshot Working Copy Changes
    CLI->>WC: start_working_copy_mutation()
    WC->>WC: Scan local filesystem changes
    WC->>Backend: WriteFile() & WriteTree() (Upload modified blobs/trees)
    Backend-->>WC: Returns new tree_id
    WC->>WS: UpdateWorkspace(repo_id, user, workspace_name, commit_id, op_id, tree_id)
    WS-->>WC: Workspace state persisted

    Note over CLI,WS: 2. Checkout New Commit
    CLI->>WC: check_out(new_commit)
    WC->>Backend: ReadTree(new_commit.tree_id) & ReadFile()
    Backend-->>WC: Materialize files to local disk
    WC->>WS: UpdateWorkspace(commit_id=new_commit.id, tree_id=new_commit.tree_id)
    WS-->>CLI: Checkout complete
```

---

## `WorkspaceService` RPC Operations

| RPC | Request Fields | Description |
| :--- | :--- | :--- |
| **`CreateWorkspace`** | `repo_id`, `user`, `workspace_name`, `commit_id`, `operation_id`, `tree_id` | Registers a new workspace checkout pointer. |
| **`GetWorkspace`** | `repo_id`, `user`, `workspace_name` | Retrieves the current checked-out commit, tree, and operation for a workspace. |
| **`UpdateWorkspace`** | `repo_id`, `user`, `workspace_name`, `commit_id`, `operation_id`, `tree_id` | Updates workspace pointers after snapshotting or checking out a commit. |
| **`DeleteWorkspace`** | `repo_id`, `user`, `workspace_name` | Removes workspace tracking metadata (`jj workspace forget`). |
