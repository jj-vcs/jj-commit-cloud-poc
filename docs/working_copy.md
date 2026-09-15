# Cloud Working Copy & Workspaces

`CommitCloudWorkingCopy` decouples workspace state tracking from local state files, storing active workspace checkouts in the remote database via `WorkspaceService`.

---

## Multi-Workspace Cloud Model

Multiple machines or local directories can attach to the same `repo_id` while maintaining independent working copy checkouts:
* Each workspace is uniquely identified by `(repo_id, user, workspace_name)`.
* The server's `workspaces` table records the active `commit_id`, `tree_id`, and `operation_id` for each workspace.

---

## Working Copy Lifecycle

1. **Snapshotting Local Changes (`start_working_copy_mutation`)**:
   * Scans local working directory files for modifications.
   * Uploads modified file blobs (`WriteFile`) and updated directory trees (`WriteTree`) to `BackendService`.
   * Persists the updated `commit_id`, `tree_id`, and `operation_id` to the server via `UpdateWorkspace`.
2. **Checking Out a Commit (`check_out`)**:
   * Fetches the target commit's root tree (`ReadTree`) and file blobs (`ReadFile`) from `BackendService`.
   * Materializes the files to the local working copy directory.
   * Updates the workspace's remote pointer via `UpdateWorkspace`.

---

## `WorkspaceService` RPC Operations

| RPC | Request Fields | Description |
| :--- | :--- | :--- |
| **`CreateWorkspace`** | `repo_id`, `user`, `workspace_name`, `commit_id`, `operation_id`, `tree_id` | Registers a new workspace checkout pointer. |
| **`GetWorkspace`** | `repo_id`, `user`, `workspace_name` | Retrieves the current checked-out commit, tree, and operation for a workspace. |
| **`UpdateWorkspace`** | `repo_id`, `user`, `workspace_name`, `commit_id`, `operation_id`, `tree_id` | Updates workspace pointers after snapshotting or checking out a commit. |
| **`DeleteWorkspace`** | `repo_id`, `user`, `workspace_name` | Removes workspace tracking metadata (`jj workspace forget`). |
| **`ListWorkspaces`** | `repo_id` | Lists all active workspaces registered under a repository. |
