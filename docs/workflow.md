# End-to-End Workflows

This guide outlines the core commands and workflows for running `jj-cc-server`, initializing cloud repositories, synchronizing workspaces, and importing Git repositories.

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
* Calls `RegisterRepository` on the server to allocate a unique `repo_id` (UUID).
* Uploads the empty root tree, initial working copy commit, view, and root operation.
* Registers the `default` workspace in `WorkspaceService` and writes `.jj/repo/store/commit_cloud/config.toml`.

### B. Join an Existing Cloud Repository (`--repo-id`)
```bash
jj cc init --server http://127.0.0.1:8080 --repo-id <EXISTING_UUID> ./workspace-b
```
* Calls `ReconcileOpHeads` to fetch the latest resolved operation head for `<EXISTING_UUID>`.
* Creates a new working copy commit for this workspace and registers it via `CreateWorkspace`.
* Writes `.jj/repo/store/commit_cloud/config.toml` pointing to `<EXISTING_UUID>`.

---

## 3. Concurrent Edits & Auto-Reconciliation

When multiple workspaces push concurrent operations, `op_heads` temporarily holds multiple head IDs:
1. On the next CLI command (e.g., `jj log`, `jj status`, `jj new`), `CommitCloudOpHeadsStore::get_op_heads()` invokes `ReconcileOpHeads` on the server.
2. The server loads the divergent operations via `ServerBackend` and `ServerOpStore`, performs a 3-way view merge (`RepoLoader::load_at_head()`), and writes the merged operation.
3. The server atomically replaces the divergent heads with the single merged operation ID via Compare-And-Swap (`update_op_heads_compare_and_swap`).

---

## 4. Importing a Git Repository (`jj cc import-git`)

```bash
jj cc import-git --git-dir /path/to/git/repo
```
* Opens the local `.git` repository using `gix` (Gitoxide).
* Traverses the Git commit graph and uploads file blobs (`WriteFile`), directory trees (`WriteTree`), and commits (`WriteCommit`) to `BackendService`.
* Maps Git branches/refs to `jj` bookmarks in the initial `View` and updates the repository's operation head.
