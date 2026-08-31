-- SQLite Schema for Commit Cloud Server

-- Repository registration table for tracking active Commit Cloud repos
-- Used when running `jj cc init` to register a new remote repository
CREATE TABLE IF NOT EXISTS repos (
    repo_id TEXT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Commit objects store for serialized commit metadata and history nodes
-- Used when reading and writing commits during change history operations
CREATE TABLE IF NOT EXISTS commits (
    repo_id TEXT NOT NULL,
    commit_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, commit_id)
);

-- Operation objects store for Jujutsu's operation log graph entries
-- Used when recording repository state transitions and running operation log queries
CREATE TABLE IF NOT EXISTS operations (
    repo_id TEXT NOT NULL,
    op_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, op_id)
);

-- Operation heads tracking table for resolving the latest operation heads
-- Used during op log updates to advance the repository operation head pointers
CREATE TABLE IF NOT EXISTS op_heads (
    repo_id TEXT NOT NULL,
    op_id BLOB NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, op_id)
);

-- Directory tree objects store for serialized directory trees and entry lists
-- Used during snapshotting and tree walking to resolve directory hierarchies
CREATE TABLE IF NOT EXISTS trees (
    repo_id TEXT NOT NULL,
    tree_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, tree_id)
);

-- File content / blob store for binary and text file contents
-- Used when reading and writing file contents for working copy snapshots and VFS reads
CREATE TABLE IF NOT EXISTS files (
    repo_id TEXT NOT NULL,
    file_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, file_id)
);

-- Symlink target metadata store for symbolic links in the repository tree
-- Used when reading and writing symlink entries in project directory trees
CREATE TABLE IF NOT EXISTS symlinks (
    repo_id TEXT NOT NULL,
    symlink_id BLOB NOT NULL,
    target TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, symlink_id)
);

-- View objects store for repository views (bookmarks, working copy commit IDs, remote refs)
-- Used when saving and loading repository state views associated with operations
CREATE TABLE IF NOT EXISTS views (
    repo_id TEXT NOT NULL,
    view_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, view_id)
);

-- Workspace metadata store for tracking active named workspaces and working copies
-- Used when managing working copy checkouts and detecting un-snapshotted changes
CREATE TABLE IF NOT EXISTS workspaces (
    repo_id TEXT NOT NULL,
    user TEXT NOT NULL,
    workspace_name TEXT NOT NULL,
    commit_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    tree_id BLOB NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, user, workspace_name)
);
