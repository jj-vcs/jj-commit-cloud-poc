-- SQLite Schema for Commit Cloud Server

CREATE TABLE IF NOT EXISTS repositories (
    repo_id TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS commits (
    repo_id TEXT NOT NULL,
    commit_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, commit_id)
);

CREATE TABLE IF NOT EXISTS operations (
    repo_id TEXT NOT NULL,
    op_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, op_id)
);

CREATE TABLE IF NOT EXISTS op_heads (
    repo_id TEXT NOT NULL,
    op_id BLOB NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, op_id)
);

CREATE TABLE IF NOT EXISTS trees (
    repo_id TEXT NOT NULL,
    tree_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, tree_id)
);

CREATE TABLE IF NOT EXISTS files (
    repo_id TEXT NOT NULL,
    file_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, file_id)
);

CREATE TABLE IF NOT EXISTS symlinks (
    repo_id TEXT NOT NULL,
    symlink_id BLOB NOT NULL,
    target TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, symlink_id)
);

CREATE TABLE IF NOT EXISTS views (
    repo_id TEXT NOT NULL,
    view_id BLOB NOT NULL,
    data BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (repo_id, view_id)
);


