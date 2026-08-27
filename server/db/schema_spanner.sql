-- Google Cloud Spanner Schema for Commit Cloud Server

-- Repository registration table for tracking active Commit Cloud repos
CREATE TABLE IF NOT EXISTS repos (
  repo_id STRING(64) NOT NULL,
  name STRING(256),
  created_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (repo_id);

-- Commit objects store for serialized commit metadata
CREATE TABLE IF NOT EXISTS commits (
  repo_id STRING(64) NOT NULL,
  commit_id BYTES(MAX) NOT NULL,
  data BYTES(MAX),
  created_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (repo_id, commit_id);

-- Directory tree objects store for serialized directory trees
CREATE TABLE IF NOT EXISTS trees (
  repo_id STRING(64) NOT NULL,
  tree_id BYTES(MAX) NOT NULL,
  data BYTES(MAX),
  created_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (repo_id, tree_id);

-- File content / blob store for file contents
CREATE TABLE IF NOT EXISTS files (
  repo_id STRING(64) NOT NULL,
  file_id BYTES(MAX) NOT NULL,
  content BYTES(MAX),
  created_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (repo_id, file_id);

-- View objects store for repository views
CREATE TABLE IF NOT EXISTS views (
  repo_id STRING(64) NOT NULL,
  view_id BYTES(MAX) NOT NULL,
  data BYTES(MAX),
  created_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (repo_id, view_id);

-- Operation objects store for Jujutsu's operation log graph entries
CREATE TABLE IF NOT EXISTS operations (
  repo_id STRING(64) NOT NULL,
  op_id BYTES(MAX) NOT NULL,
  data BYTES(MAX),
  created_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (repo_id, op_id);

-- Operation heads tracking table for resolving the latest operation heads
CREATE TABLE IF NOT EXISTS op_heads (
  repo_id STRING(64) NOT NULL,
  op_id BYTES(MAX) NOT NULL,
  updated_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (repo_id, op_id);
