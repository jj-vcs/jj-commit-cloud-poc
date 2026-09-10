-- Google Cloud Spanner Schema for Commit Cloud Server

CREATE TABLE IF NOT EXISTS repos (
  repo_id STRING(64) NOT NULL,
  name STRING(256),
  created_at TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (repo_id);

CREATE TABLE IF NOT EXISTS commits (
  repo_id STRING(64) NOT NULL,
  commit_id BYTES(MAX) NOT NULL,
  data BYTES(MAX)
) PRIMARY KEY (repo_id, commit_id);

CREATE TABLE IF NOT EXISTS trees (
  repo_id STRING(64) NOT NULL,
  tree_id BYTES(MAX) NOT NULL,
  data BYTES(MAX)
) PRIMARY KEY (repo_id, tree_id);

CREATE TABLE IF NOT EXISTS files (
  repo_id STRING(64) NOT NULL,
  file_id BYTES(MAX) NOT NULL,
  data BYTES(MAX)
) PRIMARY KEY (repo_id, file_id);

CREATE TABLE IF NOT EXISTS views (
  repo_id STRING(64) NOT NULL,
  view_id BYTES(MAX) NOT NULL,
  data BYTES(MAX)
) PRIMARY KEY (repo_id, view_id);

CREATE TABLE IF NOT EXISTS operations (
  repo_id STRING(64) NOT NULL,
  op_id BYTES(MAX) NOT NULL,
  data BYTES(MAX)
) PRIMARY KEY (repo_id, op_id);

CREATE TABLE IF NOT EXISTS op_heads (
  repo_id STRING(64) NOT NULL,
  op_id BYTES(MAX) NOT NULL
) PRIMARY KEY (repo_id, op_id);

CREATE TABLE IF NOT EXISTS workspaces (
  repo_id STRING(64) NOT NULL,
  user STRING(256) NOT NULL,
  workspace_name STRING(256) NOT NULL,
  commit_id BYTES(MAX),
  operation_id BYTES(MAX),
  tree_id BYTES(MAX)
) PRIMARY KEY (repo_id, user, workspace_name);
