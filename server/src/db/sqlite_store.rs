use crate::db::db_store::{DatabaseStore, DbError};
use cc_proto::backend::{Commit, ReadTreeResponse, TreeEntry};
use cc_proto::op_store::{Operation, View};
use prost::Message;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DbError> {
        let conn = Connection::open(path).map_err(|e| DbError::Internal(e.to_string()))?;
        let schema = include_str!("../../db/schema_sqlite.sql");
        conn.execute_batch(schema)
            .map_err(|e| DbError::Internal(format!("Failed to execute schema: {e}")))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    #[allow(dead_code)]
    pub fn in_memory() -> Result<Self, DbError> {

        let conn = Connection::open_in_memory().map_err(|e| DbError::Internal(e.to_string()))?;
        let schema = include_str!("../../db/schema_sqlite.sql");
        conn.execute_batch(schema)
            .map_err(|e| DbError::Internal(format!("Failed to execute schema: {e}")))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

#[tonic::async_trait]
impl DatabaseStore for SqliteStore {
    async fn register_repository(&self, requested_repo_id: Option<&str>) -> Result<String, DbError> {
        let repo_id = requested_repo_id
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO repositories (repo_id) VALUES (?1)",
            params![repo_id],
        )
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(repo_id)
    }


    async fn read_commit(&self, repo_id: &str, commit_id: &[u8]) -> Result<Option<Commit>, DbError> {
        let conn = self.conn.lock().unwrap();
        let blob: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM commits WHERE repo_id = ?1 AND commit_id = ?2",
                params![repo_id, commit_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(bytes) = blob {
            let commit = Commit::decode(&bytes[..])
                .map_err(|e| DbError::InvalidData(format!("Failed to decode commit proto: {e}")))?;
            Ok(Some(commit))
        } else {
            Ok(None)
        }
    }

    async fn write_commit(&self, repo_id: &str, mut commit: Commit) -> Result<Vec<u8>, DbError> {
        let commit_id = if commit.commit_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&commit.change_id);
            hasher.update(&commit.root_tree_id);
            for p in &commit.parent_commit_ids {
                hasher.update(p);
            }
            hasher.update(commit.description.as_bytes());
            let hash = hasher.finalize();
            hash.as_bytes()[0..20].to_vec()
        } else {
            commit.commit_id.clone()
        };

        commit.commit_id = commit_id.clone();
        let bytes = commit.encode_to_vec();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO commits (repo_id, commit_id, data) VALUES (?1, ?2, ?3)",
            params![repo_id, commit_id, bytes],
        )
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(commit_id)
    }

    async fn read_tree(&self, repo_id: &str, tree_id: &[u8]) -> Result<Option<Vec<TreeEntry>>, DbError> {
        let conn = self.conn.lock().unwrap();
        let blob: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM trees WHERE repo_id = ?1 AND tree_id = ?2",
                params![repo_id, tree_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(bytes) = blob {
            let resp = ReadTreeResponse::decode(&bytes[..])
                .map_err(|e| DbError::InvalidData(format!("Failed to decode tree proto: {e}")))?;
            Ok(Some(resp.entries))
        } else {
            Ok(None)
        }
    }

    async fn write_tree(&self, repo_id: &str, entries: Vec<TreeEntry>) -> Result<Vec<u8>, DbError> {
        let mut hasher = blake3::Hasher::new();
        for entry in &entries {
            hasher.update(entry.name.as_bytes());
            hasher.update(&entry.entry_id);
        }
        let hash = hasher.finalize();
        let tree_id = hash.as_bytes()[0..20].to_vec();

        let resp = ReadTreeResponse {
            tree_id: tree_id.clone(),
            entries,
        };
        let bytes = resp.encode_to_vec();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO trees (repo_id, tree_id, data) VALUES (?1, ?2, ?3)",
            params![repo_id, tree_id, bytes],
        )
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(tree_id)
    }

    async fn read_file(&self, repo_id: &str, file_id: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        let conn = self.conn.lock().unwrap();
        let blob: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM files WHERE repo_id = ?1 AND file_id = ?2",
                params![repo_id, file_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(blob)
    }

    async fn write_file(&self, repo_id: &str, content: &[u8]) -> Result<Vec<u8>, DbError> {
        let hash = blake3::hash(content);
        let file_id = hash.as_bytes()[0..20].to_vec();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO files (repo_id, file_id, data) VALUES (?1, ?2, ?3)",
            params![repo_id, file_id, content],
        )
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(file_id)
    }

    async fn read_symlink(&self, repo_id: &str, symlink_id: &[u8]) -> Result<Option<String>, DbError> {
        let conn = self.conn.lock().unwrap();
        let target: Option<String> = conn
            .query_row(
                "SELECT target FROM symlinks WHERE repo_id = ?1 AND symlink_id = ?2",
                params![repo_id, symlink_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(target)
    }

    async fn write_symlink(&self, repo_id: &str, target: &str) -> Result<Vec<u8>, DbError> {
        let hash = blake3::hash(target.as_bytes());
        let symlink_id = hash.as_bytes()[0..20].to_vec();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO symlinks (repo_id, symlink_id, target) VALUES (?1, ?2, ?3)",
            params![repo_id, symlink_id, target],
        )
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(symlink_id)
    }

    async fn read_operation(&self, repo_id: &str, op_id: &[u8]) -> Result<Option<Operation>, DbError> {
        let conn = self.conn.lock().unwrap();
        let blob: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM operations WHERE repo_id = ?1 AND op_id = ?2",
                params![repo_id, op_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(bytes) = blob {
            let op = Operation::decode(&bytes[..])
                .map_err(|e| DbError::InvalidData(format!("Failed to decode op proto: {e}")))?;
            Ok(Some(op))
        } else {
            Ok(None)
        }
    }

    async fn write_operation(&self, repo_id: &str, mut op: Operation) -> Result<Vec<u8>, DbError> {
        let op_id = if op.operation_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&op.view_id);
            for p in &op.parent_op_ids {
                hasher.update(p);
            }
            let hash = hasher.finalize();
            hash.as_bytes()[0..20].to_vec()
        } else {
            op.operation_id.clone()
        };

        op.operation_id = op_id.clone();
        let bytes = op.encode_to_vec();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO operations (repo_id, op_id, data) VALUES (?1, ?2, ?3)",
            params![repo_id, op_id, bytes],
        )
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(op_id)
    }

    async fn read_view(&self, repo_id: &str, view_id: &[u8]) -> Result<Option<View>, DbError> {
        let conn = self.conn.lock().unwrap();
        let blob: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM views WHERE repo_id = ?1 AND view_id = ?2",
                params![repo_id, view_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(bytes) = blob {
            let view = View::decode(&bytes[..])
                .map_err(|e| DbError::InvalidData(format!("Failed to decode view proto: {e}")))?;
            Ok(Some(view))
        } else {
            Ok(None)
        }
    }

    async fn write_view(&self, repo_id: &str, mut view: View) -> Result<Vec<u8>, DbError> {
        let view_id = if view.view_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            for h in &view.head_commit_ids {
                hasher.update(h);
            }
            let hash = hasher.finalize();
            hash.as_bytes()[0..20].to_vec()
        } else {
            view.view_id.clone()
        };

        view.view_id = view_id.clone();
        let bytes = view.encode_to_vec();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO views (repo_id, view_id, data) VALUES (?1, ?2, ?3)",
            params![repo_id, view_id, bytes],
        )
        .map_err(|e| DbError::Internal(e.to_string()))?;

        Ok(view_id)
    }

    async fn get_op_heads(&self, repo_id: &str) -> Result<Vec<Vec<u8>>, DbError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT op_id FROM op_heads WHERE repo_id = ?1")
            .map_err(|e| DbError::Internal(e.to_string()))?;
        let rows = stmt
            .query_map(params![repo_id], |row| row.get::<_, Vec<u8>>(0))
            .map_err(|e| DbError::Internal(e.to_string()))?;

        let mut heads = Vec::new();
        for r in rows {
            heads.push(r.map_err(|e| DbError::Internal(e.to_string()))?);
        }
        Ok(heads)
    }

    async fn add_op_head(&self, repo_id: &str, op_id: &[u8]) -> Result<Vec<Vec<u8>>, DbError> {
        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO op_heads (repo_id, op_id) VALUES (?1, ?2)",
                params![repo_id, op_id],
            )
            .map_err(|e| DbError::Internal(e.to_string()))?;
        }
        self.get_op_heads(repo_id).await
    }

    async fn remove_op_head(&self, repo_id: &str, op_id: &[u8]) -> Result<Vec<Vec<u8>>, DbError> {
        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "DELETE FROM op_heads WHERE repo_id = ?1 AND op_id = ?2",
                params![repo_id, op_id],
            )
            .map_err(|e| DbError::Internal(e.to_string()))?;
        }
        self.get_op_heads(repo_id).await
    }
}
