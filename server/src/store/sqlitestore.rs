use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use prost::Message;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::Store;

#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init_tables()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init_tables()?;
        Ok(store)
    }

    fn init_tables(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let schema = include_str!("../../db/schema_sqlite.sql");
        conn.execute_batch(schema)?;
        Ok(())
    }
}

#[derive(prost::Message)]
struct TreeEntryList {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<TreeEntry>,
}

#[async_trait]
impl Store for SqliteStore {
    async fn is_repo_registered(&self, repo_id: &str) -> bool {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT 1 FROM repos WHERE repo_id = ?1")
                .ok()?;
            stmt.exists(params![repo_id]).ok()
        })
        .await
        .ok()
        .flatten()
        .unwrap_or(false)
    }

    async fn register_repo(&self, repo_id: String, name: Option<String>) {
        let conn = self.conn.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR IGNORE INTO repos (repo_id, name) VALUES (?1, ?2)",
                params![repo_id, name],
            );
        })
        .await;
    }

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> Option<Commit> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let commit_id = commit_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM commits WHERE repo_id = ?1 AND commit_id = ?2")
                .ok()?;
            let data: Vec<u8> = stmt
                .query_row(params![repo_id, commit_id], |row| row.get(0))
                .ok()?;
            Commit::decode(data.as_slice()).ok()
        })
        .await
        .ok()
        .flatten()
    }

    async fn put_commit(&self, repo_id: String, commit_id: Vec<u8>, commit: Commit) {
        let conn = self.conn.clone();
        let mut buf = Vec::new();
        commit.encode(&mut buf).unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO commits (repo_id, commit_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, commit_id, buf],
            );
        })
        .await;
    }

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> Option<Vec<TreeEntry>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let tree_id = tree_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM trees WHERE repo_id = ?1 AND tree_id = ?2")
                .ok()?;
            let data: Vec<u8> = stmt
                .query_row(params![repo_id, tree_id], |row| row.get(0))
                .ok()?;
            let list = TreeEntryList::decode(data.as_slice()).ok()?;
            Some(list.entries)
        })
        .await
        .ok()
        .flatten()
    }

    async fn put_tree(&self, repo_id: String, tree_id: Vec<u8>, entries: Vec<TreeEntry>) {
        let conn = self.conn.clone();
        let list = TreeEntryList { entries };
        let mut buf = Vec::new();
        list.encode(&mut buf).unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO trees (repo_id, tree_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, tree_id, buf],
            );
        })
        .await;
    }

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> Option<Vec<u8>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let file_id = file_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM files WHERE repo_id = ?1 AND file_id = ?2")
                .ok()?;
            stmt.query_row(params![repo_id, file_id], |row| row.get(0))
                .ok()
        })
        .await
        .ok()
        .flatten()
    }

    async fn put_file(&self, repo_id: String, file_id: Vec<u8>, content: Vec<u8>) {
        let conn = self.conn.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO files (repo_id, file_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, file_id, content],
            );
        })
        .await;
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> Option<Operation> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let op_id = op_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM operations WHERE repo_id = ?1 AND op_id = ?2")
                .ok()?;
            let data: Vec<u8> = stmt
                .query_row(params![repo_id, op_id], |row| row.get(0))
                .ok()?;
            Operation::decode(data.as_slice()).ok()
        })
        .await
        .ok()
        .flatten()
    }

    async fn put_operation(&self, repo_id: String, op_id: Vec<u8>, op: Operation) {
        let conn = self.conn.clone();
        let mut buf = Vec::new();
        op.encode(&mut buf).unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO operations (repo_id, op_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, op_id, buf],
            );
        })
        .await;
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> Option<View> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let view_id = view_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM views WHERE repo_id = ?1 AND view_id = ?2")
                .ok()?;
            let data: Vec<u8> = stmt
                .query_row(params![repo_id, view_id], |row| row.get(0))
                .ok()?;
            View::decode(data.as_slice()).ok()
        })
        .await
        .ok()
        .flatten()
    }

    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) {
        let conn = self.conn.clone();
        let mut buf = Vec::new();
        view.encode(&mut buf).unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(
                "INSERT OR REPLACE INTO views (repo_id, view_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, view_id, buf],
            );
        })
        .await;
    }

    async fn get_op_heads(&self, repo_id: &str) -> Option<Vec<Vec<u8>>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT op_id FROM op_heads WHERE repo_id = ?1")
                .ok()?;
            let rows = stmt.query_map(params![repo_id], |row| row.get(0)).ok()?;
            let mut heads = Vec::new();
            for r in rows {
                if let Ok(id) = r {
                    heads.push(id);
                }
            }
            if heads.is_empty() {
                None
            } else {
                Some(heads)
            }
        })
        .await
        .ok()
        .flatten()
    }

    async fn update_op_heads(
        &self,
        repo_id: String,
        old_ids: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> Vec<Vec<u8>> {
        let conn = self.conn.clone();
        let old_ids = old_ids.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn.transaction().ok()?;
            for old in &old_ids {
                let _ = tx.execute(
                    "DELETE FROM op_heads WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, old],
                );
            }
            let _ = tx.execute(
                "INSERT OR REPLACE INTO op_heads (repo_id, op_id) VALUES (?1, ?2)",
                params![repo_id, new_id],
            );
            let _ = tx.commit();

            let mut stmt = conn
                .prepare("SELECT op_id FROM op_heads WHERE repo_id = ?1")
                .ok()?;
            let rows = stmt.query_map(params![repo_id], |row| row.get(0)).ok()?;
            let mut heads = Vec::new();
            for r in rows {
                if let Ok(id) = r {
                    heads.push(id);
                }
            }
            Some(heads)
        })
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sqlite_file_write_success() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();
        store.register_repo(repo_id.clone(), None).await;

        let file_id = vec![1, 2, 3, 4];
        let content = b"hello file content".to_vec();

        store.put_file(repo_id.clone(), file_id.clone(), content.clone()).await;

        let read = store.get_file(&repo_id, &file_id).await;
        assert_eq!(read, Some(content));
    }
}
