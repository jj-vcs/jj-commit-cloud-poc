use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use prost::Message;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::{SpannerDialect, SqlDialect, SqliteDialect, Store};

#[derive(Clone)]
pub struct SqlStore<D: SqlDialect> {
    pub(crate) conn: Arc<Mutex<Connection>>,
    pub(crate) dialect: D,
}

pub type SqliteStore = SqlStore<SqliteDialect>;
pub type SpannerStore = SqlStore<SpannerDialect>;

impl SqlStore<SqliteDialect> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            dialect: SqliteDialect,
        };
        store.init_tables()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            dialect: SqliteDialect,
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

impl SqlStore<SpannerDialect> {
    pub fn open_spanner(_db_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            dialect: SpannerDialect,
        };
        let schema = include_str!("../../db/schema_sqlite.sql");
        store.conn.lock().unwrap().execute_batch(schema)?;
        Ok(store)
    }
}

#[derive(prost::Message)]
struct TreeEntryList {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<TreeEntry>,
}

#[async_trait]
impl<D: SqlDialect> Store for SqlStore<D> {
    async fn is_repo_registered(&self, repo_id: &str) -> bool {
        let conn = self.conn.clone();
        let query = self.dialect.is_repo_registered_query();
        let repo_id = repo_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(query).ok()?;
            stmt.exists(params![repo_id]).ok()
        })
        .await
        .ok()
        .flatten()
        .unwrap_or(false)
    }

    async fn register_repo(&self, repo_id: String, name: Option<String>) {
        let conn = self.conn.clone();
        let query = self.dialect.register_repo_query();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(query, params![repo_id, name]);
        })
        .await;
    }

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> Option<Commit> {
        let conn = self.conn.clone();
        let query = self.dialect.get_commit_query();
        let repo_id = repo_id.to_string();
        let commit_id = commit_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(query).ok()?;
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
        let query = self.dialect.put_commit_query();
        let mut buf = Vec::new();
        commit.encode(&mut buf).unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(query, params![repo_id, commit_id, buf]);
        })
        .await;
    }

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> Option<Vec<TreeEntry>> {
        let conn = self.conn.clone();
        let query = self.dialect.get_tree_query();
        let repo_id = repo_id.to_string();
        let tree_id = tree_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(query).ok()?;
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
        let query = self.dialect.put_tree_query();
        let list = TreeEntryList { entries };
        let mut buf = Vec::new();
        list.encode(&mut buf).unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(query, params![repo_id, tree_id, buf]);
        })
        .await;
    }

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> Option<Vec<u8>> {
        let conn = self.conn.clone();
        let query = self.dialect.get_file_query();
        let repo_id = repo_id.to_string();
        let file_id = file_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(query).ok()?;
            stmt.query_row(params![repo_id, file_id], |row| row.get(0))
                .ok()
        })
        .await
        .ok()
        .flatten()
    }

    async fn put_file(&self, repo_id: String, file_id: Vec<u8>, content: Vec<u8>) {
        let conn = self.conn.clone();
        let query = self.dialect.put_file_query();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(query, params![repo_id, file_id, content]);
        })
        .await;
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> Option<Operation> {
        let conn = self.conn.clone();
        let query = self.dialect.get_operation_query();
        let repo_id = repo_id.to_string();
        let op_id = op_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(query).ok()?;
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
        let query = self.dialect.put_operation_query();
        let mut buf = Vec::new();
        op.encode(&mut buf).unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(query, params![repo_id, op_id, buf]);
        })
        .await;
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> Option<View> {
        let conn = self.conn.clone();
        let query = self.dialect.get_view_query();
        let repo_id = repo_id.to_string();
        let view_id = view_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(query).ok()?;
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
        let query = self.dialect.put_view_query();
        let mut buf = Vec::new();
        view.encode(&mut buf).unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let _ = conn.execute(query, params![repo_id, view_id, buf]);
        })
        .await;
    }

    async fn get_op_heads(&self, repo_id: &str) -> Option<Vec<Vec<u8>>> {
        let conn = self.conn.clone();
        let query = self.dialect.get_op_heads_query();
        let repo_id = repo_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(query).ok()?;
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
        let delete_query = self.dialect.delete_op_head_query();
        let insert_query = self.dialect.insert_op_head_query();
        let select_query = self.dialect.get_op_heads_query();
        let old_ids = old_ids.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn.transaction().ok()?;
            for old in &old_ids {
                let _ = tx.execute(delete_query, params![repo_id, old]);
            }
            let _ = tx.execute(insert_query, params![repo_id, new_id]);
            let _ = tx.commit();

            let mut stmt = conn.prepare(select_query).ok()?;
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
