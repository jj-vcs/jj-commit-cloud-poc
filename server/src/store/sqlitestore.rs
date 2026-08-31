use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use prost::Message;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::{Store, StoreError, StoreResult};

#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
}


// SqliteStore startup and constructor methods return Result<Self, Error>
// while the methods on the Store trait return StoreResult<T>. This will 
// match the startup method return type for the rest of the storage methods. 
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
    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT 1 FROM repos WHERE repo_id = ?1")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            stmt.exists(params![repo_id])
                .map_err(|e| StoreError::Read(e.to_string()))
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn register_repo(&self, repo_id: String, name: Option<String>) -> StoreResult<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR IGNORE INTO repos (repo_id, name) VALUES (?1, ?2)",
                params![repo_id, name],
            )
            .map_err(|e| StoreError::Write(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let commit_id = commit_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM commits WHERE repo_id = ?1 AND commit_id = ?2")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Option<Vec<u8>> = stmt
                .query_row(params![repo_id, commit_id], |row| row.get(0))
                .optional()
                .map_err(|e| StoreError::Read(e.to_string()))?;
            match data {
                Some(bytes) => Ok(Some(
                    Commit::decode(bytes.as_slice())
                        .map_err(|e| StoreError::Decode(e.to_string()))?,
                )),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn put_commit(
        &self,
        repo_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()> {
        let conn = self.conn.clone();
        let mut buf = Vec::new();
        commit
            .encode(&mut buf)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO commits (repo_id, commit_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, commit_id, buf],
            )
            .map_err(|e| StoreError::Write(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> StoreResult<Option<Vec<TreeEntry>>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let tree_id = tree_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM trees WHERE repo_id = ?1 AND tree_id = ?2")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Option<Vec<u8>> = stmt
                .query_row(params![repo_id, tree_id], |row| row.get(0))
                .optional()
                .map_err(|e| StoreError::Read(e.to_string()))?;
            match data {
                Some(bytes) => {
                    let list = TreeEntryList::decode(bytes.as_slice())
                        .map_err(|e| StoreError::Decode(e.to_string()))?;
                    Ok(Some(list.entries))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn put_tree(
        &self,
        repo_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()> {
        let conn = self.conn.clone();
        let list = TreeEntryList { entries };
        let mut buf = Vec::new();
        list.encode(&mut buf)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO trees (repo_id, tree_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, tree_id, buf],
            )
            .map_err(|e| StoreError::Write(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let file_id = file_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM files WHERE repo_id = ?1 AND file_id = ?2")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            stmt.query_row(params![repo_id, file_id], |row| row.get(0))
                .optional()
                .map_err(|e| StoreError::Read(e.to_string()))
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn put_file(
        &self,
        repo_id: String,
        file_id: Vec<u8>,
        content: Vec<u8>,
    ) -> StoreResult<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO files (repo_id, file_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, file_id, content],
            )
            .map_err(|e| StoreError::Write(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let op_id = op_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM operations WHERE repo_id = ?1 AND op_id = ?2")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Option<Vec<u8>> = stmt
                .query_row(params![repo_id, op_id], |row| row.get(0))
                .optional()
                .map_err(|e| StoreError::Read(e.to_string()))?;
            match data {
                Some(bytes) => Ok(Some(
                    Operation::decode(bytes.as_slice())
                        .map_err(|e| StoreError::Decode(e.to_string()))?,
                )),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn put_operation(
        &self,
        repo_id: String,
        op_id: Vec<u8>,
        op: Operation,
    ) -> StoreResult<()> {
        let conn = self.conn.clone();
        let mut buf = Vec::new();
        op.encode(&mut buf)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO operations (repo_id, op_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, op_id, buf],
            )
            .map_err(|e| StoreError::Write(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let view_id = view_id.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT data FROM views WHERE repo_id = ?1 AND view_id = ?2")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Option<Vec<u8>> = stmt
                .query_row(params![repo_id, view_id], |row| row.get(0))
                .optional()
                .map_err(|e| StoreError::Read(e.to_string()))?;
            match data {
                Some(bytes) => Ok(Some(
                    View::decode(bytes.as_slice())
                        .map_err(|e| StoreError::Decode(e.to_string()))?,
                )),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) -> StoreResult<()> {
        let conn = self.conn.clone();
        let mut buf = Vec::new();
        view.encode(&mut buf)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO views (repo_id, view_id, data) VALUES (?1, ?2, ?3)",
                params![repo_id, view_id, buf],
            )
            .map_err(|e| StoreError::Write(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT op_id FROM op_heads WHERE repo_id = ?1")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let rows = stmt
                .query_map(params![repo_id], |row| row.get(0))
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let mut heads = Vec::new();
            for r in rows {
                heads.push(r.map_err(|e| StoreError::Read(e.to_string()))?);
            }
            if heads.is_empty() {
                Ok(None)
            } else {
                Ok(Some(heads))
            }
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn update_op_heads(
        &self,
        repo_id: String,
        old_ids: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let conn = self.conn.clone();
        let old_ids = old_ids.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap();
            let tx = conn
                .transaction()
                .map_err(|e| StoreError::Write(e.to_string()))?;
            for old in &old_ids {
                tx.execute(
                    "DELETE FROM op_heads WHERE repo_id = ?1 AND op_id = ?2",
                    params![repo_id, old],
                )
                .map_err(|e| StoreError::Write(e.to_string()))?;
            }
            tx.execute(
                "INSERT OR REPLACE INTO op_heads (repo_id, op_id) VALUES (?1, ?2)",
                params![repo_id, new_id],
            )
            .map_err(|e| StoreError::Write(e.to_string()))?;
            tx.commit()
                .map_err(|e| StoreError::Write(e.to_string()))?;

            let mut stmt = conn
                .prepare("SELECT op_id FROM op_heads WHERE repo_id = ?1")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let rows = stmt
                .query_map(params![repo_id], |row| row.get(0))
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let mut heads = Vec::new();
            for r in rows {
                heads.push(r.map_err(|e| StoreError::Read(e.to_string()))?);
            }
            Ok(heads)
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        let user = user.to_string();
        let workspace_name = workspace_name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT commit_id, operation_id, tree_id FROM workspaces WHERE repo_id = ?1 AND user = ?2 AND workspace_name = ?3")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let res: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = stmt
                .query_row(params![repo_id, user, workspace_name], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .optional()
                .map_err(|e| StoreError::Read(e.to_string()))?;
            Ok(res.map(|(commit_id, operation_id, tree_id)| WorkspaceState {
                repo_id,
                user,
                workspace_name,
                commit_id,
                operation_id,
                tree_id,
            }))
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn put_workspace(&self, workspace: WorkspaceState) -> StoreResult<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO workspaces (repo_id, user, workspace_name, commit_id, operation_id, tree_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    workspace.repo_id,
                    workspace.user,
                    workspace.workspace_name,
                    workspace.commit_id,
                    workspace.operation_id,
                    workspace.tree_id,
                ],
            )
            .map_err(|e| StoreError::Write(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }

    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        let conn = self.conn.clone();
        let repo_id = repo_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT user, workspace_name, commit_id, operation_id, tree_id FROM workspaces WHERE repo_id = ?1")
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let rows = stmt
                .query_map(params![repo_id], |row| {
                    Ok(WorkspaceState {
                        repo_id: repo_id.clone(),
                        user: row.get(0)?,
                        workspace_name: row.get(1)?,
                        commit_id: row.get(2)?,
                        operation_id: row.get(3)?,
                        tree_id: row.get(4)?,
                    })
                })
                .map_err(|e| StoreError::Read(e.to_string()))?;
            let mut workspaces = Vec::new();
            for r in rows {
                workspaces.push(r.map_err(|e| StoreError::Read(e.to_string()))?);
            }
            Ok(workspaces)
        })
        .await
        .map_err(|e| StoreError::Task(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sqlite_file_write_success() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();
        store.register_repo(repo_id.clone(), None).await.unwrap();

        let file_id = vec![1, 2, 3, 4];
        let content = b"hello file content".to_vec();

        store.put_file(repo_id.clone(), file_id.clone(), content.clone()).await.unwrap();

        let read = store.get_file(&repo_id, &file_id).await.unwrap();
        assert_eq!(read, Some(content));
    }
}
