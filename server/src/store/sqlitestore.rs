use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use prost::Message;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::{Store, StoreError, StoreResult};

// Path to the SQLite database schema file relative to this source file.
pub const SQLITE_DATABASE_SCHEMA_PATH: &str = "../../db/schema_sqlite.sql";
// SQLite database schema definition embedded from the schema SQL file.
pub const SQLITE_DATABASE_SCHEMA: &str = include_str!("../../db/schema_sqlite.sql");

#[derive(Clone)]
pub struct SqliteStore {
    // Stored path to the SQLite database file on disk.
    // We use Option<PathBuf> because the store can either be backed by a real file on disk
    // (Some(path)), or running completely in RAM for fast unit testing (None).
    // Storing this path allows the server to know where its database file lives for diagnostics,
    // logging, and re-opening/reconnecting if the database connection drops or encounters an error.
    path: Option<std::path::PathBuf>,
    conn: Arc<Mutex<Connection>>,
}

// SqliteStore startup and constructor methods return Result<Self, Error>
// while the methods on the Store trait return StoreResult<T>. This will 
// match the startup method return type for the rest of the storage methods. 
impl SqliteStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let path_buf = path.as_ref().to_path_buf();
        let conn = Connection::open(&path_buf)?;
        let store = Self {
            path: Some(path_buf),
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init_tables()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            path: None,
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init_tables()?;
        Ok(store)
    }

    // Returns the path to the database file on disk, or None if running in memory.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    // Reconnects to the SQLite database using the stored path if the connection drops or fails.
    pub fn reconnect(&self) -> Result<(), rusqlite::Error> {
        let new_conn = match &self.path {
            Some(path) => Connection::open(path)?,
            None => Connection::open_in_memory()?,
        };
        new_conn.execute_batch(SQLITE_DATABASE_SCHEMA)?;
        let mut conn = self.conn.lock().unwrap();
        *conn = new_conn;
        Ok(())
    }

    #[cfg(test)]
    pub fn open_readonly<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let path_buf = path.as_ref().to_path_buf();
        let conn = Connection::open_with_flags(&path_buf, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Ok(Self {
            path: Some(path_buf),
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn init_tables(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(SQLITE_DATABASE_SCHEMA)?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn sample_commit() -> Commit {
        Commit {
            commit_id: vec![1, 2, 3],
            change_id: vec![4, 5, 6],
            parent_commit_ids: vec![vec![7, 8, 9]],
            root_tree_id: vec![vec![10, 11, 12]],
            description: "test commit description".to_string(),
            author: Some(Signature {
                name: "Author Name".to_string(),
                email: "author@example.com".to_string(),
                timestamp: Some(Timestamp {
                    millis_since_epoch: 123456789,
                    tz_offset: 0,
                }),
            }),
            committer: Some(Signature {
                name: "Committer Name".to_string(),
                email: "committer@example.com".to_string(),
                timestamp: Some(Timestamp {
                    millis_since_epoch: 123456790,
                    tz_offset: 0,
                }),
            }),
            predecessors: vec![vec![13, 14, 15]],
            conflict_labels: vec!["label1".to_string()],
            secure_sig: None,
        }
    }

    fn sample_tree_entries() -> Vec<TreeEntry> {
        vec![
            TreeEntry {
                name: "file1.txt".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: vec![1, 2, 3],
                        executable: false,
                        copy_id: vec![],
                    })),
                }),
            },
            TreeEntry {
                name: "dir1".to_string(),
                value: Some(TreeValue {
                    value: Some(tree_value::Value::TreeId(vec![4, 5, 6])),
                }),
            },
        ]
    }

    fn sample_operation() -> Operation {
        Operation {
            view_id: vec![1, 2, 3],
            parents: vec![vec![4, 5, 6]],
            metadata: Some(OperationMetadata {
                start_time_millis: 1000,
                end_time_millis: 2000,
                description: "test operation".to_string(),
                is_snapshot: false,
                workspace_name: Some("default".to_string()),
                hostname: "localhost".to_string(),
                username: "user".to_string(),
                attributes: HashMap::new(),
            }),
            commit_predecessors: vec![],
            commit_predecessors_set: false,
        }
    }

    fn sample_view() -> View {
        let mut wc_commit_ids = HashMap::new();
        wc_commit_ids.insert("default".to_string(), vec![1, 2, 3]);

        let mut local_bookmarks = HashMap::new();
        local_bookmarks.insert(
            "main".to_string(),
            RefTarget {
                removes: vec![],
                adds: vec![RefTargetTerm {
                    commit_id: vec![4, 5, 6],
                }],
            },
        );

        let mut remote_bookmarks = HashMap::new();
        remote_bookmarks.insert(
            "origin/main".to_string(),
            RemoteRef {
                target: Some(RefTarget {
                    removes: vec![],
                    adds: vec![RefTargetTerm {
                        commit_id: vec![7, 8, 9],
                    }],
                }),
                is_tracked: true,
            },
        );

        View {
            head_ids: vec![vec![1, 2], vec![3, 4]],
            wc_commit_ids,
            local_bookmarks,
            remote_bookmarks,
        }
    }

    fn create_readonly_store() -> (tempfile::TempDir, SqliteStore) {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("readonly_test.db");
        // Create database and initialize schema tables
        let init_store = SqliteStore::open(&file_path).unwrap();
        drop(init_store);

        let readonly_store = SqliteStore::open_readonly(&file_path).unwrap();
        (temp_dir, readonly_store)
    }

    #[tokio::test]
    async fn test_sqlite_register_and_check_repo_succeeds() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();

        assert_eq!(store.is_repo_registered(&repo_id).await.unwrap(), false);
        store.register_repo(repo_id.clone(), Some("my-repo".to_string())).await.unwrap();
        assert_eq!(store.is_repo_registered(&repo_id).await.unwrap(), true);
        assert!(store.register_repo(repo_id.clone(), None).await.is_ok());
    }

    #[tokio::test]
    async fn test_sqlite_put_and_read_file_succeeds() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();
        store.register_repo(repo_id.clone(), None).await.unwrap();

        let file_id = vec![1, 2, 3, 4];
        let content = b"hello file content".to_vec();

        assert_eq!(store.get_file(&repo_id, &file_id).await.unwrap(), None);
        store.put_file(repo_id.clone(), file_id.clone(), content.clone()).await.unwrap();
        let read = store.get_file(&repo_id, &file_id).await.unwrap();
        assert_eq!(read, Some(content));
    }

    #[tokio::test]
    async fn test_sqlite_put_and_read_commit_succeeds() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();
        store.register_repo(repo_id.clone(), None).await.unwrap();

        let commit = sample_commit();
        let commit_id = commit.commit_id.clone();

        assert_eq!(store.get_commit(&repo_id, &commit_id).await.unwrap(), None);
        store.put_commit(repo_id.clone(), commit_id.clone(), commit.clone()).await.unwrap();
        let read = store.get_commit(&repo_id, &commit_id).await.unwrap();
        assert_eq!(read, Some(commit));
    }

    #[tokio::test]
    async fn test_sqlite_put_and_read_tree_succeeds() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();
        store.register_repo(repo_id.clone(), None).await.unwrap();

        let tree_entries = sample_tree_entries();
        let tree_id = vec![10, 20, 30];

        assert_eq!(store.get_tree(&repo_id, &tree_id).await.unwrap(), None);
        store.put_tree(repo_id.clone(), tree_id.clone(), tree_entries.clone()).await.unwrap();
        let read = store.get_tree(&repo_id, &tree_id).await.unwrap();
        assert_eq!(read, Some(tree_entries));
    }

    #[tokio::test]
    async fn test_sqlite_put_and_read_operation_succeeds() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();
        store.register_repo(repo_id.clone(), None).await.unwrap();

        let op = sample_operation();
        let op_id = vec![40, 50, 60];

        assert_eq!(store.get_operation(&repo_id, &op_id).await.unwrap(), None);
        store.put_operation(repo_id.clone(), op_id.clone(), op.clone()).await.unwrap();
        let read = store.get_operation(&repo_id, &op_id).await.unwrap();
        assert_eq!(read, Some(op));
    }

    #[tokio::test]
    async fn test_sqlite_put_and_read_view_succeeds() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();
        store.register_repo(repo_id.clone(), None).await.unwrap();

        let view = sample_view();
        let view_id = vec![70, 80, 90];

        assert_eq!(store.get_view(&repo_id, &view_id).await.unwrap(), None);
        store.put_view(repo_id.clone(), view_id.clone(), view.clone()).await.unwrap();
        let read = store.get_view(&repo_id, &view_id).await.unwrap();
        assert_eq!(read, Some(view));
    }

    #[tokio::test]
    async fn test_sqlite_put_and_read_op_heads_succeeds() {
        let store = SqliteStore::in_memory().unwrap();
        let repo_id = "test-repo".to_string();
        store.register_repo(repo_id.clone(), None).await.unwrap();

        assert_eq!(store.get_op_heads(&repo_id).await.unwrap(), None);
        let head1 = vec![101];
        let heads = store.update_op_heads(repo_id.clone(), &[], head1.clone()).await.unwrap();
        assert_eq!(heads, vec![head1.clone()]);
        assert_eq!(store.get_op_heads(&repo_id).await.unwrap(), Some(vec![head1.clone()]));

        let head2 = vec![102];
        let heads = store.update_op_heads(repo_id.clone(), &[head1], head2.clone()).await.unwrap();
        assert_eq!(heads, vec![head2.clone()]);
        assert_eq!(store.get_op_heads(&repo_id).await.unwrap(), Some(vec![head2]));
    }

    #[tokio::test]
    async fn test_sqlite_path_getter() {
        let mem_store = SqliteStore::in_memory().unwrap();
        assert_eq!(mem_store.path(), None);

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.db");
        let file_store = SqliteStore::open(&file_path).unwrap();
        assert_eq!(file_store.path(), Some(file_path.as_path()));
    }

    #[tokio::test]
    async fn test_sqlite_reconnect_preserves_disk_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("reconnect_test.db");
        let store = SqliteStore::open(&file_path).unwrap();

        let repo_id = "test-reconnect-repo".to_string();
        store.register_repo(repo_id.clone(), None).await.unwrap();

        let file_id = vec![10, 20, 30];
        let content = b"persisted content".to_vec();
        store.put_file(repo_id.clone(), file_id.clone(), content.clone()).await.unwrap();

        // Reconnect to the database using the stored path
        assert!(store.reconnect().is_ok());

        // Verify data is still readable after reconnection
        let read = store.get_file(&repo_id, &file_id).await.unwrap();
        assert_eq!(read, Some(content));
    }

    // 1. Write error tests
    #[tokio::test]
    async fn test_sqlite_register_repo_fails_on_readonly_store() {
        let (_dir, store) = create_readonly_store();
        let err = store.register_repo("repo1".to_string(), None).await.unwrap_err();
        match &err {
            StoreError::Write(msg) => {
                assert_eq!(msg, "attempt to write a readonly database");
            }
            _ => panic!("expected StoreError::Write, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage write error: attempt to write a readonly database");
    }

    #[tokio::test]
    async fn test_sqlite_put_commit_fails_on_readonly_store() {
        let (_dir, store) = create_readonly_store();
        let err = store.put_commit("repo1".to_string(), vec![1u8], sample_commit()).await.unwrap_err();
        match &err {
            StoreError::Write(msg) => {
                assert_eq!(msg, "attempt to write a readonly database");
            }
            _ => panic!("expected StoreError::Write, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage write error: attempt to write a readonly database");
    }

    #[tokio::test]
    async fn test_sqlite_put_tree_fails_on_readonly_store() {
        let (_dir, store) = create_readonly_store();
        let err = store.put_tree("repo1".to_string(), vec![1u8], sample_tree_entries()).await.unwrap_err();
        match &err {
            StoreError::Write(msg) => {
                assert_eq!(msg, "attempt to write a readonly database");
            }
            _ => panic!("expected StoreError::Write, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage write error: attempt to write a readonly database");
    }

    #[tokio::test]
    async fn test_sqlite_put_file_fails_on_readonly_store() {
        let (_dir, store) = create_readonly_store();
        let err = store.put_file("repo1".to_string(), vec![1u8], b"data".to_vec()).await.unwrap_err();
        match &err {
            StoreError::Write(msg) => {
                assert_eq!(msg, "attempt to write a readonly database");
            }
            _ => panic!("expected StoreError::Write, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage write error: attempt to write a readonly database");
    }

    #[tokio::test]
    async fn test_sqlite_put_operation_fails_on_readonly_store() {
        let (_dir, store) = create_readonly_store();
        let err = store.put_operation("repo1".to_string(), vec![1u8], sample_operation()).await.unwrap_err();
        match &err {
            StoreError::Write(msg) => {
                assert_eq!(msg, "attempt to write a readonly database");
            }
            _ => panic!("expected StoreError::Write, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage write error: attempt to write a readonly database");
    }

    #[tokio::test]
    async fn test_sqlite_put_view_fails_on_readonly_store() {
        let (_dir, store) = create_readonly_store();
        let err = store.put_view("repo1".to_string(), vec![1u8], sample_view()).await.unwrap_err();
        match &err {
            StoreError::Write(msg) => {
                assert_eq!(msg, "attempt to write a readonly database");
            }
            _ => panic!("expected StoreError::Write, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage write error: attempt to write a readonly database");
    }

    #[tokio::test]
    async fn test_sqlite_update_op_heads_fails_on_readonly_store() {
        let (_dir, store) = create_readonly_store();
        let err = store.update_op_heads("repo1".to_string(), &[], vec![1u8]).await.unwrap_err();
        match &err {
            StoreError::Write(msg) => {
                assert_eq!(msg, "attempt to write a readonly database");
            }
            _ => panic!("expected StoreError::Write, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage write error: attempt to write a readonly database");
    }

    // 2. Read error tests (missing tables)
    #[tokio::test]
    async fn test_sqlite_is_repo_registered_fails_on_missing_table() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute("DROP TABLE repos", []).unwrap();
        let err = store.is_repo_registered("r1").await.unwrap_err();
        match &err {
            StoreError::Read(msg) => {
                assert_eq!(msg, "no such table: repos");
            }
            _ => panic!("expected StoreError::Read, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage read error: no such table: repos");
    }

    #[tokio::test]
    async fn test_sqlite_get_commit_fails_on_missing_table() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute("DROP TABLE commits", []).unwrap();
        let err = store.get_commit("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Read(msg) => {
                assert_eq!(msg, "no such table: commits");
            }
            _ => panic!("expected StoreError::Read, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage read error: no such table: commits");
    }

    #[tokio::test]
    async fn test_sqlite_get_tree_fails_on_missing_table() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute("DROP TABLE trees", []).unwrap();
        let err = store.get_tree("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Read(msg) => {
                assert_eq!(msg, "no such table: trees");
            }
            _ => panic!("expected StoreError::Read, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage read error: no such table: trees");
    }

    #[tokio::test]
    async fn test_sqlite_get_file_fails_on_missing_table() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute("DROP TABLE files", []).unwrap();
        let err = store.get_file("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Read(msg) => {
                assert_eq!(msg, "no such table: files");
            }
            _ => panic!("expected StoreError::Read, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage read error: no such table: files");
    }

    #[tokio::test]
    async fn test_sqlite_get_operation_fails_on_missing_table() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute("DROP TABLE operations", []).unwrap();
        let err = store.get_operation("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Read(msg) => {
                assert_eq!(msg, "no such table: operations");
            }
            _ => panic!("expected StoreError::Read, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage read error: no such table: operations");
    }

    #[tokio::test]
    async fn test_sqlite_get_view_fails_on_missing_table() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute("DROP TABLE views", []).unwrap();
        let err = store.get_view("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Read(msg) => {
                assert_eq!(msg, "no such table: views");
            }
            _ => panic!("expected StoreError::Read, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage read error: no such table: views");
    }

    #[tokio::test]
    async fn test_sqlite_get_op_heads_fails_on_missing_table() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute("DROP TABLE op_heads", []).unwrap();
        let err = store.get_op_heads("r1").await.unwrap_err();
        match &err {
            StoreError::Read(msg) => {
                assert_eq!(msg, "no such table: op_heads");
            }
            _ => panic!("expected StoreError::Read, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage read error: no such table: op_heads");
    }

    // 3. Decode error tests (corrupted data)
    #[tokio::test]
    async fn test_sqlite_get_commit_fails_on_corrupted_data() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute(
            "INSERT INTO commits (repo_id, commit_id, data) VALUES (?1, ?2, ?3)",
            params!["r1", vec![1u8], vec![0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8]],
        ).unwrap();
        let err = store.get_commit("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Decode(msg) => {
                assert_eq!(msg, "failed to decode Protobuf message: invalid varint");
            }
            _ => panic!("expected StoreError::Decode, got {err:?}"),
        }
        assert_eq!(err.to_string(), "stored data is corrupt: failed to decode Protobuf message: invalid varint");
    }

    #[tokio::test]
    async fn test_sqlite_get_tree_fails_on_corrupted_data() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute(
            "INSERT INTO trees (repo_id, tree_id, data) VALUES (?1, ?2, ?3)",
            params!["r1", vec![1u8], vec![0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8]],
        ).unwrap();
        let err = store.get_tree("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Decode(msg) => {
                assert_eq!(msg, "failed to decode Protobuf message: invalid varint");
            }
            _ => panic!("expected StoreError::Decode, got {err:?}"),
        }
        assert_eq!(err.to_string(), "stored data is corrupt: failed to decode Protobuf message: invalid varint");
    }

    #[tokio::test]
    async fn test_sqlite_get_operation_fails_on_corrupted_data() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute(
            "INSERT INTO operations (repo_id, op_id, data) VALUES (?1, ?2, ?3)",
            params!["r1", vec![1u8], vec![0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8]],
        ).unwrap();
        let err = store.get_operation("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Decode(msg) => {
                assert_eq!(msg, "failed to decode Protobuf message: invalid varint");
            }
            _ => panic!("expected StoreError::Decode, got {err:?}"),
        }
        assert_eq!(err.to_string(), "stored data is corrupt: failed to decode Protobuf message: invalid varint");
    }

    #[tokio::test]
    async fn test_sqlite_get_view_fails_on_corrupted_data() {
        let store = SqliteStore::in_memory().unwrap();
        store.conn.lock().unwrap().execute(
            "INSERT INTO views (repo_id, view_id, data) VALUES (?1, ?2, ?3)",
            params!["r1", vec![1u8], vec![0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8]],
        ).unwrap();
        let err = store.get_view("r1", &[1]).await.unwrap_err();
        match &err {
            StoreError::Decode(msg) => {
                assert_eq!(msg, "failed to decode Protobuf message: invalid varint");
            }
            _ => panic!("expected StoreError::Decode, got {err:?}"),
        }
        assert_eq!(err.to_string(), "stored data is corrupt: failed to decode Protobuf message: invalid varint");
    }

    // 4. Encode error test
    #[test]
    fn test_sqlite_store_encode_error() {
        let err = StoreError::Encode("test protobuf encode error".to_string());
        match &err {
            StoreError::Encode(msg) => {
                assert_eq!(msg, "test protobuf encode error");
            }
            _ => panic!("expected StoreError::Encode, got {err:?}"),
        }
        assert_eq!(err.to_string(), "storage encode error: test protobuf encode error");
    }
}
