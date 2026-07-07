// Copyright 2024-2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::path::Path;
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use thiserror::Error;
use prost::Message as _;

use jj_lib::backend::{BackendInitError, BackendLoadError};
use jj_lib::content_hash::blake2b_hash;
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_store::{
    OpStore, OpStoreError, OpStoreResult, Operation, OperationId, RootOperationData, View, ViewId,
};
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};
use crate::third_party::proto_helpers::{
    operation_from_proto, operation_to_proto, view_from_proto, view_to_proto, PostDecodeError,
};

#[derive(Debug, Error)]
pub(crate) enum SqliteOpStoreError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Proto decode error: {0}")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error("Post decode error: {0}")]
    PostDecode(#[from] PostDecodeError),
    #[error("Metadata missing: project_id not found in database. Is the backend initialized?")]
    MetadataMissing,
}

impl From<SqliteOpStoreError> for OpStoreError {
    fn from(err: SqliteOpStoreError) -> Self {
        match err {
            SqliteOpStoreError::Database(rusqlite::Error::QueryReturnedNoRows) => {
                // This might not be the best mapping, but we handle ObjectNotFound separately
                OpStoreError::Other(Box::new(err))
            }
            _ => OpStoreError::Other(Box::new(err)),
        }
    }
}

pub struct SqliteOpStore {
    connection: Mutex<Connection>,
    project_id: Vec<u8>,
    root_data: RootOperationData,
    root_operation_id: OperationId,
    root_view_id: ViewId,
}

impl Debug for SqliteOpStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteOpStore")
            .field("project_id", &self.project_id)
            .finish()
    }
}

impl SqliteOpStore {
    pub(crate) fn name() -> &'static str {
        "sqlite"
    }

    fn db_path(store_path: &Path) -> std::path::PathBuf {
        // In classic mode, store_path is .jj/repo/op_store/ and DB is at .jj/repo/store/store.db.
        // In unified mode, store_path is .jj/repo/ and DB is at .jj/repo/store.db.
        if store_path.file_name().map_or(false, |n| n == "op_store" || n == "op_heads") {
            store_path
                .parent()
                .unwrap()
                .join("store")
                .join("store.db")
        } else {
            store_path.join("store.db")
        }
    }

    pub fn init(
        store_path: &Path,
        root_data: RootOperationData,
    ) -> Result<Self, BackendInitError> {
        let db_path = Self::db_path(store_path);
        let conn = Connection::open(&db_path)
            .map_err(|err| BackendInitError(err.into()))?;

        // Enable WAL mode for concurrency
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|err| BackendInitError(err.into()))?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|err| BackendInitError(err.into()))?;

        // ...
        // ... (Keep the rest of the implementation)
        // ... (We will use replace_file_content to only target the signatures if needed)

        // Create tables if they don't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS operations (
                project_id BLOB NOT NULL,
                op_id BLOB NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (project_id, op_id)
            )",
            [],
        )
        .map_err(|err| BackendInitError(err.into()))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS views (
                project_id BLOB NOT NULL,
                view_id BLOB NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (project_id, view_id)
            )",
            [],
        )
        .map_err(|err| BackendInitError(err.into()))?;

        // Read project_id from metadata (must have been created by SqliteBackend::init)
        let project_id: Vec<u8> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = 'project_id'",
                [],
                |row| row.get(0),
            )
            .map_err(|_| BackendInitError(SqliteOpStoreError::MetadataMissing.into()))?;

        Ok(Self {
            connection: Mutex::new(conn),
            project_id,
            root_data,
            root_operation_id: OperationId::from_bytes(&[0; 64]),
            root_view_id: ViewId::from_bytes(&[0; 64]),
        })
    }

    pub fn load(
        store_path: &Path,
        root_data: RootOperationData,
    ) -> Result<Self, BackendLoadError> {
        let db_path = Self::db_path(store_path);
        let conn = Connection::open(&db_path)
            .map_err(|err| BackendLoadError(err.into()))?;

        // Read project_id
        let project_id: Vec<u8> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = 'project_id'",
                [],
                |row| row.get(0),
            )
            .map_err(|err| BackendLoadError(err.into()))?;

        Ok(Self {
            connection: Mutex::new(conn),
            project_id,
            root_data,
            root_operation_id: OperationId::from_bytes(&[0; 64]),
            root_view_id: ViewId::from_bytes(&[0; 64]),
        })
    }
}

#[async_trait]
impl OpStore for SqliteOpStore {
    fn name(&self) -> &str {
        Self::name()
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        if *id == self.root_view_id {
            return Ok(View::make_root(self.root_data.root_commit_id.clone()));
        }

        let conn = self.connection.lock().unwrap();
        let data: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM views WHERE project_id = ?1 AND view_id = ?2",
                [&self.project_id, id.as_bytes()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| OpStoreError::Other(err.into()))?;

        let Some(buf) = data else {
            return Err(OpStoreError::ObjectNotFound {
                object_type: "view".to_string(),
                hash: id.hex(),
                source: Box::new(rusqlite::Error::QueryReturnedNoRows),
            });
        };

        let proto = jj_lib::protos::simple_op_store::View::decode(&*buf)
            .map_err(|err| OpStoreError::ReadObject {
                object_type: "view".to_string(),
                hash: id.hex(),
                source: Box::new(err),
            })?;

        view_from_proto(proto).map_err(|err| OpStoreError::ReadObject {
            object_type: "view".to_string(),
            hash: id.hex(),
            source: Box::new(err),
        })
    }

    async fn write_view(&self, view: &View) -> OpStoreResult<ViewId> {
        let proto = view_to_proto(view);
        let buf = proto.encode_to_vec();
        let id = ViewId::new(blake2b_hash(view).to_vec());

        let conn = self.connection.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO views (project_id, view_id, data) VALUES (?1, ?2, ?3)",
            [&self.project_id, id.as_bytes(), &buf],
        )
        .map_err(|err| OpStoreError::WriteObject {
            object_type: "view",
            source: Box::new(err),
        })?;

        Ok(id)
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if *id == self.root_operation_id {
            return Ok(Operation::make_root(self.root_view_id.clone()));
        }

        let conn = self.connection.lock().unwrap();
        let data: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM operations WHERE project_id = ?1 AND op_id = ?2",
                [&self.project_id, id.as_bytes()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| OpStoreError::Other(err.into()))?;

        let Some(buf) = data else {
            return Err(OpStoreError::ObjectNotFound {
                object_type: "operation".to_string(),
                hash: id.hex(),
                source: Box::new(rusqlite::Error::QueryReturnedNoRows),
            });
        };

        let proto = jj_lib::protos::simple_op_store::Operation::decode(&*buf)
            .map_err(|err| OpStoreError::ReadObject {
                object_type: "operation".to_string(),
                hash: id.hex(),
                source: Box::new(err),
            })?;

        let mut operation = operation_from_proto(proto).map_err(|err| {
            OpStoreError::ReadObject {
                object_type: "operation".to_string(),
                hash: id.hex(),
                source: Box::new(err),
            }
        })?;

        if operation.parents.is_empty() {
            operation.parents.push(self.root_operation_id.clone());
        }
        Ok(operation)
    }

    async fn write_operation(&self, operation: &Operation) -> OpStoreResult<OperationId> {
        assert!(!operation.parents.is_empty());
        let proto = operation_to_proto(operation);
        let buf = proto.encode_to_vec();
        let id = OperationId::new(blake2b_hash(operation).to_vec());

        let conn = self.connection.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO operations (project_id, op_id, data) VALUES (?1, ?2, ?3)",
            [&self.project_id, id.as_bytes(), &buf],
        )
        .map_err(|err| OpStoreError::WriteObject {
            object_type: "operation",
            source: Box::new(err),
        })?;

        Ok(id)
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        let matches_root = prefix.matches(&self.root_operation_id);
        let hex_prefix = prefix.hex();
        
        if hex_prefix.len() == 128 {
            // Fast path for full-length ID
            let id_bytes = prefix.as_full_bytes().unwrap();
            if matches_root {
                return Ok(PrefixResolution::SingleMatch(self.root_operation_id.clone()));
            }
            let conn = self.connection.lock().unwrap();
            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM operations WHERE project_id = ?1 AND op_id = ?2",
                    [&self.project_id, id_bytes],
                    |_| Ok(true),
                )
                .optional()
                .map_err(|err| OpStoreError::Other(err.into()))?
                .unwrap_or(false);

            if exists {
                return Ok(PrefixResolution::SingleMatch(OperationId::from_bytes(id_bytes)));
            } else {
                return Ok(PrefixResolution::NoMatch);
            }
        }

        let conn = self.connection.lock().unwrap();
        // We query all op_ids for the project and filter in memory because hex prefix
        // matching on blob is tricky in SQLite without custom functions.
        // Since the number of operations in a local repo is typically small (hundreds/thousands),
        // loading the keys is fast.
        let mut stmt = conn
            .prepare("SELECT op_id FROM operations WHERE project_id = ?1")
            .map_err(|err| OpStoreError::Other(err.into()))?;
        
        let rows = stmt
            .query_map([&self.project_id], |row| row.get::<_, Vec<u8>>(0))
            .map_err(|err| OpStoreError::Other(err.into()))?;

        let mut matched = matches_root.then(|| self.root_operation_id.clone());

        for op_id_bytes_res in rows {
            let op_id_bytes = op_id_bytes_res.map_err(|err| OpStoreError::Other(err.into()))?;
            let op_id = OperationId::from_bytes(&op_id_bytes);
            if op_id.hex().starts_with(&hex_prefix) {
                if matched.is_some() {
                    return Ok(PrefixResolution::AmbiguousMatch);
                }
                matched = Some(op_id);
            }
        }

        if let Some(id) = matched {
            Ok(PrefixResolution::SingleMatch(id))
        } else {
            Ok(PrefixResolution::NoMatch)
        }
    }

    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        // Stubbed out for MVP, just like Backend::gc
        Ok(())
    }
}

// --- OpHeadsStore Implementation ---

pub struct SqliteOpHeadsStore {
    connection: Mutex<Connection>,
    project_id: Vec<u8>,
}

impl Debug for SqliteOpHeadsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteOpHeadsStore")
            .field("project_id", &self.project_id)
            .finish()
    }
}

impl SqliteOpHeadsStore {
    pub(crate) fn name() -> &'static str {
        "sqlite"
    }

    fn db_path(store_path: &Path) -> std::path::PathBuf {
        // In classic mode, store_path is .jj/repo/op_heads/ and DB is at .jj/repo/store/store.db.
        // In unified mode, store_path is .jj/repo/ and DB is at .jj/repo/store.db.
        if store_path.file_name().map_or(false, |n| n == "op_store" || n == "op_heads") {
            store_path
                .parent()
                .unwrap()
                .join("store")
                .join("store.db")
        } else {
            store_path.join("store.db")
        }
    }

    pub fn init(
        store_path: &Path,
        root_operation_id: &OperationId,
    ) -> Result<Self, BackendInitError> {
        let db_path = Self::db_path(store_path);
        let conn = Connection::open(&db_path)
            .map_err(|err| BackendInitError(err.into()))?;

        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|err| BackendInitError(err.into()))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS op_heads (
                project_id BLOB NOT NULL,
                op_id BLOB NOT NULL,
                PRIMARY KEY (project_id, op_id)
            )",
            [],
        )
        .map_err(|err| BackendInitError(err.into()))?;

        // Read project_id
        let project_id: Vec<u8> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = 'project_id'",
                [],
                |row| row.get(0),
            )
            .map_err(|_| BackendInitError(SqliteOpStoreError::MetadataMissing.into()))?;

        // Write root_operation_id to op_heads
        conn.execute(
            "INSERT OR IGNORE INTO op_heads (project_id, op_id) VALUES (?1, ?2)",
            [&project_id, root_operation_id.as_bytes()],
        )
        .map_err(|err| BackendInitError(err.into()))?;

        Ok(Self {
            connection: Mutex::new(conn),
            project_id,
        })
    }

    pub fn load(store_path: &Path) -> Result<Self, BackendLoadError> {
        let db_path = Self::db_path(store_path);

        let conn = Connection::open(&db_path)
            .map_err(|err| BackendLoadError(err.into()))?;

        let project_id: Vec<u8> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = 'project_id'",
                [],
                |row| row.get(0),
            )
            .map_err(|err| BackendLoadError(err.into()))?;

        Ok(Self {
            connection: Mutex::new(conn),
            project_id,
        })
    }
}

struct DummyLock;
impl OpHeadsStoreLock for DummyLock {}

#[async_trait]
impl OpHeadsStore for SqliteOpHeadsStore {
    fn name(&self) -> &str {
        Self::name()
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let conn = self.connection.lock().unwrap();
        
        conn.execute("BEGIN IMMEDIATE TRANSACTION", [])
            .map_err(|err| OpHeadsStoreError::Write {
                new_op_id: new_id.clone(),
                source: Box::new(err),
            })?;

        let run_update = || -> rusqlite::Result<()> {
            for old_id in old_ids {
                conn.execute(
                    "DELETE FROM op_heads WHERE project_id = ?1 AND op_id = ?2",
                    [&self.project_id, old_id.as_bytes()],
                )?;
            }
            conn.execute(
                "INSERT OR IGNORE INTO op_heads (project_id, op_id) VALUES (?1, ?2)",
                [&self.project_id, new_id.as_bytes()],
            )?;
            Ok(())
        };

        match run_update() {
            Ok(_) => {
                conn.execute("COMMIT", [])
                    .map_err(|err| OpHeadsStoreError::Write {
                        new_op_id: new_id.clone(),
                        source: Box::new(err),
                    })?;
                Ok(())
            }
            Err(err) => {
                conn.execute("ROLLBACK", []).ok();
                Err(OpHeadsStoreError::Write {
                    new_op_id: new_id.clone(),
                    source: Box::new(err),
                })
            }
        }
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let conn = self.connection.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT op_id FROM op_heads WHERE project_id = ?1")
            .map_err(|err| OpHeadsStoreError::Read(Box::new(err)))?;
        
        let rows = stmt
            .query_map([&self.project_id], |row| row.get::<_, Vec<u8>>(0))
            .map_err(|err| OpHeadsStoreError::Read(Box::new(err)))?;

        let mut heads = Vec::new();
        for op_id_bytes_res in rows {
            let op_id_bytes = op_id_bytes_res.map_err(|err| OpHeadsStoreError::Read(Box::new(err)))?;
            heads.push(OperationId::from_bytes(&op_id_bytes));
        }

        heads.sort();

        if heads.is_empty() {
            Err(OpHeadsStoreError::Read(
                "Corrupt repository: no head operation".into(),
            ))
        } else {
            Ok(heads)
        }
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(DummyLock))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jj_lib::backend::{CommitId, MillisSinceEpoch, Timestamp};
    use jj_lib::op_store::{OperationMetadata, TimestampRange};
    use tempfile::tempdir;

    fn init_stores(temp_dir: &Path) -> (SqliteOpStore, SqliteOpHeadsStore) {
        let store_path = temp_dir.join("store");
        std::fs::create_dir(&store_path).unwrap();
        // Init backend first to create DB and project_id
        let _backend = crate::local_backend::SqliteBackend::init(&store_path);

        let op_store_path = temp_dir.join("op_store");
        std::fs::create_dir(&op_store_path).unwrap();
        let root_commit_id = CommitId::from_bytes(&[0; 64]);
        let root_data = RootOperationData {
            root_commit_id: root_commit_id.clone(),
        };
        let op_store = SqliteOpStore::init(&op_store_path, root_data).unwrap();

        let op_heads_path = temp_dir.join("op_heads");
        std::fs::create_dir(&op_heads_path).unwrap();
        let op_heads_store = SqliteOpHeadsStore::init(&op_heads_path, op_store.root_operation_id()).unwrap();

        (op_store, op_heads_store)
    }

    #[tokio::test]
    async fn test_op_store_read_write_view() {
        let temp_dir = tempdir().unwrap();
        let (op_store, _) = init_stores(temp_dir.path());

        // Create a dummy view
        let mut view = View::make_root(CommitId::from_bytes(&[0; 64]));
        view.head_ids.insert(CommitId::from_bytes(&[1; 64]));
        view.head_ids.insert(CommitId::from_bytes(&[2; 64]));

        // Write view
        let view_id = op_store.write_view(&view).await.unwrap();

        // Read view back
        let read_view = op_store.read_view(&view_id).await.unwrap();
        assert_eq!(read_view.head_ids, view.head_ids);
    }

    #[tokio::test]
    async fn test_op_store_read_write_operation() {
        let temp_dir = tempdir().unwrap();
        let (op_store, _) = init_stores(temp_dir.path());

        // Create a dummy operation
        let view_id = ViewId::from_bytes(&[3; 64]);
        let metadata = OperationMetadata {
            time: TimestampRange {
                start: Timestamp {
                    timestamp: MillisSinceEpoch(1000),
                    tz_offset: 0,
                },
                end: Timestamp {
                    timestamp: MillisSinceEpoch(2000),
                    tz_offset: 0,
                },
            },
            description: "test operation".to_string(),
            hostname: "test-host".to_string(),
            username: "test-user".to_string(),
            is_snapshot: false,
            workspace_name: None,
            attributes: Default::default(),
        };
        let operation = Operation {
            view_id,
            parents: vec![op_store.root_operation_id().clone()],
            metadata,
            commit_predecessors: None,
        };

        // Write operation
        let op_id = op_store.write_operation(&operation).await.unwrap();

        // Read operation back
        let read_op = op_store.read_operation(&op_id).await.unwrap();
        assert_eq!(read_op.view_id, operation.view_id);
        assert_eq!(read_op.parents, operation.parents);
        assert_eq!(read_op.metadata.description, operation.metadata.description);
    }

    #[tokio::test]
    async fn test_op_heads_store() {
        let temp_dir = tempdir().unwrap();
        let (op_store, op_heads_store) = init_stores(temp_dir.path());

        // Initially should contain only root operation
        let heads = op_heads_store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![op_store.root_operation_id().clone()]);

        // Update heads: add op1, remove root
        let op1_id = OperationId::from_bytes(&[1; 64]);
        
        op_heads_store
            .update_op_heads(&[op_store.root_operation_id().clone()], &op1_id)
            .await
            .unwrap();

        let heads = op_heads_store.get_op_heads().await.unwrap();
        assert_eq!(heads, vec![op1_id.clone()]);

        // Add op2, keep op1 (divergent heads)
        let op2_id = OperationId::from_bytes(&[2; 64]);
        op_heads_store.update_op_heads(&[], &op2_id).await.unwrap();

        let mut heads = op_heads_store.get_op_heads().await.unwrap();
        heads.sort();
        let mut expected = vec![op1_id, op2_id];
        expected.sort();
        assert_eq!(heads, expected);
    }
}
