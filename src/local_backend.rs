// Copyright 2024 The Jujutsu Authors
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

#![expect(missing_docs)]

use std::fmt::Debug;
use std::path::Path;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use blake2::{Blake2b512, Digest as _};
use futures::{AsyncRead, AsyncReadExt as _, StreamExt as _};
use futures::io::Cursor;
use futures::stream::{self, BoxStream};
use pollster::FutureExt as _;
use prost::Message as _;
use rusqlite::{Connection, params};

use jj_lib::backend::{
    Backend, BackendError, BackendResult, ChangeId, Commit, CommitId, CopyHistory, CopyId,
    CopyRecord, FileId, MillisSinceEpoch, RelatedCopy, SecureSig, Signature, SigningFn,
    SymlinkId, Timestamp, Tree, TreeId, TreeValue, make_root_commit,
};
use jj_lib::content_hash::blake2b_hash;
use jj_lib::index::Index;
use jj_lib::merge::MergeBuilder;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf, RepoPathComponentBuf};
use jj_lib::conflict_labels::ConflictLabels;

const COMMIT_ID_LENGTH: usize = 64;
const CHANGE_ID_LENGTH: usize = 16;

fn map_sqlite_err(err: rusqlite::Error, id: &impl ObjectId) -> BackendError {
    match err {
        rusqlite::Error::QueryReturnedNoRows => BackendError::ObjectNotFound {
            object_type: id.object_type(),
            hash: id.hex(),
            source: Box::new(err),
        },
        _ => BackendError::ReadObject {
            object_type: id.object_type(),
            hash: id.hex(),
            source: Box::new(err),
        },
    }
}

fn to_other_err(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> BackendError {
    BackendError::Other(err.into())
}

#[derive(Debug)]
pub struct SqliteBackend {
    conn: Mutex<Connection>,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
    project_id: Vec<u8>,
}

impl SqliteBackend {
    pub fn name() -> &'static str {
        "sqlite"
    }

    pub fn init(store_path: &Path) -> Self {
        let db_path = store_path.join("store.db");
        let conn = Connection::open(&db_path).unwrap();
        
        // Enable WAL mode for better performance
        conn.execute("PRAGMA journal_mode = WAL;", ()).ok();

        // Create tables (Tier 1 Schema)
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS file_contents (
                project_id BLOB NOT NULL,
                file_id BLOB NOT NULL,
                content BLOB NOT NULL,
                PRIMARY KEY (project_id, file_id)
            );
            CREATE TABLE IF NOT EXISTS trees (
                project_id BLOB NOT NULL,
                tree_id BLOB NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (project_id, tree_id)
            );
            CREATE TABLE IF NOT EXISTS commits (
                project_id BLOB NOT NULL,
                commit_id BLOB NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (project_id, commit_id)
            );
            CREATE TABLE IF NOT EXISTS symlinks (
                project_id BLOB NOT NULL,
                symlink_id BLOB NOT NULL,
                target TEXT NOT NULL,
                PRIMARY KEY (project_id, symlink_id)
            );"
        ).unwrap();

        // Generate and store project_id
        let project_id = rand::random::<[u8; 16]>().to_vec();
        conn.execute(
            "INSERT OR IGNORE INTO metadata (key, value) VALUES ('project_id', ?1)",
            params![project_id],
        ).unwrap();

        let backend = Self::load_with_conn(conn, project_id);
        
        // Write empty tree
        let empty_tree_id = backend
            .write_tree(RepoPath::root(), &Tree::default())
            .block_on()
            .unwrap();
        assert_eq!(empty_tree_id, backend.empty_tree_id);

        backend
    }

    pub fn load(store_path: &Path) -> Self {
        let db_path = store_path.join("store.db");
        let conn = Connection::open(&db_path).unwrap();
        
        let project_id: Vec<u8> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = 'project_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        Self::load_with_conn(conn, project_id)
    }

    fn load_with_conn(conn: Connection, project_id: Vec<u8>) -> Self {
        let root_commit_id = CommitId::from_bytes(&[0; COMMIT_ID_LENGTH]);
        let root_change_id = ChangeId::from_bytes(&[0; CHANGE_ID_LENGTH]);
        let empty_tree_id = TreeId::from_hex(
            "482ae5a29fbe856c7272f2071b8b0f0359ee2d89ff392b8a900643fbd0836eccd067b8bf41909e206c90d45d6e7d8b6686b93ecaee5fe1a9060d87b672101310",
        );
        Self {
            conn: Mutex::new(conn),
            root_commit_id,
            root_change_id,
            empty_tree_id,
            project_id,
        }
    }
}

#[async_trait]
impl Backend for SqliteBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let conn = self.conn.lock().unwrap();
        let content: Vec<u8> = conn
            .query_row(
                "SELECT content FROM file_contents WHERE project_id = ?1 AND file_id = ?2",
                params![self.project_id, id.to_bytes()],
                |row| row.get(0),
            )
            .map_err(|err| map_sqlite_err(err, id))?;
        Ok(Box::pin(Cursor::new(content)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buff = vec![];
        contents.read_to_end(&mut buff).await.map_err(to_other_err)?;
        
        let mut hasher = Blake2b512::new();
        hasher.update(&buff);
        let id = FileId::new(hasher.finalize().to_vec());

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO file_contents (project_id, file_id, content) VALUES (?1, ?2, ?3)",
            params![self.project_id, id.to_bytes(), buff],
        ).map_err(to_other_err)?;

        Ok(id)
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let conn = self.conn.lock().unwrap();
        let target: String = conn
            .query_row(
                "SELECT target FROM symlinks WHERE project_id = ?1 AND symlink_id = ?2",
                params![self.project_id, id.to_bytes()],
                |row| row.get(0),
            )
            .map_err(|err| map_sqlite_err(err, id))?;
        Ok(target)
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let mut hasher = Blake2b512::new();
        hasher.update(target.as_bytes());
        let id = SymlinkId::new(hasher.finalize().to_vec());

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO symlinks (project_id, symlink_id, target) VALUES (?1, ?2, ?3)",
            params![self.project_id, id.to_bytes(), target],
        ).map_err(to_other_err)?;

        Ok(id)
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "The sqlite backend doesn't support copies yet".to_string(),
        ))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "The sqlite backend doesn't support copies yet".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "The sqlite backend doesn't support copies yet".to_string(),
        ))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        let conn = self.conn.lock().unwrap();
        let buf: Vec<u8> = conn
            .query_row(
                "SELECT data FROM trees WHERE project_id = ?1 AND tree_id = ?2",
                params![self.project_id, id.to_bytes()],
                |row| row.get(0),
            )
            .map_err(|err| map_sqlite_err(err, id))?;

        let proto = jj_lib::protos::simple_store::Tree::decode(&*buf).map_err(to_other_err)?;
        Ok(tree_from_proto(proto))
    }

    async fn write_tree(&self, _path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        let proto = tree_to_proto(tree);
        let buf = proto.encode_to_vec();
        let id = TreeId::new(blake2b_hash(tree).to_vec());

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO trees (project_id, tree_id, data) VALUES (?1, ?2, ?3)",
            params![self.project_id, id.to_bytes(), buf],
        ).map_err(to_other_err)?;

        Ok(id)
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let conn = self.conn.lock().unwrap();
        let buf: Vec<u8> = conn
            .query_row(
                "SELECT data FROM commits WHERE project_id = ?1 AND commit_id = ?2",
                params![self.project_id, id.to_bytes()],
                |row| row.get(0),
            )
            .map_err(|err| map_sqlite_err(err, id))?;

        let proto = jj_lib::protos::simple_store::Commit::decode(&*buf).map_err(to_other_err)?;
        Ok(commit_from_proto(proto))
    }

    async fn write_commit(
        &self,
        mut commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        assert!(commit.secure_sig.is_none(), "commit.secure_sig was set");

        if commit.parents.is_empty() {
            return Err(BackendError::Other(
                "Cannot write a commit with no parents".into(),
            ));
        }

        let mut proto = commit_to_proto(&commit);
        if let Some(sign) = sign_with {
            let data = proto.encode_to_vec();
            let sig = sign(&data).map_err(to_other_err)?;
            proto.secure_sig = Some(sig.clone());
            commit.secure_sig = Some(SecureSig { data, sig });
        }

        let buf = proto.encode_to_vec();
        let id = CommitId::new(blake2b_hash(&commit).to_vec());

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO commits (project_id, commit_id, data) VALUES (?1, ?2, ?3)",
            params![self.project_id, id.to_bytes(), buf],
        ).map_err(to_other_err)?;

        Ok((id, commit))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }

    fn gc(&self, _index: &dyn Index, _keep_newer: SystemTime) -> BackendResult<()> {
        Ok(())
    }
}

// Proto conversion helpers (copied from simple_backend.rs)

fn commit_to_proto(commit: &Commit) -> jj_lib::protos::simple_store::Commit {
    let mut proto = jj_lib::protos::simple_store::Commit::default();
    for parent in &commit.parents {
        proto.parents.push(parent.to_bytes());
    }
    for predecessor in &commit.predecessors {
        proto.predecessors.push(predecessor.to_bytes());
    }
    proto.root_tree = commit.root_tree.iter().map(|id| id.to_bytes()).collect();
    if !commit.conflict_labels.is_resolved() {
        proto.conflict_labels = commit.conflict_labels.as_slice().to_owned();
    }
    proto.change_id = commit.change_id.to_bytes();
    proto.description = commit.description.clone();
    proto.author = Some(signature_to_proto(&commit.author));
    proto.committer = Some(signature_to_proto(&commit.committer));
    proto
}

fn commit_from_proto(mut proto: jj_lib::protos::simple_store::Commit) -> Commit {
    let secure_sig = proto.secure_sig.take().map(|sig| SecureSig {
        data: proto.encode_to_vec(),
        sig,
    });

    let parents = proto.parents.into_iter().map(CommitId::new).collect();
    let predecessors = proto.predecessors.into_iter().map(CommitId::new).collect();
    let merge_builder: MergeBuilder<_> = proto.root_tree.into_iter().map(TreeId::new).collect();
    let root_tree = merge_builder.build();
    let conflict_labels = ConflictLabels::from_vec(proto.conflict_labels);
    let change_id = ChangeId::new(proto.change_id);
    Commit {
        parents,
        predecessors,
        root_tree,
        conflict_labels: conflict_labels.into_merge(),
        change_id,
        description: proto.description,
        author: signature_from_proto(proto.author.unwrap_or_default()),
        committer: signature_from_proto(proto.committer.unwrap_or_default()),
        secure_sig,
    }
}

fn tree_to_proto(tree: &Tree) -> jj_lib::protos::simple_store::Tree {
    let mut proto = jj_lib::protos::simple_store::Tree::default();
    for entry in tree.entries() {
        proto
            .entries
            .push(jj_lib::protos::simple_store::tree::Entry {
                name: entry.name().as_internal_str().to_owned(),
                value: Some(tree_value_to_proto(entry.value())),
            });
    }
    proto
}

fn tree_from_proto(proto: jj_lib::protos::simple_store::Tree) -> Tree {
    let entries = proto
        .entries
        .into_iter()
        .map(|proto_entry| {
            let value = tree_value_from_proto(proto_entry.value.unwrap());
            (RepoPathComponentBuf::new(proto_entry.name).unwrap(), value)
        })
        .collect();
    Tree::from_sorted_entries(entries)
}

fn tree_value_to_proto(value: &TreeValue) -> jj_lib::protos::simple_store::TreeValue {
    let mut proto = jj_lib::protos::simple_store::TreeValue::default();
    match value {
        TreeValue::File {
            id,
            executable,
            copy_id,
        } => {
            proto.value = Some(jj_lib::protos::simple_store::tree_value::Value::File(
                jj_lib::protos::simple_store::tree_value::File {
                    id: id.to_bytes(),
                    executable: *executable,
                    copy_id: copy_id.to_bytes(),
                },
            ));
        }
        TreeValue::Symlink(id) => {
            proto.value = Some(jj_lib::protos::simple_store::tree_value::Value::SymlinkId(
                id.to_bytes(),
            ));
        }
        TreeValue::GitSubmodule(_id) => {
            panic!("cannot store git submodules");
        }
        TreeValue::Tree(id) => {
            proto.value = Some(jj_lib::protos::simple_store::tree_value::Value::TreeId(
                id.to_bytes(),
            ));
        }
    }
    proto
}

fn tree_value_from_proto(proto: jj_lib::protos::simple_store::TreeValue) -> TreeValue {
    match proto.value.unwrap() {
        jj_lib::protos::simple_store::tree_value::Value::TreeId(id) => {
            TreeValue::Tree(TreeId::new(id))
        }
        jj_lib::protos::simple_store::tree_value::Value::File(
            jj_lib::protos::simple_store::tree_value::File {
                id,
                executable,
                copy_id,
                ..
            },
        ) => TreeValue::File {
            id: FileId::new(id),
            executable,
            copy_id: CopyId::new(copy_id),
        },
        jj_lib::protos::simple_store::tree_value::Value::SymlinkId(id) => {
            TreeValue::Symlink(SymlinkId::new(id))
        }
    }
}

fn signature_to_proto(signature: &Signature) -> jj_lib::protos::simple_store::commit::Signature {
    jj_lib::protos::simple_store::commit::Signature {
        name: signature.name.clone(),
        email: signature.email.clone(),
        timestamp: Some(jj_lib::protos::simple_store::commit::Timestamp {
            millis_since_epoch: signature.timestamp.timestamp.0,
            tz_offset: signature.timestamp.tz_offset,
        }),
    }
}

fn signature_from_proto(proto: jj_lib::protos::simple_store::commit::Signature) -> Signature {
    let timestamp = proto.timestamp.unwrap_or_default();
    Signature {
        name: proto.name,
        email: proto.email,
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(timestamp.millis_since_epoch),
            tz_offset: timestamp.tz_offset,
        },
    }
}

#[cfg(test)]
mod tests {
    use pollster::FutureExt as _;

    use super::*;
    use jj_lib::merge::Merge;
    use testutils::TestResult;
    use testutils::new_temp_dir;

    #[test]
    fn test_sqlite_backend_basics() -> TestResult {
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();

        let backend = SqliteBackend::init(store_path);
        
        // Test that we can write and read a file
        let file_content = b"hello sqlite backend";
        let file_id = backend.write_file(RepoPath::root(), &mut Cursor::new(file_content)).block_on()?;
        
        let mut read_content = vec![];
        backend.read_file(RepoPath::root(), &file_id).block_on()?.read_to_end(&mut read_content).block_on()?;
        assert_eq!(read_content, file_content);

        // Test that we can write and read a tree
        let mut tree_entries = vec![];
        tree_entries.push((
            RepoPathComponentBuf::new("file.txt".to_string()).unwrap(),
            TreeValue::File {
                id: file_id.clone(),
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        ));
        let tree = Tree::from_sorted_entries(tree_entries);
        let tree_id = backend.write_tree(RepoPath::root(), &tree).block_on()?;
        
        let read_tree = backend.read_tree(RepoPath::root(), &tree_id).block_on()?;
        assert_eq!(read_tree, tree);

        // Test that we can write and read a commit
        let signature = Signature {
            name: "Test Writer".to_string(),
            email: "test@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let commit = Commit {
            parents: vec![backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(tree_id),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("abc12345678901234567890123456789"), // 32 hex chars = 16 bytes
            description: "initial commit".to_string(),
            author: signature.clone(),
            committer: signature,
            secure_sig: None,
        };

        let (commit_id, written_commit) = backend.write_commit(commit.clone(), None).block_on()?;
        assert_eq!(written_commit, commit);

        let read_commit = backend.read_commit(&commit_id).block_on()?;
        assert_eq!(read_commit, commit);

        Ok(())
    }
}

