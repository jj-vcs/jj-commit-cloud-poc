#![allow(elided_lifetimes_in_paths)]

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use jj_lib::backend::*;
use jj_lib::index::Index;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf, RepoPathComponentBuf};
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommitCloudConfig {
    pub server_url: String,
    pub repo_id: String,
}

impl CommitCloudConfig {
    pub fn load_from_store(
        store_path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config_path = store_path.join("config.toml");
        let content = fs::read_to_string(&config_path)?;
        Ok(toml::from_str(&content)?)
    }
}

#[derive(Debug)]
pub struct CommitCloudBackend {
    server_url: String,
    repo_id: String,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

// TODO: Replace run_async function with a persistent gRPC channel connection in the
// CommitCloudBackend trait. Member functions should check if connection exists otherwise create
// one.
fn run_async<F, Fut, T>(f: F) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>
        + Send
        + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(f())
    })
    .join()
    .map_err(|e| format!("Thread join error: {:?}", e))?
}

impl CommitCloudBackend {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn init(
        store_path: &Path,
        server_url: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let repo_id = Uuid::new_v4().to_string();
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);

        let server_url_cloned = server_url.to_string();
        let repo_id_cloned = repo_id.clone();
        run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url_cloned,
                )
                .await?;
            client
                .register_repository(tonic::Request::new(
                    cc_common::backend::RegisterRepositoryRequest {
                        repo_id: repo_id_cloned,
                    },
                ))
                .await?;
            Ok(())
        })?;

        // Write local config toml
        let config_path = store_path.join("config.toml");
        let config = CommitCloudConfig {
            server_url: server_url.to_string(),
            repo_id: repo_id.clone(),
        };
        fs::write(&config_path, toml::to_string_pretty(&config)?)?;

        Ok(Self {
            server_url: server_url.to_string(),
            repo_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        let root_commit_id = CommitId::from_bytes(&cc_common::ROOT_COMMIT_ID_BYTES);
        let root_change_id = ChangeId::from_bytes(&cc_common::ROOT_CHANGE_ID_BYTES);
        let empty_tree_id = TreeId::from_hex(cc_common::EMPTY_TREE_ID_HEX);
        Ok(Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }
}

// Functions below turn commit objects into protobuf objects defined in proto/src/backend.rs
// see upstream jj/lib/src/simple_backend.rs for an example on how they are implemented in the
// simple backend.
//
// These are needed for the Backend:: function calls to reusably serialize jj objects to their proto
// counterparts

// TODO: Investigate if there is a better way to do these object to proto conversions
fn signature_to_proto(sig: &Signature) -> cc_common::backend::Signature {
    cc_common::backend::Signature {
        name: sig.name.clone(),
        email: sig.email.clone(),
        timestamp: Some(cc_common::backend::Timestamp {
            millis_since_epoch: sig.timestamp.timestamp.0,
            tz_offset: sig.timestamp.tz_offset,
        }),
    }
}

fn signature_from_proto(proto: cc_common::backend::Signature) -> Signature {
    let ts = proto.timestamp.unwrap_or_default();
    Signature {
        name: proto.name,
        email: proto.email,
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(ts.millis_since_epoch),
            tz_offset: ts.tz_offset,
        },
    }
}

fn commit_to_proto(commit: &Commit) -> cc_common::backend::Commit {
    cc_common::backend::Commit {
        commit_id: vec![],
        change_id: commit.change_id.to_bytes().to_vec(),
        parent_commit_ids: commit
            .parents
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        root_tree_id: commit
            .root_tree
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        description: commit.description.clone(),
        author: Some(signature_to_proto(&commit.author)),
        committer: Some(signature_to_proto(&commit.committer)),
        predecessors: commit
            .predecessors
            .iter()
            .map(|id| id.to_bytes().to_vec())
            .collect(),
        conflict_labels: commit.conflict_labels.as_slice().to_owned(),
        secure_sig: commit.secure_sig.as_ref().map(|s| s.sig.clone()),
    }
}

fn commit_from_proto(proto_commit: cc_common::backend::Commit) -> Commit {
    let author = signature_from_proto(proto_commit.author.unwrap_or_default());
    let committer = signature_from_proto(proto_commit.committer.unwrap_or_default());

    let merge_builder: jj_lib::merge::MergeBuilder<_> = proto_commit
        .root_tree_id
        .into_iter()
        .map(|b| TreeId::from_bytes(&b))
        .collect();

    let root_tree = merge_builder.build();
    let conflict_labels =
        jj_lib::conflict_labels::ConflictLabels::from_vec(proto_commit.conflict_labels)
            .into_merge();

    Commit {
        parents: proto_commit
            .parent_commit_ids
            .iter()
            .map(|b| CommitId::from_bytes(b))
            .collect(),
        predecessors: proto_commit
            .predecessors
            .iter()
            .map(|b| CommitId::from_bytes(b))
            .collect(),
        root_tree,
        change_id: ChangeId::from_bytes(&proto_commit.change_id),
        description: proto_commit.description,
        author,
        committer,
        conflict_labels,
        secure_sig: None,
    }
}

fn tree_entry_to_proto(
    entry: &jj_lib::backend::TreeEntry,
) -> BackendResult<cc_common::backend::TreeEntry> {
    let value = match entry.value() {
        TreeValue::File {
            id,
            executable,
            copy_id,
        } => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::File(
                cc_common::backend::File {
                    id: id.to_bytes().to_vec(),
                    executable: *executable,
                    copy_id: copy_id.to_bytes().to_vec(),
                },
            )),
        },
        TreeValue::Symlink(id) => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::SymlinkId(
                id.to_bytes().to_vec(),
            )),
        },
        TreeValue::Tree(id) => cc_common::backend::TreeValue {
            value: Some(cc_common::backend::tree_value::Value::TreeId(
                id.to_bytes().to_vec(),
            )),
        },
        TreeValue::GitSubmodule(_id) => {
            return Err(BackendError::Unsupported(
                "Git submodules are not supported".into(),
            ));
        }
    };

    Ok(cc_common::backend::TreeEntry {
        name: entry.name().as_internal_str().to_string(),
        value: Some(value),
    })
}

fn tree_entry_from_proto(
    proto_entry: cc_common::backend::TreeEntry,
) -> Result<(RepoPathComponentBuf, TreeValue), Box<dyn std::error::Error + Send + Sync>> {
    let component = RepoPathComponentBuf::new(proto_entry.name)?;
    let proto_val = proto_entry
        .value
        .ok_or_else(|| "tree entry should have contained a TreeValue")?;
    let val = match proto_val
        .value
        .ok_or_else(|| "TreeValue should have contained an inner value")?
    {
        cc_common::backend::tree_value::Value::File(f) => TreeValue::File {
            id: FileId::from_bytes(&f.id),
            executable: f.executable,
            copy_id: CopyId::from_bytes(if f.copy_id.len() == cc_common::COMMIT_ID_LENGTH {
                &f.copy_id
            } else {
                &cc_common::ROOT_COMMIT_ID_BYTES
            }),
        },
        cc_common::backend::tree_value::Value::SymlinkId(id) => {
            TreeValue::Symlink(SymlinkId::from_bytes(&id))
        }
        cc_common::backend::tree_value::Value::TreeId(id) => {
            TreeValue::Tree(TreeId::from_bytes(&id))
        }
        cc_common::backend::tree_value::Value::ConflictId(_) => {
            return Err("tree entry should not have contained a ConflictId".into());
        }
    };
    Ok((component, val))
}

#[async_trait]
impl Backend for CommitCloudBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        cc_common::COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        cc_common::CHANGE_ID_LENGTH
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
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let file_id_bytes = id.to_bytes().to_vec();
        let file_id_hex = id.hex();

        let content = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .read_file(tonic::Request::new(cc_common::backend::ReadFileRequest {
                    repo_id,
                    file_id: file_id_bytes,
                }))
                .await;

            let mut stream = match res {
                Ok(r) => r.into_inner(),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    return Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "file".into(),
                        hash: file_id_hex,
                        source: status.into(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                Err(status) => {
                    return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                }
            };

            let mut content = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| BackendError::Other(e.into()))?;
                content.extend_from_slice(&chunk.chunk);
            }
            Ok(content)
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        let cursor = futures::io::Cursor::new(content);
        Ok(Box::pin(cursor) as Pin<Box<dyn futures::AsyncRead + Send>>)
    }

    // TODO: Upgrade write_file to stream 64KB chunks directly from AsyncRead to gRPC
    // instead of buffering the whole file payload into memory in a single unary RPC.
    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn futures::AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buffer = Vec::new();
        futures::AsyncReadExt::read_to_end(contents, &mut buffer)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let file_id_bytes = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .write_file(tonic::Request::new(cc_common::backend::WriteFileRequest {
                    repo_id,
                    content: buffer,
                }))
                .await?;
            Ok(res.into_inner().file_id)
        })
        .map_err(|e| BackendError::Other(e.into()))?;

        Ok(FileId::from_bytes(&file_id_bytes))
    }

    async fn read_symlink(&self, _path: &RepoPath, _id: &SymlinkId) -> BackendResult<String> {
        Err(BackendError::Unsupported(
            "read_symlink not supported".to_string(),
        ))
    }

    async fn write_symlink(&self, _path: &RepoPath, _target: &str) -> BackendResult<SymlinkId> {
        Err(BackendError::Unsupported(
            "write_symlink not supported".to_string(),
        ))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported(
            "copies not supported".to_string(),
        ))
    }

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let tree_id_bytes = id.to_bytes().to_vec();
        let tree_id_hex = id.hex();
        let path_str = path.as_internal_file_string().to_string();

        let proto_entries = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .read_tree(tonic::Request::new(cc_common::backend::ReadTreeRequest {
                    repo_id,
                    tree_id: tree_id_bytes,
                    path: path_str,
                }))
                .await;

            match res {
                Ok(r) => Ok(r.into_inner().entries),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "tree".into(),
                        hash: tree_id_hex,
                        source: status.into(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        let mut jj_entries = Vec::new();
        for entry in proto_entries {
            let (comp, val) =
                tree_entry_from_proto(entry).map_err(|e| BackendError::Other(e.into()))?;
            jj_entries.push((comp, val));
        }

        Ok(Tree::from_sorted_entries(jj_entries))
    }

    async fn write_tree(&self, path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        if tree.entries().next().is_none() {
            return Ok(self.empty_tree_id.clone());
        }

        let proto_entries: Result<Vec<_>, _> =
            tree.entries().map(|e| tree_entry_to_proto(&e)).collect();
        let proto_entries = proto_entries?;

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let path_str = path.as_internal_file_string().to_string();

        let tree_id_bytes = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .write_tree(tonic::Request::new(cc_common::backend::WriteTreeRequest {
                    repo_id,
                    path: path_str,
                    entries: proto_entries,
                }))
                .await?;
            Ok(res.into_inner().tree_id)
        })
        .map_err(|e| BackendError::Other(e.into()))?;

        Ok(TreeId::from_bytes(&tree_id_bytes))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id().clone(),
                self.empty_tree_id.clone(),
            ));
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let commit_id_bytes = id.to_bytes().to_vec();
        let commit_id_hex = id.hex();

        let proto_commit = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .read_commit(tonic::Request::new(cc_common::backend::ReadCommitRequest {
                    repo_id,
                    commit_id: commit_id_bytes,
                }))
                .await;

            match res {
                Ok(r) => r.into_inner().commit.ok_or_else(|| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "server response should have contained commit data",
                    )) as Box<dyn std::error::Error + Send + Sync>
                }),
                Err(status) if status.code() == tonic::Code::NotFound => {
                    Err(Box::new(BackendError::ObjectNotFound {
                        object_type: "commit".into(),
                        hash: commit_id_hex,
                        source: status.into(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(status) => Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
        .map_err(|e| match e.downcast::<BackendError>() {
            Ok(err) => *err,
            Err(e) => BackendError::Other(e),
        })?;

        Ok(commit_from_proto(proto_commit))
    }

    async fn write_commit(
        &self,
        commit: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let proto_commit = commit_to_proto(&commit);
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let returned_id_bytes = run_async(move || async move {
            let mut client =
                cc_common::backend::backend_service_client::BackendServiceClient::connect(
                    server_url,
                )
                .await?;
            let res = client
                .write_commit(tonic::Request::new(
                    cc_common::backend::WriteCommitRequest {
                        repo_id,
                        commit: Some(proto_commit),
                    },
                ))
                .await?;
            Ok(res.into_inner().commit_id)
        })
        .map_err(|e| BackendError::Other(e.into()))?;

        let returned_id = CommitId::from_bytes(&returned_id_bytes);
        Ok((returned_id, commit))
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
