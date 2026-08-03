#![allow(elided_lifetimes_in_paths)]

use async_trait::async_trait;
use cc_proto::backend as proto_backend;
use cc_proto::backend::backend_service_client::BackendServiceClient;
use futures::stream::{self, BoxStream};
use futures::StreamExt as _;
use jj_lib::backend::*;
use jj_lib::index::Index;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf, RepoPathComponentBuf};
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::time::SystemTime;

const HASH_LENGTH: usize = 20;
const CHANGE_ID_LENGTH: usize = 16;

fn make_request<T>(payload: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(payload);
    if let Ok(token) = std::env::var("JJ_CC_AUTH_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            if let Ok(val) = format!("Bearer {}", token).parse() {
                req.metadata_mut().insert("authorization", val);
            }
        }
    }
    req
}

fn run_async<F: std::future::Future + Send + 'static>(f: F) -> F::Output

where
    F::Output: Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(f)
            })
            .join()
            .unwrap()
        } else {
            tokio::task::block_in_place(|| handle.block_on(f))
        }
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(f)
    }
}

#[derive(Debug, Clone)]
pub struct CommitCloudBackend {
    repo_id: String,
    server_url: String,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

impl CommitCloudBackend {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn init(
        store_path: &Path,
        server_url: &str,
        explicit_repo_id: Option<&str>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let root_commit_id = CommitId::from_bytes(&[0u8; HASH_LENGTH]);
        let root_change_id = ChangeId::from_bytes(&[0u8; CHANGE_ID_LENGTH]);
        let empty_tree_id = TreeId::from_hex("4b825dc642cb6eb9a060e54bf8d69288fbee4904");

        let repo_id = if let Some(id) = explicit_repo_id {
            id.to_string()
        } else {
            let server_url_owned = server_url.to_string();

            run_async(async move {
                let mut client = BackendServiceClient::connect(server_url_owned).await?;
                let resp = client
                    .register_repository(make_request(proto_backend::RegisterRepositoryRequest {
                        repo_id: String::new(),
                    }))
                    .await?;

                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(resp.into_inner().repo_id)
            })?
        };


        let config_path = store_path.join("config.toml");
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let config_content = format!(
            "server_url = \"{}\"\nrepo_id = \"{}\"\n",
            server_url, repo_id
        );
        fs::write(&config_path, config_content)?;

        Ok(Self {
            repo_id,
            server_url: server_url.to_string(),
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config_path = store_path.join("config.toml");
        let content = fs::read_to_string(&config_path)?;

        let mut server_url = String::new();
        let mut repo_id = String::new();
        for line in content.lines() {
            if let Some(val) = line.strip_prefix("server_url = ") {
                server_url = val.trim_matches('"').to_string();
            } else if let Some(val) = line.strip_prefix("repo_id = ") {
                repo_id = val.trim_matches('"').to_string();
            }
        }

        let root_commit_id = CommitId::from_bytes(&[0u8; HASH_LENGTH]);
        let root_change_id = ChangeId::from_bytes(&[0u8; CHANGE_ID_LENGTH]);
        let empty_tree_id = TreeId::from_hex("4b825dc642cb6eb9a060e54bf8d69288fbee4904");

        Ok(Self {
            repo_id,
            server_url,
            root_commit_id,
            root_change_id,
            empty_tree_id,
        })
    }
}

#[async_trait]
impl Backend for CommitCloudBackend {
    fn name(&self) -> &str {
        Self::name()
    }

    fn commit_id_length(&self) -> usize {
        HASH_LENGTH
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
    ) -> BackendResult<Pin<Box<dyn futures::AsyncRead + Send>>> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let file_id = id.as_bytes().to_vec();

        run_async(async move {
            let mut client = BackendServiceClient::connect(server_url)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let resp = client
                .read_file(make_request(proto_backend::ReadFileRequest {
                    repo_id,
                    file_id,
                }))
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let mut stream = resp.into_inner();
            let mut content = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| BackendError::Other(e.into()))?;
                content.extend_from_slice(&chunk.chunk);
            }

            let cursor = futures::io::Cursor::new(content);
            Ok(Box::pin(cursor) as Pin<Box<dyn futures::AsyncRead + Send>>)
        })
    }

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

        run_async(async move {
            let mut client = BackendServiceClient::connect(server_url)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let resp = client
                .write_file(make_request(proto_backend::WriteFileRequest {
                    repo_id,
                    content: buffer,
                }))
                .await
                .map_err(|e| BackendError::Other(e.into()))?;


            let file_id = FileId::from_bytes(&resp.into_inner().file_id);
            Ok(file_id)
        })
    }

    async fn read_symlink(&self, _path: &RepoPath, _id: &SymlinkId) -> BackendResult<String> {
        Err(BackendError::Unsupported("read_symlink not supported".to_string()))
    }

    async fn write_symlink(&self, _path: &RepoPath, _target: &str) -> BackendResult<SymlinkId> {
        Err(BackendError::Unsupported("write_symlink not supported".to_string()))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn write_copy(&self, _contents: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        Err(BackendError::Unsupported("copies not supported".to_string()))
    }

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::from_sorted_entries(vec![]));
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let tree_id = id.as_bytes().to_vec();
        let path_str = path.as_internal_file_string().to_string();

        run_async(async move {
            let mut client = BackendServiceClient::connect(server_url)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let resp = client
                .read_tree(make_request(proto_backend::ReadTreeRequest {
                    repo_id,
                    tree_id,
                    path: path_str,
                }))
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let proto_entries = resp.into_inner().entries;
            let mut jj_entries = Vec::new();
            for entry in proto_entries {
                let path_component = RepoPathComponentBuf::new(entry.name)
                    .map_err(|e| BackendError::Other(e.into()))?;

                let value = match proto_backend::TreeEntryType::try_from(entry.entry_type) {
                    Ok(proto_backend::TreeEntryType::File) => TreeValue::File {
                        id: FileId::from_bytes(&entry.entry_id),
                        executable: entry.executable != 0,
                        copy_id: CopyId::from_bytes(&[0u8; HASH_LENGTH]),
                    },
                    Ok(proto_backend::TreeEntryType::Symlink) => {
                        TreeValue::Symlink(SymlinkId::from_bytes(&entry.entry_id))
                    }
                    Ok(proto_backend::TreeEntryType::Tree) => {
                        TreeValue::Tree(TreeId::from_bytes(&entry.entry_id))
                    }
                    _ => TreeValue::File {
                        id: FileId::from_bytes(&entry.entry_id),
                        executable: false,
                        copy_id: CopyId::from_bytes(&[0u8; HASH_LENGTH]),
                    },
                };

                jj_entries.push((path_component, value));
            }

            Ok(Tree::from_sorted_entries(jj_entries))
        })
    }

    async fn write_tree(&self, path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        let mut proto_entries = Vec::new();
        for entry in tree.entries() {
            let (entry_type, entry_id, executable) = match entry.value() {
                TreeValue::File { id, executable, .. } => (
                    proto_backend::TreeEntryType::File as i32,
                    id.as_bytes().to_vec(),
                    if *executable { 1 } else { 0 },
                ),
                TreeValue::Symlink(id) => (
                    proto_backend::TreeEntryType::Symlink as i32,
                    id.as_bytes().to_vec(),
                    0,
                ),
                TreeValue::Tree(id) => (
                    proto_backend::TreeEntryType::Tree as i32,
                    id.as_bytes().to_vec(),
                    0,
                ),
                TreeValue::GitSubmodule(id) => (
                    proto_backend::TreeEntryType::File as i32,
                    id.as_bytes().to_vec(),
                    0,
                ),
            };

            proto_entries.push(proto_backend::TreeEntry {
                name: entry.name().as_internal_str().to_string(),
                entry_id,
                entry_type,
                executable,
            });
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let path_str = path.as_internal_file_string().to_string();

        run_async(async move {
            let mut client = BackendServiceClient::connect(server_url)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let resp = client
                .write_tree(make_request(proto_backend::WriteTreeRequest {
                    repo_id,
                    path: path_str,
                    entries: proto_entries,
                }))
                .await
                .map_err(|e| BackendError::Other(e.into()))?;


            let tree_id = TreeId::from_bytes(&resp.into_inner().tree_id);
            Ok(tree_id)
        })
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
        let commit_id = id.as_bytes().to_vec();

        run_async(async move {
            let mut client = BackendServiceClient::connect(server_url)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let resp = client
                .read_commit(make_request(proto_backend::ReadCommitRequest {
                    repo_id,
                    commit_id,
                }))
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let proto_commit = resp.into_inner().commit.ok_or_else(|| {
                BackendError::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Commit not found on server",
                )))
            })?;

            let jj_parents = proto_commit
                .parent_commit_ids
                .iter()
                .map(|b| CommitId::from_bytes(b))
                .collect();

            let author = if let Some(s) = proto_commit.author {
                Signature {
                    name: s.name,
                    email: s.email,
                    timestamp: Timestamp {
                        timestamp: MillisSinceEpoch(s.timestamp_millis),
                        tz_offset: 0,
                    },
                }
            } else {
                Signature {
                    name: String::new(),
                    email: String::new(),
                    timestamp: Timestamp {
                        timestamp: MillisSinceEpoch(0),
                        tz_offset: 0,
                    },
                }
            };

            let committer = if let Some(s) = proto_commit.committer {
                Signature {
                    name: s.name,
                    email: s.email,
                    timestamp: Timestamp {
                        timestamp: MillisSinceEpoch(s.timestamp_millis),
                        tz_offset: 0,
                    },
                }
            } else {
                Signature {
                    name: String::new(),
                    email: String::new(),
                    timestamp: Timestamp {
                        timestamp: MillisSinceEpoch(0),
                        tz_offset: 0,
                    },
                }
            };

            Ok(Commit {
                parents: jj_parents,
                predecessors: vec![],
                root_tree: Merge::resolved(TreeId::from_bytes(&proto_commit.root_tree_id)),
                change_id: ChangeId::from_bytes(&proto_commit.change_id),
                description: proto_commit.description,
                author,
                committer,
                secure_sig: None,
                conflict_labels: Merge::resolved(String::new()),
            })
        })
    }

    async fn write_commit(
        &self,
        commit: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let root_tree_bytes = commit
            .root_tree
            .as_resolved()
            .map(|t| t.as_bytes().to_vec())
            .unwrap_or_else(|| vec![0u8; HASH_LENGTH]);

        let proto_commit = proto_backend::Commit {
            commit_id: vec![],
            change_id: commit.change_id.as_bytes().to_vec(),
            parent_commit_ids: commit.parents.iter().map(|p| p.as_bytes().to_vec()).collect(),
            root_tree_id: root_tree_bytes,
            description: commit.description.clone(),
            author: Some(proto_backend::Signature {
                name: commit.author.name.clone(),
                email: commit.author.email.clone(),
                timestamp_millis: commit.author.timestamp.timestamp.0,
            }),
            committer: Some(proto_backend::Signature {
                name: commit.committer.name.clone(),
                email: commit.committer.email.clone(),
                timestamp_millis: commit.committer.timestamp.timestamp.0,
            }),
        };

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        run_async(async move {
            let mut client = BackendServiceClient::connect(server_url)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            let resp = client
                .write_commit(make_request(proto_backend::WriteCommitRequest {
                    repo_id,
                    commit: Some(proto_commit),
                }))
                .await
                .map_err(|e| BackendError::Other(e.into()))?;


            let commit_id_bytes = resp.into_inner().commit_id;
            let commit_id = CommitId::from_bytes(&commit_id_bytes);

            Ok((commit_id, commit))
        })
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
