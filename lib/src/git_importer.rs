use cc_proto::backend::backend_service_client::BackendServiceClient;
use cc_proto::backend::{
    Commit, RegisterRepositoryRequest, Signature, TreeEntry, TreeEntryType, WriteCommitRequest,
    WriteFileRequest, WriteTreeRequest,
};
use cc_proto::op_heads_store::op_heads_store_service_client::OpHeadsStoreServiceClient;
use cc_proto::op_heads_store::AddOpHeadRequest;
use cc_proto::op_store::op_store_service_client::OpStoreServiceClient;
use cc_proto::op_store::{
    Operation, OperationMetadata, View, WriteOperationRequest, WriteViewRequest,
};
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

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

pub struct GitImporter {
    pub git_dir: PathBuf,
    pub repo_id: String,
    pub server_url: String,
}

impl GitImporter {
    pub fn new(git_dir: PathBuf, repo_id: String, server_url: String) -> Self {
        Self {
            git_dir,
            repo_id,
            server_url,
        }
    }

    pub async fn run(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let dot_git = if self.git_dir.file_name() == Some(std::ffi::OsStr::new(".git")) {
            self.git_dir.clone()
        } else {
            self.git_dir.join(".git")
        };

        let repo = gix::open(&dot_git)
            .or_else(|_| gix::open(&self.git_dir))
            .or_else(|_| gix::discover(&self.git_dir))
            .map_err(|e| format!("Failed to open Git repository at {:?}: {e}", self.git_dir))?;

        let mut backend_client = crate::util::connect_backend_client(self.server_url.clone())
            .await
            .map_err(|e| format!("Failed to connect to backend service: {e}"))?;

        let mut op_client = crate::util::connect_op_store_client(self.server_url.clone()).await?;
        let mut op_heads_client = crate::util::connect_op_heads_client(self.server_url.clone()).await?;

        // 1. Register or verify target repository on server
        let reg_res = backend_client
            .register_repository(make_request(RegisterRepositoryRequest {
                repo_id: self.repo_id.clone(),
            }))
            .await?
            .into_inner();

        let repo_id = reg_res.repo_id;

        // 2. Write JJ Root Commit (20 zero bytes)
        let root_commit = Commit {
            commit_id: vec![0u8; 20],
            change_id: vec![0u8; 16],
            parent_commit_ids: Vec::new(),
            root_tree_id: vec![0u8; 20],
            description: "".to_string(),
            author: None,
            committer: None,
        };
        let _ = backend_client
            .write_commit(make_request(WriteCommitRequest {
                repo_id: repo_id.clone(),
                commit: Some(root_commit),
            }))
            .await?;

        let mut blob_map: HashMap<gix::hash::ObjectId, Vec<u8>> = HashMap::new();
        let mut tree_map: HashMap<gix::hash::ObjectId, Vec<u8>> = HashMap::new();

        // Resolve HEAD / branch commit IDs
        let mut head_commit_ids = Vec::new();

        if let Ok(head_commit) = repo.head_commit() {
            head_commit_ids.push(head_commit.id.as_bytes().to_vec());
        }

        if head_commit_ids.is_empty() {
            if let Ok(head_id) = repo.head_id() {
                head_commit_ids.push(head_id.detach().as_bytes().to_vec());
            }
        }

        // Fallback for Jujutsu-colocated or reftable-format Git repos
        if head_commit_ids.is_empty() {
            if let Ok(platform) = repo.references() {
                if let Ok(all_refs) = platform.all() {
                    for r_res in all_refs {
                        if let Ok(r) = r_res {
                            if r.name().as_bstr() != "refs/heads/.invalid" {
                                if let Ok(peeled) = r.into_fully_peeled_id() {
                                    let bytes = peeled.detach().as_bytes().to_vec();
                                    if !head_commit_ids.contains(&bytes) {
                                        head_commit_ids.push(bytes);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // CLI fallback for reftable format repositories
        if head_commit_ids.is_empty() {
            if let Ok(output) = std::process::Command::new("git")
                .args([
                    "-C",
                    self.git_dir.to_str().unwrap_or("."),
                    "for-each-ref",
                    "--format=%(objectname)",
                ])
                .output()
            {
                if output.status.success() {
                    let text = String::from_utf8_lossy(&output.stdout);
                    for line in text.lines() {
                        let line = line.trim();
                        if let Ok(oid) = gix::hash::ObjectId::from_hex(line.as_bytes()) {
                            let bytes = oid.as_bytes().to_vec();
                            if !head_commit_ids.contains(&bytes) {
                                head_commit_ids.push(bytes);
                            }
                        }
                    }
                }
            }
        }

        if head_commit_ids.is_empty() {
            return Err("No active branch or commit references found in Git repository".into());
        }

        let starting_oids: Vec<gix::hash::ObjectId> = head_commit_ids
            .iter()
            .filter_map(|bytes| gix::hash::ObjectId::try_from(bytes.as_slice()).ok())
            .collect();

        let mut walk = repo.rev_walk(starting_oids).all()?;

        let mut commits_to_process = Vec::new();
        while let Some(info_res) = walk.next() {
            let info = info_res?;
            commits_to_process.push(info.id);
        }
        commits_to_process.reverse(); // Bottom-up processing

        for commit_oid in commits_to_process {
            let object = repo.find_object(commit_oid)?;
            let commit_ref = object.to_commit_ref();

            // Process root tree
            let tree_oid = commit_ref.tree();
            let root_tree_id = self
                .process_tree(
                    &repo,
                    tree_oid,
                    &mut backend_client,
                    &repo_id,
                    &mut blob_map,
                    &mut tree_map,
                )
                .await?;

            let author_sig = commit_ref.author().map_err(|e| format!("{e}"))?;
            let committer_sig = commit_ref.committer().map_err(|e| format!("{e}"))?;

            let mut parent_ids: Vec<Vec<u8>> =
                commit_ref.parents().map(|p| p.as_bytes().to_vec()).collect();

            // Root commits in Git must point to the JJ root commit
            if parent_ids.is_empty() {
                parent_ids.push(vec![0u8; 20]);
            }

            let author_time_millis = author_sig.time().map(|t| t.seconds * 1000).unwrap_or(0);
            let committer_time_millis = committer_sig.time().map(|t| t.seconds * 1000).unwrap_or(0);

            let jj_commit = Commit {
                commit_id: commit_oid.as_bytes().to_vec(),
                change_id: Uuid::new_v4().as_bytes().to_vec(),
                parent_commit_ids: parent_ids,
                root_tree_id,
                description: commit_ref.message.to_string(),
                author: Some(Signature {
                    name: author_sig.name.to_string(),
                    email: author_sig.email.to_string(),
                    timestamp_millis: author_time_millis,
                }),
                committer: Some(Signature {
                    name: committer_sig.name.to_string(),
                    email: committer_sig.email.to_string(),
                    timestamp_millis: committer_time_millis,
                }),
            };

            let _ = backend_client
                .write_commit(make_request(WriteCommitRequest {
                    repo_id: repo_id.clone(),
                    commit: Some(jj_commit),
                }))
                .await?;
        }

        // Create JJ View
        let view = View {
            view_id: Vec::new(),
            head_commit_ids: head_commit_ids.clone(),
            public_head_commit_ids: Vec::new(),
            wc_commit_ids: HashMap::new(),
            bookmarks: HashMap::new(),
            tags: HashMap::new(),
            git_refs: HashMap::new(),
        };

        let view_res = op_client
            .write_view(make_request(WriteViewRequest {
                repo_id: repo_id.clone(),
                view: Some(view),
            }))
            .await?
            .into_inner();

        let view_id = view_res.view_id;

        // Create JJ Operation & OpHead
        let operation = Operation {
            operation_id: Vec::new(),
            view_id,
            parent_op_ids: Vec::new(),
            metadata: Some(OperationMetadata {
                description: "Imported from Git repository".to_string(),
                username: "git-importer".to_string(),
                hostname: "localhost".to_string(),
                start_time_millis: 0,
                end_time_millis: 0,
                tags: HashMap::new(),
            }),
        };

        let op_res = op_client
            .write_operation(make_request(WriteOperationRequest {
                repo_id: repo_id.clone(),
                operation: Some(operation),
            }))
            .await?
            .into_inner();

        let op_id = op_res.operation_id;

        let _ = op_heads_client
            .add_op_head(make_request(AddOpHeadRequest {
                repo_id: repo_id.clone(),
                op_head_id: op_id,
            }))
            .await?;

        Ok(repo_id)
    }

    async fn process_tree(
        &self,
        repo: &gix::Repository,
        tree_oid: gix::hash::ObjectId,
        backend_client: &mut BackendServiceClient<tonic::transport::Channel>,
        repo_id: &str,
        blob_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
        tree_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(existing) = tree_map.get(&tree_oid) {
            return Ok(existing.clone());
        }

        let tree_obj = repo.find_object(tree_oid)?;
        let tree_ref = gix::objs::TreeRef::from_bytes(&tree_obj.data, tree_oid.kind())
            .map_err(|e| format!("Failed to parse tree: {e}"))?;

        let mut entries = Vec::new();

        for entry in tree_ref.entries {
            let name = entry.filename.to_string();
            let entry_oid = entry.oid.to_owned();

            let (entry_id, entry_type, executable) = if entry.mode.is_tree() {
                let child_tree_id = Box::pin(self.process_tree(
                    repo,
                    entry_oid,
                    backend_client,
                    repo_id,
                    blob_map,
                    tree_map,
                ))
                .await?;
                (child_tree_id, TreeEntryType::Tree, 0)
            } else if entry.mode.is_link() {
                (entry_oid.as_bytes().to_vec(), TreeEntryType::Symlink, 0)
            } else {
                let blob_id = if let Some(existing) = blob_map.get(&entry_oid) {
                    existing.clone()
                } else {
                    let blob_obj = repo.find_object(entry_oid)?;
                    let content = blob_obj.data.to_vec();
                    let write_res = backend_client
                        .write_file(make_request(WriteFileRequest {
                            repo_id: repo_id.to_string(),
                            content,
                        }))
                        .await?
                        .into_inner();
                    let written_id = write_res.file_id;
                    blob_map.insert(entry_oid, written_id.clone());
                    written_id
                };
                let is_exec = if entry.mode.is_executable() { 1 } else { 0 };
                (blob_id, TreeEntryType::File, is_exec)
            };

            entries.push(TreeEntry {
                name,
                entry_id,
                entry_type: entry_type as i32,
                executable,
            });
        }

        let write_tree_res = backend_client
            .write_tree(make_request(WriteTreeRequest {
                repo_id: repo_id.to_string(),
                path: "".to_string(),
                entries,
            }))
            .await?
            .into_inner();

        let written_tree_id = write_tree_res.tree_id;
        tree_map.insert(tree_oid, written_tree_id.clone());

        Ok(written_tree_id)
    }
}
