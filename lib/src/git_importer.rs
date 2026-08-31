use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::{
    tree_value, Commit, File, Signature, Timestamp, TreeEntry, TreeValue, WriteCommitRequest,
    WriteFileRequest, WriteTreeRequest,
};
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::{
    GetOpHeadsRequest, Operation, OperationMetadata, ReadOperationRequest, ReadViewRequest,
    RefTarget, RefTargetTerm, UpdateOpHeadsRequest, View, WriteOperationRequest, WriteViewRequest,
};
use cc_common::{EMPTY_TREE_ID_BYTES, ROOT_CHANGE_ID_BYTES, ROOT_COMMIT_ID_BYTES};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const CONCURRENT_BLOB_UPLOADS: usize = 32;

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

        Self::normalize_head_branch(&dot_git);

        let opts = gix::open::Options::isolated();
        let thread_safe_repo = gix::ThreadSafeRepository::open_opts(&dot_git, opts.clone())
            .or_else(|_| gix::ThreadSafeRepository::open_opts(&self.git_dir, opts.clone()))
            .or_else(|_| gix::ThreadSafeRepository::open(&dot_git))
            .or_else(|_| gix::ThreadSafeRepository::open(&self.git_dir))
            .or_else(|_| gix::ThreadSafeRepository::discover(&self.git_dir))
            .map_err(|e| format!("Failed to open Git repository at {:?}: {e}", self.git_dir))?;

        let mut backend_client = BackendServiceClient::connect(self.server_url.clone())
            .await
            .map_err(|e| format!("Failed to connect to backend service: {e}"))?;

        let mut op_client = OpStoreServiceClient::connect(self.server_url.clone())
            .await
            .map_err(|e| format!("Failed to connect to op store service: {e}"))?;

        let repo_id = self.repo_id.clone();

        // 1. Ensure Jujutsu Root Commit exists on server
        self.write_root_commit(&mut backend_client, &repo_id).await?;

        // 2. Discover HEAD commit IDs and collect commits & blobs to upload
        let mut blob_map: HashMap<gix::hash::ObjectId, Vec<u8>> = HashMap::new();
        let (head_commit_ids, commits_to_process, blobs_to_upload) =
            self.collect_git_objects(&thread_safe_repo, &dot_git, &mut blob_map)?;

        // 3. Concurrently upload file blobs
        self.upload_blobs(&mut backend_client, &repo_id, blobs_to_upload, &mut blob_map).await?;

        // 4. Convert and upload commit graph
        let mut tree_map: HashMap<gix::hash::ObjectId, Vec<u8>> = HashMap::new();
        for commit_oid in commits_to_process {
            self.import_commit(
                &thread_safe_repo,
                commit_oid,
                &mut backend_client,
                &repo_id,
                &mut blob_map,
                &mut tree_map,
            )
            .await?;
        }

        // 5. Update Op Store View and Operations
        self.publish_op_head(&mut op_client, &repo_id, &head_commit_ids).await?;

        Ok(repo_id)
    }

    /// Normalizes HEAD ref if it points to an invalid branch name (.invalid from templates)
    fn normalize_head_branch(dot_git: &Path) {
        let head_path = dot_git.join("HEAD");
        if let Ok(head_str) = std::fs::read_to_string(&head_path) {
            if head_str.contains(".invalid") {
                let invalid_ref = dot_git.join("refs/heads/.invalid");
                let main_ref = dot_git.join("refs/heads/main");
                if invalid_ref.exists() && !main_ref.exists() {
                    let _ = std::fs::rename(&invalid_ref, &main_ref);
                }
                let _ = std::fs::write(&head_path, "ref: refs/heads/main\n");
            }
        }
    }

    /// Writes the synthetic Jujutsu root commit
    async fn write_root_commit(
        &self,
        backend_client: &mut BackendServiceClient<tonic::transport::Channel>,
        repo_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let root_commit = Commit {
            commit_id: ROOT_COMMIT_ID_BYTES.to_vec(),
            change_id: ROOT_CHANGE_ID_BYTES.to_vec(),
            parent_commit_ids: Vec::new(),
            root_tree_id: vec![EMPTY_TREE_ID_BYTES.to_vec()],
            description: "".to_string(),
            author: None,
            committer: None,
            predecessors: Vec::new(),
            conflict_labels: Vec::new(),
            secure_sig: None,
        };
        let _ = backend_client
            .write_commit(make_request(WriteCommitRequest {
                repo_id: repo_id.to_string(),
                commit: Some(root_commit),
            }))
            .await?;
        Ok(())
    }

    /// Resolves head commit IDs and collects commits & blobs to process
    fn collect_git_objects(
        &self,
        thread_safe_repo: &gix::ThreadSafeRepository,
        dot_git: &Path,
        blob_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
    ) -> Result<
        (
            Vec<Vec<u8>>,
            Vec<gix::hash::ObjectId>,
            Vec<(gix::hash::ObjectId, Vec<u8>)>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let repo = thread_safe_repo.to_thread_local();
        let head_commit_ids = self.discover_head_commit_ids(&repo, dot_git);

        // Collect all reachable commits in topological order (parents before children)
        let mut commits_to_process = Vec::new();
        let mut visited = HashSet::new();

        for head in &head_commit_ids {
            let head_oid = gix::hash::ObjectId::try_from(head.as_slice())?;
            let mut stack = vec![head_oid];

            while let Some(oid) = stack.pop() {
                if visited.insert(oid) {
                    commits_to_process.push(oid);
                    if let Ok(obj) = repo.find_object(oid) {
                        let commit_ref = obj.to_commit_ref();
                        for parent in commit_ref.parents() {
                            stack.push(parent);
                        }
                    }
                }
            }
        }
        commits_to_process.reverse();

        // Collect all file blobs for parallel upload
        let mut blobs_to_upload = Vec::new();
        for commit_oid in &commits_to_process {
            let obj = repo.find_object(*commit_oid)?;
            let commit_ref = obj.to_commit_ref();
            let mut tree_stack = vec![commit_ref.tree()];

            while let Some(tree_oid) = tree_stack.pop() {
                if let Ok(tree_obj) = repo.find_object(tree_oid) {
                    if let Ok(tree_ref) = gix::objs::TreeRef::from_bytes(&tree_obj.data) {
                        for entry in tree_ref.entries {
                            if entry.mode.is_tree() {
                                tree_stack.push(entry.oid.to_owned());
                            } else if !entry.mode.is_link() {
                                if !blob_map.contains_key(&entry.oid.to_owned()) {
                                    if let Ok(blob_obj) = repo.find_object(entry.oid) {
                                        blobs_to_upload
                                            .push((entry.oid.to_owned(), blob_obj.data.to_vec()));
                                        blob_map.insert(entry.oid.to_owned(), Vec::new());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok((head_commit_ids, commits_to_process, blobs_to_upload))
    }

    /// Discovers all head commit IDs across loose refs, packed refs, HEAD, and git CLI
    fn discover_head_commit_ids(&self, repo: &gix::Repository, dot_git: &Path) -> Vec<Vec<u8>> {
        let mut head_commit_ids = Vec::new();

        fn walk_refs(dir: &Path, out: &mut Vec<Vec<u8>>) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        walk_refs(&path, out);
                    } else if path.is_file() {
                        if let Ok(content) = std::fs::read_to_string(&path) {
                            if let Ok(oid) = gix::hash::ObjectId::from_hex(content.trim().as_bytes()) {
                                let bytes = oid.as_bytes().to_vec();
                                if !out.contains(&bytes) {
                                    out.push(bytes);
                                }
                            }
                        }
                    }
                }
            }
        }

        walk_refs(&dot_git.join("refs"), &mut head_commit_ids);

        if let Ok(packed) = std::fs::read_to_string(dot_git.join("packed-refs")) {
            for line in packed.lines() {
                let line = line.trim();
                if !line.starts_with('#') && !line.starts_with('^') {
                    if let Some((hash, _name)) = line.split_once(' ') {
                        if let Ok(oid) = gix::hash::ObjectId::from_hex(hash.trim().as_bytes()) {
                            let bytes = oid.as_bytes().to_vec();
                            if !head_commit_ids.contains(&bytes) {
                                head_commit_ids.push(bytes);
                            }
                        }
                    }
                }
            }
        }

        if let Ok(head_str) = std::fs::read_to_string(dot_git.join("HEAD")) {
            let head_str = head_str.trim();
            if let Some(target_ref) = head_str.strip_prefix("ref: ") {
                let ref_path = dot_git.join(target_ref.trim());
                if let Ok(ref_str) = std::fs::read_to_string(ref_path) {
                    if let Ok(oid) = gix::hash::ObjectId::from_hex(ref_str.trim().as_bytes()) {
                        let bytes = oid.as_bytes().to_vec();
                        if !head_commit_ids.contains(&bytes) {
                            head_commit_ids.push(bytes);
                        }
                    }
                }
            } else if let Ok(oid) = gix::hash::ObjectId::from_hex(head_str.as_bytes()) {
                let bytes = oid.as_bytes().to_vec();
                if !head_commit_ids.contains(&bytes) {
                    head_commit_ids.push(bytes);
                }
            }
        }

        if head_commit_ids.is_empty() {
            if let Ok(head_commit) = repo.head_commit() {
                let bytes = head_commit.id.as_bytes().to_vec();
                if !head_commit_ids.contains(&bytes) {
                    head_commit_ids.push(bytes);
                }
            }
        }

        // Git CLI fallback (for reftable or custom backends)
        if head_commit_ids.is_empty() {
            if let Ok(output) = std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&self.git_dir)
                .output()
            {
                if output.status.success() {
                    let hash_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if let Ok(oid) = gix::hash::ObjectId::from_hex(hash_str.as_bytes()) {
                        let bytes = oid.as_bytes().to_vec();
                        if !head_commit_ids.contains(&bytes) {
                            head_commit_ids.push(bytes);
                        }
                    }
                }
            }
        }

        if head_commit_ids.is_empty() {
            if let Ok(output) = std::process::Command::new("git")
                .args(["for-each-ref", "--format=%(objectname)"])
                .current_dir(&self.git_dir)
                .output()
            {
                if output.status.success() {
                    for line in String::from_utf8_lossy(&output.stdout).lines() {
                        let hash_str = line.trim();
                        if let Ok(oid) = gix::hash::ObjectId::from_hex(hash_str.as_bytes()) {
                            let bytes = oid.as_bytes().to_vec();
                            if !head_commit_ids.contains(&bytes) {
                                head_commit_ids.push(bytes);
                            }
                        }
                    }
                }
            }
        }

        head_commit_ids
    }

    /// Uploads file blobs concurrently using parallel workers over multiplexed HTTP/2 channel
    async fn upload_blobs(
        &self,
        backend_client: &mut BackendServiceClient<tonic::transport::Channel>,
        repo_id: &str,
        blobs_to_upload: Vec<(gix::hash::ObjectId, Vec<u8>)>,
        blob_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let upload_stream =
            futures::stream::iter(blobs_to_upload.into_iter().map(|(blob_oid, content)| {
                let mut client = backend_client.clone();
                let r_id = repo_id.to_string();
                async move {
                    let write_res = client
                        .write_file(make_request(WriteFileRequest {
                            repo_id: r_id,
                            content,
                        }))
                        .await?
                        .into_inner();
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                        blob_oid,
                        write_res.file_id,
                    ))
                }
            }))
            .buffer_unordered(CONCURRENT_BLOB_UPLOADS);

        let uploaded_blobs: Vec<Result<(gix::hash::ObjectId, Vec<u8>), _>> =
            upload_stream.collect().await;
        for res in uploaded_blobs {
            let (blob_oid, file_id) = res?;
            blob_map.insert(blob_oid, file_id);
        }
        Ok(())
    }

    /// Imports a single commit into Jujutsu Commit Cloud format
    async fn import_commit(
        &self,
        thread_safe_repo: &gix::ThreadSafeRepository,
        commit_oid: gix::hash::ObjectId,
        backend_client: &mut BackendServiceClient<tonic::transport::Channel>,
        repo_id: &str,
        blob_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
        tree_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (
            tree_oid,
            author_sig_name,
            author_sig_email,
            author_time_millis,
            author_tz_offset,
            committer_sig_name,
            committer_sig_email,
            committer_time_millis,
            committer_tz_offset,
            mut parent_ids,
            message,
        ) = {
            let repo = thread_safe_repo.to_thread_local();
            let object = repo.find_object(commit_oid)?;
            let commit_ref = object.to_commit_ref();

            let tree_oid = commit_ref.tree();
            let author_sig = commit_ref.author();
            let committer_sig = commit_ref.committer();

            let parent_ids: Vec<Vec<u8>> =
                commit_ref.parents().map(|p| p.as_bytes().to_vec()).collect();

            let author_time_millis = author_sig.time.seconds * 1000;
            let author_tz_offset = author_sig.time.offset / 60;
            let committer_time_millis = committer_sig.time.seconds * 1000;
            let committer_tz_offset = committer_sig.time.offset / 60;

            (
                tree_oid,
                author_sig.name.to_string(),
                author_sig.email.to_string(),
                author_time_millis,
                author_tz_offset,
                committer_sig.name.to_string(),
                committer_sig.email.to_string(),
                committer_time_millis,
                committer_tz_offset,
                parent_ids,
                commit_ref.message.to_string(),
            )
        };

        // Git root commits must attach to JJ's universal root commit
        if parent_ids.is_empty() {
            parent_ids.push(ROOT_COMMIT_ID_BYTES.to_vec());
        }

        // Process root tree
        let root_tree_id = self
            .process_tree(
                thread_safe_repo,
                tree_oid,
                backend_client,
                repo_id,
                blob_map,
                tree_map,
            )
            .await?;

        let jj_commit = Commit {
            commit_id: commit_oid.as_bytes().to_vec(),
            change_id: Uuid::new_v4().as_bytes().to_vec(),
            parent_commit_ids: parent_ids,
            root_tree_id: vec![root_tree_id],
            description: message,
            author: Some(Signature {
                name: author_sig_name,
                email: author_sig_email,
                timestamp: Some(Timestamp {
                    millis_since_epoch: author_time_millis,
                    tz_offset: author_tz_offset,
                }),
            }),
            committer: Some(Signature {
                name: committer_sig_name,
                email: committer_sig_email,
                timestamp: Some(Timestamp {
                    millis_since_epoch: committer_time_millis,
                    tz_offset: committer_tz_offset,
                }),
            }),
            predecessors: Vec::new(),
            conflict_labels: Vec::new(),
            secure_sig: None,
        };

        backend_client
            .write_commit(make_request(WriteCommitRequest {
                repo_id: repo_id.to_string(),
                commit: Some(jj_commit),
            }))
            .await?;

        Ok(())
    }

    /// Recursively processes Git tree objects into Commit Cloud tree protos
    async fn process_tree(
        &self,
        thread_safe_repo: &gix::ThreadSafeRepository,
        tree_oid: gix::hash::ObjectId,
        backend_client: &mut BackendServiceClient<tonic::transport::Channel>,
        repo_id: &str,
        blob_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
        tree_map: &mut HashMap<gix::hash::ObjectId, Vec<u8>>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(existing) = tree_map.get(&tree_oid) {
            return Ok(existing.clone());
        }

        let raw_entries = {
            let repo = thread_safe_repo.to_thread_local();
            let tree_obj = repo.find_object(tree_oid)?;
            let tree_ref = gix::objs::TreeRef::from_bytes(&tree_obj.data)
                .map_err(|e| format!("Failed to parse tree: {e}"))?;

            let mut raw = Vec::new();
            for entry in tree_ref.entries {
                let name = entry.filename.to_string();
                let entry_oid = entry.oid.to_owned();
                let is_tree = entry.mode.is_tree();
                let is_link = entry.mode.is_link();
                let is_exec = entry.mode.is_executable();
                let blob_data = if !is_tree && !is_link && !blob_map.contains_key(&entry_oid) {
                    let blob_obj = repo.find_object(entry_oid)?;
                    Some(blob_obj.data.to_vec())
                } else {
                    None
                };
                raw.push((name, entry_oid, is_tree, is_link, is_exec, blob_data));
            }
            raw
        };

        let mut entries = Vec::new();

        for (name, entry_oid, is_tree, is_link, is_exec, blob_data) in raw_entries {
            let tree_value = if is_tree {
                let child_tree_id = Box::pin(self.process_tree(
                    thread_safe_repo,
                    entry_oid,
                    backend_client,
                    repo_id,
                    blob_map,
                    tree_map,
                ))
                .await?;
                TreeValue {
                    value: Some(tree_value::Value::TreeId(child_tree_id)),
                }
            } else if is_link {
                TreeValue {
                    value: Some(tree_value::Value::SymlinkId(entry_oid.as_bytes().to_vec())),
                }
            } else {
                let blob_id = if let Some(existing) = blob_map.get(&entry_oid) {
                    if existing.is_empty() {
                        let content = blob_data.unwrap_or_default();
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
                    } else {
                        existing.clone()
                    }
                } else {
                    let content = blob_data.unwrap_or_default();
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
                TreeValue {
                    value: Some(tree_value::Value::File(File {
                        id: blob_id,
                        executable: is_exec,
                        copy_id: Vec::new(),
                    })),
                }
            };

            entries.push(TreeEntry {
                name,
                value: Some(tree_value),
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

    /// Chains the imported commit history into the Op Store View and Operations
    async fn publish_op_head(
        &self,
        op_client: &mut OpStoreServiceClient<tonic::transport::Channel>,
        repo_id: &str,
        head_commit_ids: &[Vec<u8>],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Fetch existing op heads from server so we chain operations
        let op_heads_res = op_client
            .get_op_heads(make_request(GetOpHeadsRequest {
                repo_id: repo_id.to_string(),
            }))
            .await?
            .into_inner();
        let old_op_head_ids = op_heads_res.op_head_ids;

        let mut view = View {
            head_ids: Vec::new(),
            wc_commit_ids: HashMap::new(),
            local_bookmarks: HashMap::new(),
            remote_bookmarks: HashMap::new(),
        };

        // If an operation head exists, inherit its existing view state
        for op_head in &old_op_head_ids {
            if let Ok(op_res) = op_client
                .read_operation(make_request(ReadOperationRequest {
                    repo_id: repo_id.to_string(),
                    operation_id: op_head.clone(),
                }))
                .await
            {
                if let Some(op) = op_res.into_inner().operation {
                    if let Ok(view_res) = op_client
                        .read_view(make_request(ReadViewRequest {
                            repo_id: repo_id.to_string(),
                            view_id: op.view_id,
                        }))
                        .await
                    {
                        if let Some(v) = view_res.into_inner().view {
                            view = v;
                            break;
                        }
                    }
                }
            }
        }

        // Add imported head commit IDs
        for head in head_commit_ids {
            if !view.head_ids.contains(head) {
                view.head_ids.push(head.clone());
            }
        }

        // Point 'main' bookmark to the imported primary head
        if let Some(primary_head) = head_commit_ids.first() {
            view.local_bookmarks.insert(
                "main".to_string(),
                RefTarget {
                    adds: vec![RefTargetTerm {
                        commit_id: primary_head.clone(),
                    }],
                    removes: Vec::new(),
                },
            );
        }

        let view_res = op_client
            .write_view(make_request(WriteViewRequest {
                repo_id: repo_id.to_string(),
                view: Some(view),
            }))
            .await?
            .into_inner();

        let operation = Operation {
            view_id: view_res.view_id,
            parents: old_op_head_ids.clone(),
            metadata: Some(OperationMetadata {
                start_time_millis: 0,
                end_time_millis: 0,
                description: "Imported from Git repository".to_string(),
                is_snapshot: false,
                workspace_name: None,
                hostname: "localhost".to_string(),
                username: "git-importer".to_string(),
                attributes: HashMap::new(),
            }),
            commit_predecessors: Vec::new(),
            commit_predecessors_set: false,
        };

        let op_res = op_client
            .write_operation(make_request(WriteOperationRequest {
                repo_id: repo_id.to_string(),
                operation: Some(operation),
            }))
            .await?
            .into_inner();

        let _ = op_client
            .update_op_heads(make_request(UpdateOpHeadsRequest {
                repo_id: repo_id.to_string(),
                old_op_head_ids,
                new_op_head_id: op_res.operation_id,
            }))
            .await?;

        Ok(())
    }
}
