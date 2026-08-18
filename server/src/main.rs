// TODO: Refactor hashing helper functions into a shared hashing utils file.

use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Mutex;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use cc_common::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_common::backend::*;
use cc_common::op_store::op_store_service_server::{OpStoreService, OpStoreServiceServer};
use cc_common::op_store::*;

const EMPTY_STRING_PLACEHOLDER: &str = "JJ_EMPTY_STRING";

fn signature_to_git(sig: Option<&cc_common::backend::Signature>) -> gix::actor::Signature {
    let (name, email) = match sig {
        Some(s) => (
            if s.name.is_empty() { EMPTY_STRING_PLACEHOLDER } else { &s.name },
            if s.email.is_empty() { EMPTY_STRING_PLACEHOLDER } else { &s.email },
        ),
        None => (EMPTY_STRING_PLACEHOLDER, EMPTY_STRING_PLACEHOLDER),
    };
    let (secs, offset_mins) = sig
        .and_then(|s| s.timestamp.as_ref())
        .map_or((0, 0), |t| (t.millis_since_epoch / 1000, t.tz_offset));

    gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(secs, offset_mins * 60),
    }
}

/// Standalone black-box function that computes a Git commit hash using `gix` (Gitoxide).
pub fn compute_git_commit_hash(commit: &cc_common::backend::Commit) -> Vec<u8> {
    use gix::objs::WriteTo;

    let tree_id = commit
        .root_tree_id
        .first()
        .and_then(|id| gix::hash::ObjectId::try_from(id.as_slice()).ok())
        .unwrap_or_else(|| gix::hash::ObjectId::empty_tree(gix::hash::Kind::Sha1));

    // Exclude Jujutsu's root commit ID (Git root commits have 0 parents in Git representation)
    let parents: Vec<gix::hash::ObjectId> = commit
        .parent_commit_ids
        .iter()
        .filter(|id| id.as_slice() != cc_common::ROOT_COMMIT_ID_BYTES)
        .filter_map(|id| gix::hash::ObjectId::try_from(id.as_slice()).ok())
        .collect();

    let mut extra_headers = Vec::new();
    if !commit.change_id.is_empty() {
        let mut rev_change_id = commit.change_id.clone();
        rev_change_id.reverse();
        let hex_str = rev_change_id.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        extra_headers.push(("change-id".into(), hex_str.into()));
    }

    let gix_commit = gix::objs::Commit {
        tree: tree_id,
        parents: parents.into(),
        author: signature_to_git(commit.author.as_ref()),
        committer: signature_to_git(commit.committer.as_ref()),
        encoding: None,
        extra_headers,
        message: commit.description.as_bytes().into(),
    };

    let mut buf = Vec::new();
    gix_commit.write_to(&mut buf).expect("gix commit should have serialized successfully");
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Commit, &buf);
    hash.as_bytes().to_vec()
}

/// Standalone black-box function that computes a Git blob hash using `gix` (Gitoxide).
pub fn compute_git_blob_hash(content: &[u8]) -> Vec<u8> {
    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, content);
    hash.as_bytes().to_vec()
}

/// Standalone black-box function that computes a Git tree hash using `gix` (Gitoxide).
pub fn compute_git_tree_hash(entries: &[cc_common::backend::TreeEntry]) -> Vec<u8> {
    use gix::objs::WriteTo;

    let mut gix_entries = Vec::new();
    for entry in entries {
        let (kind, id_bytes) = match entry.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(cc_common::backend::tree_value::Value::File(f)) => {
                let k = if f.executable { gix::objs::tree::EntryKind::BlobExecutable } else { gix::objs::tree::EntryKind::Blob };
                (k, &f.id[..])
            }
            Some(cc_common::backend::tree_value::Value::TreeId(id)) => (gix::objs::tree::EntryKind::Tree, &id[..]),
            Some(cc_common::backend::tree_value::Value::SymlinkId(id)) => (gix::objs::tree::EntryKind::Link, &id[..]),
            _ => (gix::objs::tree::EntryKind::Blob, &cc_common::ROOT_COMMIT_ID_BYTES[..]),
        };
        if let Ok(oid) = gix::hash::ObjectId::try_from(id_bytes) {
            gix_entries.push(gix::objs::tree::Entry {
                mode: kind.into(),
                filename: entry.name.as_bytes().into(),
                oid,
            });
        }
    }

    // Sort entries according to Git canonical tree entry ordering rules (gix::objs::tree::Entry implements Ord for Git tree sorting).
    gix_entries.sort_unstable();

    let gix_tree = gix::objs::Tree { entries: gix_entries };
    let mut buf = Vec::new();
    gix_tree.write_to(&mut buf).expect("gix tree should have serialized successfully");

    let hash = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Tree, &buf);
    hash.as_bytes().to_vec()
}

#[derive(Parser, Debug)]
#[command(name = "jj-cc-server", about = "Jujutsu Commit Cloud Server")]
struct Args {
    /// Host interface to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on (use 0 for ephemeral port assignment)
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

#[derive(Debug, Default)]
pub struct CommitCloudServerImpl {
    repos: Mutex<HashSet<String>>,
    commits: Mutex<HashMap<String, HashMap<Vec<u8>, Commit>>>,
    trees: Mutex<HashMap<String, HashMap<Vec<u8>, Vec<TreeEntry>>>>,
    files: Mutex<HashMap<String, HashMap<Vec<u8>, Vec<u8>>>>,
    operations: Mutex<HashMap<String, HashMap<Vec<u8>, Operation>>>,
    views: Mutex<HashMap<String, HashMap<Vec<u8>, View>>>,
    op_heads: Mutex<HashMap<String, Vec<Vec<u8>>>>,
}

#[tonic::async_trait]
impl BackendService for CommitCloudServerImpl {
    async fn register_repository(
        &self,
        request: tonic::Request<RegisterRepositoryRequest>,
    ) -> Result<tonic::Response<RegisterRepositoryResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Registering repository: {}", req.repo_id);
        let mut repos = self.repos.lock().unwrap();
        repos.insert(req.repo_id.clone());
        Ok(tonic::Response::new(RegisterRepositoryResponse {
            repo_id: req.repo_id,
        }))
    }

    async fn read_commit(
        &self,
        request: tonic::Request<ReadCommitRequest>,
    ) -> Result<tonic::Response<ReadCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;
        let commit_id = req.commit_id;

        if !self.repos.lock().unwrap().contains(&repo_id) {
            return Err(tonic::Status::not_found("repository should have been registered before requesting commits"));
        }

        let commits = self.commits.lock().unwrap();
        if let Some(repo_commits) = commits.get(&repo_id) {
            if let Some(commit) = repo_commits.get(&commit_id) {
                return Ok(tonic::Response::new(ReadCommitResponse {
                    commit: Some(commit.clone()),
                }));
            }
        }
        Err(tonic::Status::not_found("commit should have been present in cloud database"))
    }

    async fn write_commit(
        &self,
        request: tonic::Request<WriteCommitRequest>,
    ) -> Result<tonic::Response<WriteCommitResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        if !self.repos.lock().unwrap().contains(&repo_id) {
            return Err(tonic::Status::not_found("repository should have been registered before requesting commits"));
        }

        let mut commit = req.commit.ok_or_else(|| tonic::Status::invalid_argument("request should have contained commit data"))?;
        let commit_id = if commit.commit_id.is_empty() {
            compute_git_commit_hash(&commit)
        } else {
            commit.commit_id.clone()
        };
        commit.commit_id = commit_id.clone();
        info!("Writing commit {:?} for repo {}", commit_id, repo_id);

        let mut commits = self.commits.lock().unwrap();
        let repo_commits = commits.entry(repo_id).or_default();
        repo_commits.insert(commit_id.clone(), commit);

        Ok(tonic::Response::new(WriteCommitResponse { commit_id }))
    }

    async fn read_tree(
        &self,
        request: tonic::Request<ReadTreeRequest>,
    ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;
        let tree_id = req.tree_id;

        if !self.repos.lock().unwrap().contains(&repo_id) {
            return Err(tonic::Status::not_found("repository should have been registered before requesting trees"));
        }

        if tree_id == cc_common::EMPTY_TREE_ID_BYTES {
            return Ok(tonic::Response::new(ReadTreeResponse {
                tree_id,
                entries: vec![],
            }));
        }

        let trees = self.trees.lock().unwrap();
        if let Some(repo_trees) = trees.get(&repo_id) {
            if let Some(entries) = repo_trees.get(&tree_id) {
                return Ok(tonic::Response::new(ReadTreeResponse {
                    tree_id,
                    entries: entries.clone(),
                }));
            }
        }
        Err(tonic::Status::not_found("tree should have been present in cloud database"))
    }

    async fn write_tree(
        &self,
        request: tonic::Request<WriteTreeRequest>,
    ) -> Result<tonic::Response<WriteTreeResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        if !self.repos.lock().unwrap().contains(&repo_id) {
            return Err(tonic::Status::not_found("repository should have been registered before writing trees"));
        }

        let tree_id = compute_git_tree_hash(&req.entries);

        let mut trees = self.trees.lock().unwrap();
        let repo_trees = trees.entry(repo_id).or_default();
        repo_trees.insert(tree_id.clone(), req.entries);

        Ok(tonic::Response::new(WriteTreeResponse { tree_id }))
    }

    type ReadFileStream = tokio_stream::wrappers::ReceiverStream<Result<ReadFileResponse, tonic::Status>>;

    async fn read_file(
        &self,
        request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;
        let file_id = req.file_id;

        if !self.repos.lock().unwrap().contains(&repo_id) {
            return Err(tonic::Status::not_found("repository should have been registered before reading files"));
        }

        let files = self.files.lock().unwrap();
        let content = files
            .get(&repo_id)
            .and_then(|repo_files| repo_files.get(&file_id))
            .cloned()
            .ok_or_else(|| tonic::Status::not_found("file should have been present in cloud database"))?;

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx.send(Ok(ReadFileResponse { chunk: content })).await;
        });

        Ok(tonic::Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    // TODO: Upgrade write_file RPC handler to consume tonic::Streaming<WriteFileRequest>
    // to handle chunked streaming uploads for large files (>4MB) without hitting gRPC limits.
    async fn write_file(
        &self,
        request: tonic::Request<WriteFileRequest>,
    ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
        let req = request.into_inner();
        let repo_id = req.repo_id;

        if !self.repos.lock().unwrap().contains(&repo_id) {
            return Err(tonic::Status::not_found("repository should have been registered before writing files"));
        }

        let file_id = compute_git_blob_hash(&req.content);

        let mut files = self.files.lock().unwrap();
        let repo_files = files.entry(repo_id).or_default();
        repo_files.insert(file_id.clone(), req.content);

        Ok(tonic::Response::new(WriteFileResponse { file_id }))
    }

    async fn read_symlink(
        &self,
        _request: tonic::Request<ReadSymlinkRequest>,
    ) -> Result<tonic::Response<ReadSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn write_symlink(
        &self,
        _request: tonic::Request<WriteSymlinkRequest>,
    ) -> Result<tonic::Response<WriteSymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
}

#[tonic::async_trait]
impl OpStoreService for CommitCloudServerImpl {
    async fn read_operation(
        &self,
        request: tonic::Request<ReadOperationRequest>,
    ) -> Result<tonic::Response<ReadOperationResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Reading operation: {} for repo: {}", hex::encode(&req.operation_id), req.repo_id);

        let repos = self.repos.lock().unwrap();
        if !repos.contains(&req.repo_id) {
            return Err(tonic::Status::not_found(format!("repository should have been registered before reading operations: {}", req.repo_id)));
        }

        let operations = self.operations.lock().unwrap();
        if let Some(repo_ops) = operations.get(&req.repo_id) {
            if let Some(op) = repo_ops.get(&req.operation_id) {
                return Ok(tonic::Response::new(ReadOperationResponse {
                    operation: Some(op.clone()),
                }));
            }
        }

        if req.operation_id == cc_common::ROOT_OPERATION_ID_BYTES {
            let root_op = Operation {
                view_id: cc_common::ROOT_VIEW_ID_BYTES.to_vec(),
                parents: vec![],
                metadata: Some(OperationMetadata {
                    start_time_millis: 0,
                    end_time_millis: 0,
                    description: "root()".to_string(),
                    hostname: "".to_string(),
                    username: "".to_string(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: HashMap::new(),
                }),
                commit_predecessors: vec![],
                commit_predecessors_set: true,
            };
            return Ok(tonic::Response::new(ReadOperationResponse {
                operation: Some(root_op),
            }));
        }

        Err(tonic::Status::not_found(format!(
            "operation should have been present in cloud database: {} in repo: {}",
            hex::encode(&req.operation_id),
            req.repo_id
        )))
    }

    async fn write_operation(
        &self,
        request: tonic::Request<WriteOperationRequest>,
    ) -> Result<tonic::Response<WriteOperationResponse>, tonic::Status> {
        let req = request.into_inner();
        let op = req
            .operation
            .ok_or_else(|| tonic::Status::invalid_argument("request should have contained an operation object"))?;

        info!("Writing operation for repo: {}", req.repo_id);

        let repos = self.repos.lock().unwrap();
        if !repos.contains(&req.repo_id) {
            return Err(tonic::Status::not_found(format!("repository should have been registered before writing operations: {}", req.repo_id)));
        }

        // Sort keys for deterministic op_id generation over full operation payload
        let mut buf = Vec::new();
        buf.extend_from_slice(&op.view_id);
        for p in &op.parents {
            buf.extend_from_slice(p);
        }
        if let Some(meta) = &op.metadata {
            buf.extend_from_slice(&meta.start_time_millis.to_le_bytes());
            buf.extend_from_slice(&meta.end_time_millis.to_le_bytes());
            buf.extend_from_slice(meta.description.as_bytes());
            buf.extend_from_slice(meta.hostname.as_bytes());
            buf.extend_from_slice(meta.username.as_bytes());
            buf.extend_from_slice(&(meta.is_snapshot as u8).to_le_bytes());
            if let Some(ws) = &meta.workspace_name {
                buf.extend_from_slice(ws.as_bytes());
            }
            let sorted_attrs: std::collections::BTreeMap<_, _> = meta.attributes.iter().collect();
            for (k, v) in sorted_attrs {
                buf.extend_from_slice(k.as_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
        }
        for pred in &op.commit_predecessors {
            buf.extend_from_slice(&pred.commit_id);
            for p_id in &pred.predecessor_ids {
                buf.extend_from_slice(p_id);
            }
        }
        let op_id = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf).as_bytes()[..cc_common::OPERATION_ID_LENGTH].to_vec();

        let mut operations = self.operations.lock().unwrap();
        let repo_ops = operations.entry(req.repo_id).or_default();
        repo_ops.insert(op_id.clone(), op);

        Ok(tonic::Response::new(WriteOperationResponse {
            operation_id: op_id,
        }))
    }

    async fn read_view(
        &self,
        request: tonic::Request<ReadViewRequest>,
    ) -> Result<tonic::Response<ReadViewResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Reading view: {} for repo: {}", hex::encode(&req.view_id), req.repo_id);

        let repos = self.repos.lock().unwrap();
        if !repos.contains(&req.repo_id) {
            return Err(tonic::Status::not_found(format!("repository should have been registered before reading views: {}", req.repo_id)));
        }

        let views = self.views.lock().unwrap();
        if let Some(repo_views) = views.get(&req.repo_id) {
            if let Some(v) = repo_views.get(&req.view_id) {
                return Ok(tonic::Response::new(ReadViewResponse {
                    view_id: req.view_id.clone(),
                    view: Some(v.clone()),
                }));
            }
        }

        if req.view_id == cc_common::ROOT_VIEW_ID_BYTES {
            let root_view = View {
                head_ids: vec![cc_common::ROOT_COMMIT_ID_BYTES.to_vec()],
                wc_commit_ids: HashMap::new(),
                local_bookmarks: HashMap::new(),
                remote_bookmarks: HashMap::new(),
            };
            return Ok(tonic::Response::new(ReadViewResponse {
                view_id: req.view_id.clone(),
                view: Some(root_view),
            }));
        }

        Err(tonic::Status::not_found(format!(
            "view should have been present in cloud database: {} in repo: {}",
            hex::encode(&req.view_id),
            req.repo_id
        )))
    }

    async fn write_view(
        &self,
        request: tonic::Request<WriteViewRequest>,
    ) -> Result<tonic::Response<WriteViewResponse>, tonic::Status> {
        let req = request.into_inner();
        let view = req
            .view
            .ok_or_else(|| tonic::Status::invalid_argument("request should have contained a view object"))?;

        info!("Writing view for repo: {}", req.repo_id);

        let repos = self.repos.lock().unwrap();
        if !repos.contains(&req.repo_id) {
            return Err(tonic::Status::not_found(format!("repository should have been registered before writing views: {}", req.repo_id)));
        }

        // Sort keys for deterministic view_id generation. Since our Proto definition uses Map<>
        // and does not guarantee any orderding of keys.
        let mut buf = Vec::new();
        let mut head_ids = view.head_ids.clone();
        head_ids.sort();
        for head in &head_ids {
            buf.extend_from_slice(head);
        }

        let sorted_wc: std::collections::BTreeMap<_, _> = view.wc_commit_ids.iter().collect();
        for (k, v) in sorted_wc {
            buf.extend_from_slice(k.as_bytes());
            buf.extend_from_slice(v);
        }

        let append_ref_target = |buf: &mut Vec<u8>, target: &cc_common::op_store::RefTarget| {
            let mut removes: Vec<_> = target.removes.iter().map(|t| &t.commit_id).collect();
            removes.sort();
            for commit_id in removes {
                buf.extend_from_slice(commit_id);
            }
            let mut adds: Vec<_> = target.adds.iter().map(|t| &t.commit_id).collect();
            adds.sort();
            for commit_id in adds {
                buf.extend_from_slice(commit_id);
            }
        };

        let sorted_bookmarks: std::collections::BTreeMap<_, _> = view.local_bookmarks.iter().collect();
        for (name, target) in sorted_bookmarks {
            buf.extend_from_slice(name.as_bytes());
            append_ref_target(&mut buf, target);
        }

        let sorted_remotes: std::collections::BTreeMap<_, _> = view.remote_bookmarks.iter().collect();
        for (name, remote_ref) in sorted_remotes {
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&(remote_ref.is_tracked as u8).to_le_bytes());
            if let Some(target) = &remote_ref.target {
                append_ref_target(&mut buf, target);
            }
        }

        let view_id = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf).as_bytes()[..cc_common::VIEW_ID_LENGTH].to_vec();

        let mut views = self.views.lock().unwrap();
        let repo_views = views.entry(req.repo_id).or_default();
        repo_views.insert(view_id.clone(), view);

        Ok(tonic::Response::new(WriteViewResponse { view_id }))
    }

    async fn get_op_heads(
        &self,
        request: tonic::Request<GetOpHeadsRequest>,
    ) -> Result<tonic::Response<GetOpHeadsResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Get op heads for repo: {}", req.repo_id);

        let repos = self.repos.lock().unwrap();
        if !repos.contains(&req.repo_id) {
            return Err(tonic::Status::not_found(format!("repository should have been registered before requesting op heads: {}", req.repo_id)));
        }

        let op_heads = self.op_heads.lock().unwrap();
        let heads = op_heads.get(&req.repo_id).cloned().unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        Ok(tonic::Response::new(GetOpHeadsResponse {
            op_head_ids: heads,
        }))
    }

    async fn update_op_heads(
        &self,
        request: tonic::Request<UpdateOpHeadsRequest>,
    ) -> Result<tonic::Response<UpdateOpHeadsResponse>, tonic::Status> {
        let req = request.into_inner();
        info!("Update op heads for repo: {}", req.repo_id);

        let repos = self.repos.lock().unwrap();
        if !repos.contains(&req.repo_id) {
            return Err(tonic::Status::not_found(format!("repository should have been registered before updating op heads: {}", req.repo_id)));
        }

        let mut op_heads = self.op_heads.lock().unwrap();
        let current_heads = op_heads.entry(req.repo_id).or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        if req.old_op_head_ids.is_empty() || req.old_op_head_ids.iter().any(|old| current_heads.contains(old)) {
            *current_heads = vec![req.new_op_head_id];
        }

        Ok(tonic::Response::new(UpdateOpHeadsResponse {
            current_op_head_ids: current_heads.clone(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env().add_directive("jj_cc_server=info".parse()?))
        .init();

    let args = Args::parse();
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!("jj-cc-server listening on {}", local_addr);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let service = std::sync::Arc::new(CommitCloudServerImpl::default());

    Server::builder()
        .add_service(health_service)
        .add_service(BackendServiceServer::from_arc(service.clone()))
        .add_service(OpStoreServiceServer::from_arc(service))
        .serve_with_incoming_shutdown(incoming, async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Shutdown signal received, shutting down gracefully...");
                }
                Err(e) => {
                    warn!("Failed to listen for shutdown signal: {}, shutting down...", e);
                }
            }
        })
        .await?;

    Ok(())
}
