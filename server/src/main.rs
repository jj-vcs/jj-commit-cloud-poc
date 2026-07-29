use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Mutex;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use cc_common::backend::backend_service_server::{BackendService, BackendServiceServer};
use cc_common::backend::*;

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
        _request: tonic::Request<ReadTreeRequest>,
    ) -> Result<tonic::Response<ReadTreeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn write_tree(
        &self,
        _request: tonic::Request<WriteTreeRequest>,
    ) -> Result<tonic::Response<WriteTreeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    type ReadFileStream = tokio_stream::wrappers::ReceiverStream<Result<ReadFileResponse, tonic::Status>>;

    async fn read_file(
        &self,
        _request: tonic::Request<ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }

    async fn write_file(
        &self,
        _request: tonic::Request<WriteFileRequest>,
    ) -> Result<tonic::Response<WriteFileResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
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
    let service = CommitCloudServerImpl::default();

    Server::builder()
        .add_service(health_service)
        .add_service(BackendServiceServer::new(service))
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
