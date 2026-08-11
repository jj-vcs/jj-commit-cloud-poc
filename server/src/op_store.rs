use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use cc_common::op_store::op_store_service_server::OpStoreService;
use cc_common::op_store::*;

use crate::store::MemoryStore;

#[derive(Debug, Clone)]
pub struct CommitCloudOpStoreService {
    store: Arc<MemoryStore>,
}

impl CommitCloudOpStoreService {
    pub fn new(store: Arc<MemoryStore>) -> Self {
        Self { store }
    }

    fn ensure_repo_registered_error(
        &self,
        repo_id: &str,
        action: &str,
    ) -> Result<(), tonic::Status> {
        if !self.store.repos.lock().unwrap().contains(repo_id) {
            return Err(tonic::Status::not_found(format!(
                "repository should have been registered before {action}: {repo_id}"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl OpStoreService for CommitCloudOpStoreService {
    async fn read_operation(
        &self,
        request: tonic::Request<ReadOperationRequest>,
    ) -> Result<tonic::Response<ReadOperationResponse>, tonic::Status> {
        let req = request.into_inner();
        info!(
            "Reading operation: {} for repo: {}",
            hex::encode(&req.operation_id),
            req.repo_id
        );

        self.ensure_repo_registered_error(&req.repo_id, "reading operations")?;

        let ops = self.store.ops.lock().unwrap();
        if let Some(repo_ops) = ops.get(&req.repo_id) {
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
        let op = req.operation.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained an operation object")
        })?;

        info!("Writing operation for repo: {}", req.repo_id);

        self.ensure_repo_registered_error(&req.repo_id, "writing operations")?;

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
        let op_id = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf)
            .as_bytes()[..cc_common::OPERATION_ID_LENGTH]
            .to_vec();

        let mut ops = self.store.ops.lock().unwrap();
        let repo_ops = ops.entry(req.repo_id).or_default();
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
        info!(
            "Reading view: {} for repo: {}",
            hex::encode(&req.view_id),
            req.repo_id
        );

        self.ensure_repo_registered_error(&req.repo_id, "reading views")?;

        let views = self.store.views.lock().unwrap();
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
        let view = req.view.ok_or_else(|| {
            tonic::Status::invalid_argument("request should have contained a view object")
        })?;

        info!("Writing view for repo: {}", req.repo_id);

        self.ensure_repo_registered_error(&req.repo_id, "writing views")?;

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

        let sorted_bookmarks: std::collections::BTreeMap<_, _> =
            view.local_bookmarks.iter().collect();
        for (name, target) in sorted_bookmarks {
            buf.extend_from_slice(name.as_bytes());
            append_ref_target(&mut buf, target);
        }

        let sorted_remotes: std::collections::BTreeMap<_, _> =
            view.remote_bookmarks.iter().collect();
        for (name, remote_ref) in sorted_remotes {
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&(remote_ref.is_tracked as u8).to_le_bytes());
            if let Some(target) = &remote_ref.target {
                append_ref_target(&mut buf, target);
            }
        }

        let view_id = gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, &buf)
            .as_bytes()[..cc_common::VIEW_ID_LENGTH]
            .to_vec();

        let mut views = self.store.views.lock().unwrap();
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

        self.ensure_repo_registered_error(&req.repo_id, "requesting op heads")?;

        let op_heads = self.store.op_heads.lock().unwrap();
        let heads = op_heads
            .get(&req.repo_id)
            .cloned()
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

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

        self.ensure_repo_registered_error(&req.repo_id, "updating op heads")?;

        let mut op_heads = self.store.op_heads.lock().unwrap();
        let current_heads = op_heads
            .entry(req.repo_id)
            .or_insert_with(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        if req.old_op_head_ids.is_empty()
            || req
                .old_op_head_ids
                .iter()
                .any(|old| current_heads.contains(old))
        {
            *current_heads = vec![req.new_op_head_id];
        }

        Ok(tonic::Response::new(UpdateOpHeadsResponse {
            current_op_head_ids: current_heads.clone(),
        }))
    }
}
