use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::time::SystemTime;
use async_trait::async_trait;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::*;
use jj_lib::backend::{CommitId, MillisSinceEpoch, Timestamp};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_store::{
    OpStore, OpStoreResult, Operation, OperationId, OperationMetadata, RefTarget,
    RemoteRef, RemoteRefState, RemoteView, TimestampRange, View, ViewId,
};
use jj_lib::ref_name::{RefNameBuf, RemoteNameBuf, WorkspaceNameBuf};

use crate::util::{run_async, CommitCloudConfig};

fn ref_target_to_proto(target: &RefTarget) -> cc_common::op_store::RefTarget {
    let removes = target.removed_ids().map(|id| cc_common::op_store::RefTargetTerm { commit_id: id.as_bytes().to_vec() }).collect();
    let adds = target.added_ids().map(|id| cc_common::op_store::RefTargetTerm { commit_id: id.as_bytes().to_vec() }).collect();
    cc_common::op_store::RefTarget { removes, adds }
}

fn ref_target_from_proto(target: &cc_common::op_store::RefTarget) -> RefTarget {
    let removed_ids = target.removes.iter().map(|t| CommitId::from_bytes(&t.commit_id));
    let added_ids = target.adds.iter().map(|t| CommitId::from_bytes(&t.commit_id));
    RefTarget::from_legacy_form(removed_ids, added_ids)
}

fn operation_to_proto(op: &Operation) -> cc_common::op_store::Operation {
    let metadata = cc_common::op_store::OperationMetadata {
        start_time_millis: op.metadata.time.start.timestamp.0,
        end_time_millis: op.metadata.time.end.timestamp.0,
        description: op.metadata.description.clone(),
        hostname: op.metadata.hostname.clone(),
        username: op.metadata.username.clone(),
        is_snapshot: op.metadata.is_snapshot,
        workspace_name: op.metadata.workspace_name.as_ref().map(|w| w.as_str().to_string()),
        attributes: op.metadata.attributes.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
    };
    let commit_predecessors = match &op.commit_predecessors {
        Some(map) => map.iter().map(|(k, v)| {
            cc_common::op_store::CommitPredecessors {
                commit_id: k.as_bytes().to_vec(),
                predecessor_ids: v.iter().map(|id| id.as_bytes().to_vec()).collect(),
            }
        }).collect(),
        None => vec![],
    };

    cc_common::op_store::Operation {
        view_id: op.view_id.as_bytes().to_vec(),
        parents: op.parents.iter().map(|p| p.as_bytes().to_vec()).collect(),
        metadata: Some(metadata),
        commit_predecessors,
        commit_predecessors_set: op.commit_predecessors.is_some(),
    }
}

fn operation_from_proto(op: &cc_common::op_store::Operation) -> Operation {
    let meta = op.metadata.as_ref().cloned().unwrap_or_default();
    let start_ts = Timestamp {
        timestamp: MillisSinceEpoch(meta.start_time_millis),
        tz_offset: 0,
    };
    let end_ts = Timestamp {
        timestamp: MillisSinceEpoch(meta.end_time_millis),
        tz_offset: 0,
    };
    let metadata = OperationMetadata {
        time: TimestampRange {
            start: start_ts,
            end: end_ts,
        },
        description: meta.description,
        hostname: meta.hostname,
        username: meta.username,
        is_snapshot: meta.is_snapshot,
        workspace_name: meta.workspace_name.map(WorkspaceNameBuf::from),
        attributes: meta.attributes.into_iter().collect(),
    };
    let commit_predecessors = if op.commit_predecessors_set {
        let mut map = BTreeMap::new();
        for p in &op.commit_predecessors {
            let key = CommitId::from_bytes(&p.commit_id);
            let val = p.predecessor_ids.iter().map(|id| CommitId::from_bytes(id)).collect();
            map.insert(key, val);
        }
        Some(map)
    } else {
        None
    };

    Operation {
        view_id: ViewId::from_bytes(&op.view_id),
        parents: op.parents.iter().map(|p| OperationId::from_bytes(p)).collect(),
        metadata,
        commit_predecessors,
    }
}

fn view_to_proto(view: &View) -> cc_common::op_store::View {
    let head_ids = view.head_ids.iter().map(|id| id.as_bytes().to_vec()).collect();
    let wc_commit_ids = view.wc_commit_ids.iter().map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec())).collect();
    let local_bookmarks = view.local_bookmarks.iter().map(|(k, v)| (k.as_str().to_string(), ref_target_to_proto(v))).collect();
    let mut remote_bookmarks = HashMap::new();
    for (remote_name, remote_view) in &view.remote_views {
        for (bookmark_name, remote_ref) in &remote_view.bookmarks {
            let key = format!("{}@{}", bookmark_name.as_str(), remote_name.as_str());
            remote_bookmarks.insert(
                key,
                cc_common::op_store::RemoteRef {
                    target: Some(ref_target_to_proto(&remote_ref.target)),
                    is_tracked: remote_ref.state == RemoteRefState::Tracked,
                },
            );
        }
    }

    cc_common::op_store::View {
        head_ids,
        wc_commit_ids,
        local_bookmarks,
        remote_bookmarks,
    }
}

fn view_from_proto(view: &cc_common::op_store::View) -> View {
    let head_ids = view.head_ids.iter().map(|id| CommitId::from_bytes(id)).collect::<HashSet<_>>();
    let wc_commit_ids = view.wc_commit_ids.iter().map(|(k, v)| (WorkspaceNameBuf::from(k.as_str()), CommitId::from_bytes(v))).collect();
    let local_bookmarks = view.local_bookmarks.iter().map(|(k, v)| (RefNameBuf::from(k.as_str()), ref_target_from_proto(v))).collect();
    let mut remote_views: BTreeMap<RemoteNameBuf, RemoteView> = BTreeMap::new();
    for (key, remote_ref_proto) in &view.remote_bookmarks {
        let remote_ref = RemoteRef {
            target: remote_ref_proto.target.as_ref().map(ref_target_from_proto).unwrap_or_else(RefTarget::absent),
            state: if remote_ref_proto.is_tracked { RemoteRefState::Tracked } else { RemoteRefState::New },
        };
        if let Some((b_str, r_str)) = key.rsplit_once('@') {
            let r_name = RemoteNameBuf::from(r_str);
            let b_name = RefNameBuf::from(b_str);
            remote_views.entry(r_name).or_default().bookmarks.insert(b_name, remote_ref);
        }
    }

    View {
        head_ids,
        wc_commit_ids,
        local_bookmarks,
        local_tags: BTreeMap::new(),
        remote_views,
        git_refs: BTreeMap::new(),
        git_head: RefTarget::absent(),
    }
}

#[derive(Debug)]
pub struct CommitCloudOpStore {
    server_url: String,
    repo_id: String,
    root_operation_id: OperationId,
}

impl CommitCloudOpStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn load(store_path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = CommitCloudConfig::load_from_store(store_path)?;
        Ok(Self {
            server_url: config.server_url,
            repo_id: config.repo_id,
            root_operation_id: OperationId::from_bytes(&cc_common::ROOT_OPERATION_ID_BYTES),
        })
    }
}

#[async_trait]
impl OpStore for CommitCloudOpStore {
    fn name(&self) -> &str {
        Self::name()
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let view_id_bytes = id.as_bytes().to_vec();

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .read_view(ReadViewRequest {
                    repo_id,
                    view_id: view_id_bytes,
                })
                .await?;
            let proto_view = response
                .into_inner()
                .view
                .ok_or_else(|| "server response should have contained a view".to_string())?;
            Ok(view_from_proto(&proto_view))
        })
        .map_err(|e| jj_lib::op_store::OpStoreError::Other(e.into()))
    }

    async fn write_view(&self, contents: &View) -> OpStoreResult<ViewId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_view = view_to_proto(contents);

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .write_view(WriteViewRequest {
                    repo_id,
                    view: Some(proto_view),
                })
                .await?;
            let view_id_bytes = response.into_inner().view_id;
            Ok(ViewId::from_bytes(&view_id_bytes))
        })
        .map_err(|e| jj_lib::op_store::OpStoreError::Other(e.into()))
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let op_id_bytes = id.as_bytes().to_vec();

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .read_operation(ReadOperationRequest {
                    repo_id,
                    operation_id: op_id_bytes,
                })
                .await?;
            let proto_op = response
                .into_inner()
                .operation
                .ok_or_else(|| "server response should have contained an operation".to_string())?;
            Ok(operation_from_proto(&proto_op))
        })
        .map_err(|e| jj_lib::op_store::OpStoreError::Other(e.into()))
    }

    async fn write_operation(&self, contents: &Operation) -> OpStoreResult<OperationId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let proto_op = operation_to_proto(contents);

        run_async(move || async move {
            let mut client = OpStoreServiceClient::connect(server_url).await?;
            let response = client
                .write_operation(WriteOperationRequest {
                    repo_id,
                    operation: Some(proto_op),
                })
                .await?;
            let op_id_bytes = response.into_inner().operation_id;
            Ok(OperationId::from_bytes(&op_id_bytes))
        })
        .map_err(|e| jj_lib::op_store::OpStoreError::Other(e.into()))
    }

    // TODO: Implement server RPC to resolve short operation ID prefixes for commands like `jj op show <prefix>`
    // (returning SingleMatch, AmbiguousMatch, or NoMatch).
    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        if prefix.matches(&self.root_operation_id) {
            Ok(PrefixResolution::SingleMatch(self.root_operation_id.clone()))
        } else {
            Ok(PrefixResolution::NoMatch)
        }
    }

    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}
