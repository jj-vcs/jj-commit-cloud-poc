use async_trait::async_trait;
use cc_proto::op_store as proto_op;
use cc_proto::op_store::op_store_service_client::OpStoreServiceClient;
use jj_lib::backend::{CommitId, MillisSinceEpoch, Timestamp};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::op_store::*;
use jj_lib::ref_name::WorkspaceNameBuf;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::time::SystemTime;

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
pub struct CommitCloudOpStore {
    repo_id: String,
    server_url: String,
    root_operation_id: OperationId,
}

impl CommitCloudOpStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn new(repo_id: String, server_url: String) -> Self {
        let root_operation_id = OperationId::from_bytes(&[0u8; 20]);
        Self {
            repo_id,
            server_url,
            root_operation_id,
        }
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
        let view_id = id.as_bytes().to_vec();

        let res: OpStoreResult<View> = run_async(async move {
            let mut client = OpStoreServiceClient::connect(server_url)
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?;

            let resp = client
                .read_view(make_request(proto_op::ReadViewRequest { repo_id, view_id }))
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?;


            let proto_view = resp.into_inner().view.ok_or_else(|| {
                OpStoreError::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "View not found",
                )))
            })?;

            let head_ids: HashSet<CommitId> = proto_view
                .head_commit_ids
                .iter()
                .map(|b| CommitId::from_bytes(b))
                .collect();

            let mut wc_commit_ids = BTreeMap::new();
            for (wc_name, commit_bytes) in proto_view.wc_commit_ids {
                let name_buf = WorkspaceNameBuf::from(wc_name);
                wc_commit_ids.insert(name_buf, CommitId::from_bytes(&commit_bytes));
            }

            Ok(View {
                head_ids,
                local_bookmarks: BTreeMap::new(),
                local_tags: BTreeMap::new(),
                remote_views: BTreeMap::new(),
                git_refs: BTreeMap::new(),
                git_head: RefTarget::absent(),
                wc_commit_ids,
            })
        });

        match res {
            Ok(view) => Ok(view),
            Err(_) => {
                let mut head_ids = HashSet::new();
                head_ids.insert(CommitId::from_bytes(&[0u8; 20]));
                Ok(View {
                    head_ids,
                    local_bookmarks: BTreeMap::new(),
                    local_tags: BTreeMap::new(),
                    remote_views: BTreeMap::new(),
                    git_refs: BTreeMap::new(),
                    git_head: RefTarget::absent(),
                    wc_commit_ids: BTreeMap::new(),
                })
            }
        }
    }

    async fn write_view(&self, contents: &View) -> OpStoreResult<ViewId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let head_commit_ids = contents.head_ids.iter().map(|c| c.as_bytes().to_vec()).collect();
        let mut wc_commit_ids = HashMap::new();
        for (wc_id, commit_id) in &contents.wc_commit_ids {
            wc_commit_ids.insert(wc_id.as_str().to_string(), commit_id.as_bytes().to_vec());
        }

        let proto_view = proto_op::View {
            view_id: vec![],
            head_commit_ids,
            public_head_commit_ids: vec![],
            wc_commit_ids,
            bookmarks: HashMap::new(),
            tags: HashMap::new(),
            git_refs: HashMap::new(),
        };

        run_async(async move {
            let mut client = OpStoreServiceClient::connect(server_url)
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?;

            let resp = client
                .write_view(make_request(proto_op::WriteViewRequest {
                    repo_id,
                    view: Some(proto_view),
                }))
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?;


            let view_id = ViewId::from_bytes(&resp.into_inner().view_id);
            Ok(view_id)
        })
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if *id == self.root_operation_id {
            return Ok(Operation {
                view_id: ViewId::from_bytes(&[0u8; 20]),
                parents: vec![],
                metadata: OperationMetadata {
                    time: TimestampRange {
                        start: Timestamp {
                            timestamp: MillisSinceEpoch(0),
                            tz_offset: 0,
                        },
                        end: Timestamp {
                            timestamp: MillisSinceEpoch(0),
                            tz_offset: 0,
                        },
                    },
                    description: String::new(),
                    hostname: String::new(),
                    username: String::new(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: BTreeMap::new(),
                },
                commit_predecessors: None,
            });
        }

        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let operation_id = id.as_bytes().to_vec();

        run_async(async move {
            let mut client = OpStoreServiceClient::connect(server_url)
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?;

            let resp = client
                .read_operation(make_request(proto_op::ReadOperationRequest {
                    repo_id,
                    operation_id,
                }))
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?;


            let proto_op = resp.into_inner().operation.ok_or_else(|| {
                OpStoreError::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Operation not found",
                )))
            })?;

            let view_id = ViewId::from_bytes(&proto_op.view_id);
            let parents = proto_op
                .parent_op_ids
                .iter()
                .map(|b| OperationId::from_bytes(b))
                .collect();

            let metadata = if let Some(m) = proto_op.metadata {
                OperationMetadata {
                    time: TimestampRange {
                        start: Timestamp {
                            timestamp: MillisSinceEpoch(m.start_time_millis),
                            tz_offset: 0,
                        },
                        end: Timestamp {
                            timestamp: MillisSinceEpoch(m.end_time_millis),
                            tz_offset: 0,
                        },
                    },
                    description: m.description,
                    hostname: m.hostname,
                    username: m.username,
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: m.tags.into_iter().collect(),
                }
            } else {
                OperationMetadata {
                    time: TimestampRange {
                        start: Timestamp {
                            timestamp: MillisSinceEpoch(0),
                            tz_offset: 0,
                        },
                        end: Timestamp {
                            timestamp: MillisSinceEpoch(0),
                            tz_offset: 0,
                        },
                    },
                    description: String::new(),
                    hostname: String::new(),
                    username: String::new(),
                    is_snapshot: false,
                    workspace_name: None,
                    attributes: BTreeMap::new(),
                }
            };

            Ok(Operation {
                view_id,
                parents,
                metadata,
                commit_predecessors: None,
            })
        })
    }

    async fn write_operation(&self, contents: &Operation) -> OpStoreResult<OperationId> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        let parent_op_ids = contents.parents.iter().map(|p| p.as_bytes().to_vec()).collect();
        let metadata = Some(proto_op::OperationMetadata {
            start_time_millis: contents.metadata.time.start.timestamp.0,
            end_time_millis: contents.metadata.time.end.timestamp.0,
            hostname: contents.metadata.hostname.clone(),
            username: contents.metadata.username.clone(),
            description: contents.metadata.description.clone(),
            tags: contents.metadata.attributes.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        });

        let proto_op = proto_op::Operation {
            operation_id: vec![],
            parent_op_ids,
            view_id: contents.view_id.as_bytes().to_vec(),
            metadata,
        };

        run_async(async move {
            let mut client = OpStoreServiceClient::connect(server_url)
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?;

            let resp = client
                .write_operation(make_request(proto_op::WriteOperationRequest {
                    repo_id,
                    operation: Some(proto_op),
                }))
                .await
                .map_err(|e| OpStoreError::Other(e.into()))?;


            let op_id = OperationId::from_bytes(&resp.into_inner().operation_id);
            Ok(op_id)
        })
    }

    async fn resolve_operation_id_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        Ok(PrefixResolution::NoMatch)
    }

    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}
