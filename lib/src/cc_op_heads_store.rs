use async_trait::async_trait;
use cc_proto::op_heads_store as proto_heads;
use cc_proto::op_heads_store::op_heads_store_service_client::OpHeadsStoreServiceClient;
use jj_lib::object_id::ObjectId;
use jj_lib::op_heads_store::*;
use jj_lib::op_store::OperationId;
use std::fmt::Debug;

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

#[derive(Debug)]
struct NoopLock;
impl OpHeadsStoreLock for NoopLock {}

#[derive(Debug, Clone)]
pub struct CommitCloudOpHeadsStore {
    repo_id: String,
    server_url: String,
}

impl CommitCloudOpHeadsStore {
    pub fn name() -> &'static str {
        "commit_cloud"
    }

    pub fn new(repo_id: String, server_url: String) -> Self {
        Self { repo_id, server_url }
    }
}

#[async_trait]
impl OpHeadsStore for CommitCloudOpHeadsStore {
    fn name(&self) -> &str {
        Self::name()
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();
        let new_op_head_id = new_id.as_bytes().to_vec();
        let old_op_head_ids: Vec<Vec<u8>> = old_ids.iter().map(|i| i.as_bytes().to_vec()).collect();

        run_async(async move {
            let mut client = OpHeadsStoreServiceClient::connect(server_url)
                .await
                .map_err(|e| OpHeadsStoreError::Read(e.into()))?;

            for old_id in old_op_head_ids {
                let _ = client
                    .remove_op_head(make_request(proto_heads::RemoveOpHeadRequest {
                        repo_id: repo_id.clone(),
                        op_head_id: old_id,
                    }))
                    .await;
            }

            client
                .add_op_head(make_request(proto_heads::AddOpHeadRequest {
                    repo_id,
                    op_head_id: new_op_head_id,
                }))
                .await
                .map_err(|e| OpHeadsStoreError::Read(e.into()))?;

            Ok(())
        })
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let server_url = self.server_url.clone();
        let repo_id = self.repo_id.clone();

        run_async(async move {
            let mut client = OpHeadsStoreServiceClient::connect(server_url)
                .await
                .map_err(|e| OpHeadsStoreError::Read(e.into()))?;

            let resp = client
                .get_op_heads(make_request(proto_heads::GetOpHeadsRequest { repo_id }))
                .await
                .map_err(|e| OpHeadsStoreError::Read(e.into()))?;


            let head_ids = resp
                .into_inner()
                .op_head_ids
                .iter()
                .map(|b| OperationId::from_bytes(b.as_slice()))
                .collect();

            Ok(head_ids)
        })
    }

    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(NoopLock))
    }
}
