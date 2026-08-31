use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};
use tracing::{error, info};

#[derive(Clone)]
pub struct RemoteConnectionManager {
    server_url: String,
    channel: Arc<Mutex<Option<Channel>>>,
}

impl RemoteConnectionManager {
    pub fn new(server_url: String) -> Self {
        Self {
            server_url,
            channel: Arc::new(Mutex::new(None)),
        }
    }

    #[allow(dead_code)]
    pub fn server_url(&self) -> &str {
        &self.server_url
    }

    /// Lazily connects to the remote Commit Cloud server on demand.
    /// If an existing channel is cached, it is reused.
    pub async fn get_channel(&self) -> Result<Channel, Status> {
        let mut guard = self.channel.lock().await;
        if let Some(channel) = &*guard {
            return Ok(channel.clone());
        }

        info!("Opening lazy gRPC connection to Commit Cloud server at '{}'", self.server_url);
        let endpoint = match Endpoint::from_shared(self.server_url.clone()) {
            Ok(ep) => ep
                .connect_timeout(Duration::from_secs(10))
                .keep_alive_timeout(Duration::from_secs(10))
                .keep_alive_while_idle(true),
            Err(e) => {
                return Err(Status::internal(format!(
                    "Invalid Commit Cloud server URL '{}': {e}",
                    self.server_url
                )));
            }
        };

        let channel = match endpoint.connect().await {
            Ok(chan) => chan,
            Err(e) => {
                error!(
                    "Failed to connect to Commit Cloud server at '{}': {e}",
                    self.server_url
                );
                return Err(Status::unavailable(format!(
                    "Failed to connect to Commit Cloud server: {e}"
                )));
            }
        };

        *guard = Some(channel.clone());
        Ok(channel)
    }

    pub async fn get_backend_client(&self) -> Result<BackendServiceClient<Channel>, Status> {
        match self.get_channel().await {
            Ok(channel) => Ok(BackendServiceClient::new(channel)),
            Err(e) => Err(e),
        }
    }

    pub async fn get_op_store_client(&self) -> Result<OpStoreServiceClient<Channel>, Status> {
        match self.get_channel().await {
            Ok(channel) => Ok(OpStoreServiceClient::new(channel)),
            Err(e) => Err(e),
        }
    }

    pub async fn get_workspace_client(&self) -> Result<WorkspaceServiceClient<Channel>, Status> {
        match self.get_channel().await {
            Ok(channel) => Ok(WorkspaceServiceClient::new(channel)),
            Err(e) => Err(e),
        }
    }

    /// Resets the connection cache when a transport / connection error occurs.
    pub async fn reset(&self) {
        info!("Resetting remote connection channel cache");
        let mut guard = self.channel.lock().await;
        *guard = None;
    }

    pub fn is_connection_error(status: &Status) -> bool {
        matches!(
            status.code(),
            Code::Unavailable | Code::Unknown | Code::ResourceExhausted | Code::Aborted
        )
    }

    /// Reusable execution wrapper: intercepts connection errors and resets channel cache.
    pub async fn execute<F, Fut, T>(&self, f: F) -> Result<T, Status>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        match f().await {
            Ok(resp) => Ok(resp),
            Err(status) => {
                if Self::is_connection_error(&status) {
                    self.reset().await;
                }
                Err(status)
            }
        }
    }

    pub async fn with_backend<F, Fut, T>(&self, f: F) -> Result<T, Status>
    where
        F: FnOnce(BackendServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        match self.get_backend_client().await {
            Ok(client) => self.execute(|| f(client)).await,
            Err(status) => {
                if Self::is_connection_error(&status) {
                    self.reset().await;
                }
                Err(status)
            }
        }
    }

    pub async fn with_op_store<F, Fut, T>(&self, f: F) -> Result<T, Status>
    where
        F: FnOnce(OpStoreServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        match self.get_op_store_client().await {
            Ok(client) => self.execute(|| f(client)).await,
            Err(status) => {
                if Self::is_connection_error(&status) {
                    self.reset().await;
                }
                Err(status)
            }
        }
    }

    pub async fn with_workspace<F, Fut, T>(&self, f: F) -> Result<T, Status>
    where
        F: FnOnce(WorkspaceServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        match self.get_workspace_client().await {
            Ok(client) => self.execute(|| f(client)).await,
            Err(status) => {
                if Self::is_connection_error(&status) {
                    self.reset().await;
                }
                Err(status)
            }
        }
    }
}
