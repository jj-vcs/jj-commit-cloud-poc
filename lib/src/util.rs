use cc_proto::backend::backend_service_client::BackendServiceClient;
use cc_proto::op_heads_store::op_heads_store_service_client::OpHeadsStoreServiceClient;
use cc_proto::op_store::op_store_service_client::OpStoreServiceClient;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub async fn connect_channel(
    server_url: impl Into<String>,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    let url_str = server_url.into();
    let mut endpoint = Endpoint::from_shared(url_str.clone())?;
    if url_str.starts_with("https://") {
        let tls = ClientTlsConfig::new().with_webpki_roots();
        endpoint = endpoint.tls_config(tls)?;
    }
    Ok(endpoint.connect().await?)
}

pub async fn connect_backend_client(
    server_url: impl Into<String>,
) -> Result<BackendServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(server_url).await?;
    Ok(BackendServiceClient::new(channel))
}

pub async fn connect_op_store_client(
    server_url: impl Into<String>,
) -> Result<OpStoreServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(server_url).await?;
    Ok(OpStoreServiceClient::new(channel))
}

pub async fn connect_op_heads_client(
    server_url: impl Into<String>,
) -> Result<OpHeadsStoreServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(server_url).await?;
    Ok(OpHeadsStoreServiceClient::new(channel))
}
