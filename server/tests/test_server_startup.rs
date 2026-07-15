use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

#[tokio::test]
async fn test_server_startup_and_grpc_health_check() {
    // 1. Spawn jj-cc-server using the shared dynamic port test harness
    let server = testutils::spawn_server().await;

    // 2. Connect to the dynamic server URL
    let channel = tonic::transport::Endpoint::from_shared(server.url().to_string())
        .expect("The gRPC endpoint URI should be valid")
        .connect()
        .await
        .expect("The gRPC connection to the health service should have succeeded");

    let mut client = HealthClient::new(channel);

    let response = client
        .check(HealthCheckRequest {
            service: "".to_string(),
        })
        .await
        .expect("The gRPC health check request should have succeeded");

    assert_eq!(
        response.into_inner().status(),
        tonic_health::pb::health_check_response::ServingStatus::Serving,
        "Server is not in SERVING state"
    );
}
