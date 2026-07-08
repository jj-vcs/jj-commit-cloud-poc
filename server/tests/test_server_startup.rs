use assert_cmd::cargo::cargo_bin;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

struct ServerGuard(tokio::process::Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

fn extract_listening_address(line: &str) -> Option<String> {
    let marker = "listening on ";
    if let Some(idx) = line.find(marker) {
        let addr_part = &line[idx + marker.len()..];
        let addr = addr_part.trim().to_string();
        if !addr.is_empty() {
            return Some(addr);
        }
    }
    None
}

#[tokio::test]
async fn test_server_startup_and_grpc_health_check() {
    // Start the server with --port=0. Setting the port to 0 in tonic tells the OS 
    // to dynamically allocate any available ephemeral port, so we do not have to hardcode a port
    // and potentially face collisions.
    let mut child = Command::new(cargo_bin("jj-cc-server"))
        .arg("--port=0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("The jj-cc-server process should have spawned");

    let stdout = child.stdout.take().expect("The stdout pipe should have been captured from the child process");
    let _guard = ServerGuard(child);

    let mut reader = BufReader::new(stdout).lines();
    // Timeout of 2 seconds to find the listening address in stdout. 
    // 2 seconds is arbitrary but serves as a safe defensive timeout to prevent 
    // the test from hanging indefinitely if the server hangs.
    let timeout = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(timeout);

    let server_addr = loop {
        tokio::select! {
            line_result = reader.next_line() => {
                match line_result {
                    Ok(Some(line)) => {
                        if let Some(addr) = extract_listening_address(&line) {
                            break addr;
                        }
                    }
                    Ok(None) => panic!("The stdout pipe should not have closed before the server listening address was printed"),
                    Err(e) => panic!("A line from the server stdout should have been read successfully: {:?}", e),
                }
            }
            _ = &mut timeout => {
                panic!("The server listening address should have appeared in stdout before timeout");
            }
        }
    };

    let uri = format!("http://{}", server_addr);

    let channel = tonic::transport::Endpoint::from_shared(uri)
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
