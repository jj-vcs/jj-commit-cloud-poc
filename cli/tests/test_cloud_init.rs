use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_cloud_init_integration() {
    // Spawn jj-cc-server using the shared dynamic port test harness
    let server = testutils::spawn_server().await;

    // Execute jj cc init inside a temporary test workspace
    let temp_dir = tempdir().expect("temporary directory should have been created for testing");
    let repo_path = temp_dir.path();

    let mut cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");

    cmd.current_dir(repo_path)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create",
            ".",
        ]);

    // Assert CLI execution is successful
    cmd.assert().success();

    // Verify local Jujutsu metadata directory structure
    let jj_store_path = repo_path.join(".jj/repo/store");
    assert!(jj_store_path.exists(), "The .jj/repo/store directory should exist");

    // Verify backend type selection file
    let store_type = fs::read_to_string(jj_store_path.join("type"))
        .expect("The store type file should be readable");
    assert_eq!(store_type, "commit_cloud");

    // Verify local Commit Cloud configuration TOML
    let config_content = fs::read_to_string(jj_store_path.join("config.toml"))
        .expect("The config.toml file should be readable");

    let parsed_config: toml::Value = toml::from_str(&config_content)
        .expect("The config.toml file should be valid TOML");

    // Verify correct parameters are serialized
    let server_url = parsed_config.get("server_url")
        .and_then(|v| v.as_str())
        .expect("The server_url field should exist and be a string");
    assert_eq!(server_url, server.url());

    let repo_id_str = parsed_config.get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");

    // Validate the repo_id string is a valid UUID
    uuid::Uuid::parse_str(repo_id_str)
        .expect("The repo_id should be a valid UUID string");
}

#[tokio::test]
async fn test_cloud_init_repo_registered() {
    let server = testutils::spawn_server().await;

    let temp_dir = tempdir().expect("temporary directory should have been created for testing");
    let repo_path = temp_dir.path();

    let mut cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");

    cmd.current_dir(repo_path)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create",
            ".",
        ]);

    cmd.assert().success();

    let jj_store_path = repo_path.join(".jj/repo/store");
    let config_content = fs::read_to_string(jj_store_path.join("config.toml"))
        .expect("The config.toml file should be readable");
    let parsed_config: toml::Value = toml::from_str(&config_content)
        .expect("The config.toml file should be valid TOML");
    let repo_id_str = parsed_config.get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");

    // Verify that the repository was actually registered in the cloud server over gRPC
    let mut client = cc_proto::backend::backend_service_client::BackendServiceClient::connect(server.url().to_string())
        .await
        .expect("Failed to connect to test server over gRPC");

    let request = tonic::Request::new(cc_proto::backend::ReadCommitRequest {
        repo_id: repo_id_str.to_string(),
        commit_id: vec![0u8; cc_common::COMMIT_ID_LENGTH], // The root commit ID
    });

    let response = client.read_commit(request).await;
    assert!(response.is_ok(), "Server failed to find our repo_id in its cloud database: {:?}", response.err());
}

#[tokio::test]
async fn test_cloud_init_failure_integration() {
    let mut cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");

    cmd.args([
        "cc",
        "init",
        "--server",
        "http://invalid-server-domain-does-not-exist:9999",
        "--create",
        "/invalid_path_dest_dir",
    ]);

    cmd.assert().failure();
}
