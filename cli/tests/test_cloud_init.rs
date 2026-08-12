use std::fs;

#[tokio::test]
async fn test_cloud_init_integration() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

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
    assert_eq!(server_url, workspace.server_url());

    let repo_id_str = parsed_config.get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");

    // Validate the repo_id string is a valid UUID
    uuid::Uuid::parse_str(repo_id_str)
        .expect("The repo_id should be a valid UUID string");
}

#[tokio::test]
async fn test_cloud_init_repo_registered() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    let jj_store_path = repo_path.join(".jj/repo/store");
    let config_content = fs::read_to_string(jj_store_path.join("config.toml"))
        .expect("The config.toml file should be readable");
    let parsed_config: toml::Value = toml::from_str(&config_content)
        .expect("The config.toml file should be valid TOML");
    let repo_id_str = parsed_config.get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");

    // Verify that the repository was actually registered in the cloud server over gRPC
    let mut client = cc_common::backend::backend_service_client::BackendServiceClient::connect(workspace.server_url().to_string())
        .await
        .expect("gRPC connection to test server should have succeeded");

    let request = tonic::Request::new(cc_common::backend::ReadCommitRequest {
        repo_id: repo_id_str.to_string(),
        commit_id: vec![1u8; cc_common::COMMIT_ID_LENGTH], // Dummy commit ID
    });

    let err = client.read_commit(request).await.unwrap_err();
    assert_eq!(
        err.message(),
        "commit should have been present in cloud database",
        "Repository was not registered in the cloud server!"
    );
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
