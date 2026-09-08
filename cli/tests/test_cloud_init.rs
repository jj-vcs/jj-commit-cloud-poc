use std::fs;

#[tokio::test]
async fn test_cc_init_creates_local_workspace() {
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
async fn test_cc_init_registers_repository() {
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
async fn test_cc_init_fails_on_invalid_server_addr() {
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

#[tokio::test]
async fn test_cc_init_with_create_repo_flag() {
    let server = testutils::spawn_server().await;
    let temp_dir = tempfile::tempdir().expect("temporary directory should have been created for testing");
    let repo_path = temp_dir.path();

    let mut init_cmd = assert_cmd::Command::cargo_bin("jj").expect("The jj CLI binary should have compiled");
    init_cmd
        .current_dir(repo_path)
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create-repo",
            ".",
        ]);

    init_cmd.assert().success();

    let jj_store_path = repo_path.join(".jj/repo/store");
    let config_content = fs::read_to_string(jj_store_path.join("config.toml"))
        .expect("The config.toml file should be readable");
    let parsed_config: toml::Value = toml::from_str(&config_content)
        .expect("The config.toml file should be valid TOML");
    let repo_id_str = parsed_config.get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");
    uuid::Uuid::parse_str(repo_id_str).expect("The repo_id should be a valid UUID string");
}

#[tokio::test]
async fn test_cc_init_with_repo_id_flag() {
    let workspace1 = testutils::TestWorkspace::init().await;
    let repo1_path = workspace1.repo_path();

    // Read the repo_id created by workspace1
    let jj_store_path1 = repo1_path.join(".jj/repo/store");
    let config_content1 = fs::read_to_string(jj_store_path1.join("config.toml"))
        .expect("The config.toml file should be readable");
    let parsed_config1: toml::Value = toml::from_str(&config_content1)
        .expect("The config.toml file should be valid TOML");
    let repo_id_str = parsed_config1.get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");

    // Initialize a second workspace pointing to the same existing repo_id
    let temp_dir2 = tempfile::tempdir().expect("temporary directory should have been created for testing");
    let repo2_path = temp_dir2.path();

    let mut init_cmd = assert_cmd::Command::cargo_bin("jj").expect("The jj CLI binary should have compiled");
    init_cmd
        .current_dir(repo2_path)
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            workspace1.server_url(),
            "--repo-id",
            repo_id_str,
            ".",
        ]);

    init_cmd.assert().success();

    let jj_store_path2 = repo2_path.join(".jj/repo/store");
    let config_content2 = fs::read_to_string(jj_store_path2.join("config.toml"))
        .expect("The config.toml file should be readable");
    let parsed_config2: toml::Value = toml::from_str(&config_content2)
        .expect("The config.toml file should be valid TOML");
    let repo_id_str2 = parsed_config2.get("repo_id")
        .and_then(|v| v.as_str())
        .expect("The repo_id field should exist and be a string");
    assert_eq!(repo_id_str2, repo_id_str);
}

#[tokio::test]
async fn test_cc_init_conflicts_create_repo_and_repo_id() {
    let server = testutils::spawn_server().await;
    let temp_dir = tempfile::tempdir().expect("temporary directory should have been created for testing");
    let repo_path = temp_dir.path();

    let mut init_cmd = assert_cmd::Command::cargo_bin("jj").expect("The jj CLI binary should have compiled");
    init_cmd
        .current_dir(repo_path)
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create-repo",
            "--repo-id",
            "custom-repo-id",
            ".",
        ]);

    init_cmd.assert().failure();
}

#[tokio::test]
async fn test_cc_init_conflicts_create_and_repo_id_aliases() {
    let server = testutils::spawn_server().await;
    let temp_dir = tempfile::tempdir().expect("temporary directory should have been created for testing");
    let repo_path = temp_dir.path();

    let mut init_cmd = assert_cmd::Command::cargo_bin("jj").expect("The jj CLI binary should have compiled");
    init_cmd
        .current_dir(repo_path)
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create",
            "--repo_id",
            "custom-repo-id",
            ".",
        ]);

    init_cmd.assert().failure();
}
