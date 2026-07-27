use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_commit_cloud_full_workflow() {
    // 1. Spawn a fresh in-memory jj-cc-server instance on an OS-allocated dynamic port
    let server = testutils::spawn_server().await;

    // 2. Create temporary workspace directory
    let temp_dir = tempdir().expect("temporary directory should have been created");
    let repo_path = temp_dir.path();

    // 3. Run: sjj cc init --server <server_url>
    let mut init_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    init_cmd
        .current_dir(repo_path)
        .args(["cc", "init", "--server", server.url(), "."]);
    init_cmd.assert().success();

    // 4. Create a test file in the working directory
    let test_file_path = repo_path.join("hello.txt");
    fs::write(&test_file_path, "Hello Commit Cloud!\n").unwrap();

    // 5. Run sjj status to trigger working copy snapshot & tree/file writes to server
    let mut status_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    status_cmd.current_dir(repo_path).arg("status");
    status_cmd.assert().success();

    // 6. Run sjj new to commit working copy changes and create a new commit node
    let mut new_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    new_cmd.current_dir(repo_path).args(["new", "-m", "add hello.txt"]);
    new_cmd.assert().success();

    // 7. Verify local store configuration
    let config_path = repo_path.join(".jj/repo/store/config.toml");
    assert!(config_path.exists(), "The Commit Cloud config.toml should exist");


    let config_str = fs::read_to_string(&config_path).unwrap();
    assert!(config_str.contains("server_url ="), "config.toml must contain server_url");
    assert!(config_str.contains("repo_id ="), "config.toml must contain repo_id");
}
