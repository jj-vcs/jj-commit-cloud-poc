use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_describe_command_commit_cloud() {
    // 1. Spawn dynamic port in-memory Commit Cloud server
    let server = testutils::spawn_server().await;

    // 2. Setup temporary workspace
    let temp_dir = tempdir().expect("tempdir should be created");
    let repo_path = temp_dir.path();

    // 3. Initialize workspace with sjj cc init --server <url> .
    let mut init_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    init_cmd
        .current_dir(repo_path)
        .args(["cc", "init", "--server", server.url(), "."]);
    init_cmd.assert().success();

    // 4. Set description using -m flag
    let mut desc_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    desc_cmd
        .current_dir(repo_path)
        .args(["describe", "-m", "description from Commit Cloud CLI"]);
    
    let output = desc_cmd.output().expect("describe command should execute");
    assert!(output.status.success(), "sjj describe should succeed");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("description from Commit Cloud CLI") || stderr.contains("Working copy"),
        "stderr should mention description update");

    // 5. Verify local store config exists
    let config_path = repo_path.join(".jj/repo/store/config.toml");
    assert!(config_path.exists());
    let config_str = fs::read_to_string(config_path).unwrap();
    assert!(config_str.contains("server_url ="));
}
