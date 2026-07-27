use tempfile::tempdir;

#[tokio::test]
async fn test_cloud_log_integration() {
    // 1. Spawn a test server
    let server = testutils::spawn_server().await;

    // 2. Initialize a repository with the Commit Cloud backend
    let temp_dir = tempdir().expect("temporary directory should have been created");
    let repo_path = temp_dir.path();

    let mut init_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    init_cmd
        .current_dir(repo_path)
        .args(["cc", "init", "--server", server.url(), "--create", "."]);
    init_cmd.assert().success();

    // 3. Run `jj log` on the newly initialized repository
    let mut log_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    log_cmd
        .current_dir(repo_path)
        .args(["log", "--no-graph"]);

    // Running `log` will invoke our Backend::read_commit stub once CliRunner is wired up!
    let output = log_cmd.output().expect("Failed to execute jj log command");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("--- jj log STDOUT ---\n{}", stdout);
    println!("--- jj log STDERR ---\n{}", stderr);

    // Assert that the command succeeds once read_commit is implemented
    assert!(
        output.status.success(),
        "jj log failed with stderr: {}",
        stderr
    );
}
