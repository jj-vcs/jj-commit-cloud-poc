use tempfile::tempdir;

#[tokio::test]
async fn test_log_command_integration() {
    // Spawn jj-cc-server using the shared dynamic port test harness
    let server = testutils::spawn_server().await;

    // Execute jj cc init inside a temporary test workspace
    let temp_dir = tempdir().expect("temporary directory should have been created for testing");
    let repo_path = temp_dir.path();

    let mut init_cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");

    init_cmd
        .current_dir(repo_path)
        .args([
            "cc",
            "init",
            "--server",
            server.url(),
            "--create",
            ".",
        ]);

    init_cmd.assert().success();

    // Now run jj log for root() using -T commit_id
    let mut log_cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");

    log_cmd
        .current_dir(repo_path)
        .args(["log", "-r", "root()", "-T", "commit_id"]);

    // Special character is needed to assert accurate log output. see cli/tests/test_log_command.rs upstream
    log_cmd
        .assert()
        .success()
        .stdout("◆  0000000000000000000000000000000000000000\n");
}
