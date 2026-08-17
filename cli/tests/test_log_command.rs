#[tokio::test]
async fn test_log_root_commit_exists() {
    let workspace = testutils::TestWorkspace::init().await;

    // Special character is needed to assert accurate log output. see cli/tests/test_log_command.rs upstream
    workspace
        .jj_cmd()
        .args(["log", "-r", "root()", "-T", "commit_id"])
        .assert()
        .success()
        .stdout("◆  0000000000000000000000000000000000000000\n");
}
