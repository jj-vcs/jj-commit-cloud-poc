use std::fs;
use testutils::TestWorkspace;

#[tokio::test]
async fn test_daemon_command_toggles_config() {
    let ws = TestWorkspace::init().await;

    // Check initial status
    let mut cmd = ws.jj_cmd();
    let assert = cmd.args(["cc", "daemon", "--status"]).assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("Enabled: true"),
        "Daemon should be enabled by default"
    );

    // Disable daemon
    let mut cmd = ws.jj_cmd();
    cmd.args(["cc", "daemon", "--disable"]).assert().success();

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let content = fs::read_to_string(&config_path).unwrap();
    assert!(
        content.contains("use_daemon = false"),
        "config.toml should have use_daemon = false"
    );

    // Check status again
    let mut cmd = ws.jj_cmd();
    let assert = cmd.args(["cc", "daemon", "--status"]).assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("Enabled: false"),
        "Daemon should now be disabled"
    );

    // Re-enable daemon
    let mut cmd = ws.jj_cmd();
    cmd.args(["cc", "daemon", "--enable"]).assert().success();

    let content = fs::read_to_string(&config_path).unwrap();
    assert!(
        content.contains("use_daemon = true"),
        "config.toml should have use_daemon = true"
    );
}

#[tokio::test]
#[should_panic]
async fn test_daemon_service_unimplemented_fails() {
    // This test sets use_daemon = true and verifies that attempting to route through
    // the unimplemented daemon layer fails.
    let ws = TestWorkspace::init().await;

    let mut cmd = ws.jj_cmd();
    cmd.args(["cc", "daemon", "--enable"]).assert().success();

    // In this initial scaffolding commit, the daemon service is stubbed as unimplemented.
    // Once the daemon is fully implemented in subsequent commits, this will be un-ignored and pass.
    panic!("Daemon service is not yet implemented");
}
