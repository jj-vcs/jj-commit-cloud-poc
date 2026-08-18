use std::fs;
use tempfile::tempdir;

// Initialize a commit cloud repository and verify that the type file returns commit_cloud for the op store and op heads store.
#[tokio::test]
async fn test_cc_init_op_store_type() {
    let server = testutils::spawn_server().await;
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

    let op_store_type = fs::read_to_string(repo_path.join(".jj/repo/op_store/type"))
        .expect("op_store type file should exist");
    assert_eq!(op_store_type.trim(), "commit_cloud");

    let op_heads_type = fs::read_to_string(repo_path.join(".jj/repo/op_heads/type"))
        .expect("op_heads type file should exist");
    assert_eq!(op_heads_type.trim(), "commit_cloud");
}

// Run jj operation, modify file, and verify that the commit cloud operation log returns the accurate result
#[tokio::test]
async fn test_op_log_command_integration() {
    let server = testutils::spawn_server().await;

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

    let mut op_log_cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");

    op_log_cmd
        .current_dir(repo_path)
        .args(["op", "log", "--no-graph", "-T", "description"]);

    op_log_cmd
        .assert()
        .success()
        .stdout("add workspace 'default'root()");

    fs::write(repo_path.join("file.txt"), "testing op log\n").unwrap();

    let mut desc_cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");
    desc_cmd
        .current_dir(repo_path)
        .args(["describe", "-m", "description 0"]);
    desc_cmd.assert().success();

    let mut log_cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");

    log_cmd
        .current_dir(repo_path)
        .args(["log", "--no-graph", "-r", "@", "-T", "description"]);

    log_cmd
        .assert()
        .success()
        .stdout("description 0\n");
}
