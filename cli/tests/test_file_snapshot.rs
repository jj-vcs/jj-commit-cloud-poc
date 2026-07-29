use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_file_snapshot_integration() {
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

    // Create a file in the working copy
    fs::write(repo_path.join("hello.txt"), "hello commit cloud!\n").unwrap();

    let mut desc_cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");
    desc_cmd
        .current_dir(repo_path)
        .args(["describe", "-m", "snapshot test"]);
    desc_cmd.assert().success();

    // Run jj log with -T description to only print the commit description and verify string match to the set description above
    let mut log_cmd = assert_cmd::Command::cargo_bin("jj")
        .expect("The jj CLI binary should have compiled");

    log_cmd
        .current_dir(repo_path)
        .args(["log", "--no-graph", "-r", "@", "-T", "description"]);

    log_cmd
        .assert()
        .success()
        .stdout("snapshot test\n");
}
