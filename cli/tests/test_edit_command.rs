use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_edit_command_commit_cloud() {
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

    // 4. Create and edit a file
    let file1 = repo_path.join("file1.txt");
    fs::write(&file1, "initial content\n").unwrap();

    // 5. Run sjj new to create a new commit on top of file1
    let mut new_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    new_cmd
        .current_dir(repo_path)
        .args(["new"]);
    new_cmd.assert().success();

    // 6. Edit a second file in the new working copy
    let file2 = repo_path.join("file2.txt");
    fs::write(&file2, "second file content\n").unwrap();

    // 7. Verify both files exist
    assert!(file1.exists());
    assert!(file2.exists());
}
