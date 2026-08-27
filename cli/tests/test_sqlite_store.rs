use std::fs;

#[tokio::test]
#[should_panic]
async fn test_sqlite_store_init_and_snapshot_succeeds() {
    let db_dir = tempfile::tempdir().expect("Failed to create temp dir for sqlite db");
    let db_path = db_dir.path().join("test_store.db");

    let workspace = testutils::TestWorkspace::init_sqlite(&db_path).await;
    let repo_path = workspace.repo_path();

    // Create a file in the working copy and snapshot it
    fs::write(repo_path.join("sqlite_test.txt"), "hello sqlite store!\n").unwrap();

    workspace
        .jj_cmd()
        .args(["describe", "-m", "sqlite store test commit"])
        .assert()
        .success();

    // Verify commit description can be read back from SQLite store
    workspace
        .jj_cmd()
        .args(["log", "--no-graph", "-r", "@", "-T", "description"])
        .assert()
        .success()
        .stdout("sqlite store test commit\n");
}
