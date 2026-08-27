use std::fs;

#[tokio::test]
async fn test_spanner_store_init_and_snapshot() {
    let db_name = "projects/test-project/instances/test-instance/databases/test-db";

    let workspace = testutils::TestWorkspace::init_spanner(db_name).await;
    let repo_path = workspace.repo_path();

    // Create a file in the working copy and snapshot it
    fs::write(repo_path.join("spanner_test.txt"), "hello spanner store!\n").unwrap();

    workspace
        .jj_cmd()
        .args(["describe", "-m", "spanner store test commit"])
        .assert()
        .success();

    // Verify commit description can be read back from Spanner store
    workspace
        .jj_cmd()
        .args(["log", "--no-graph", "-r", "@", "-T", "description"])
        .assert()
        .success()
        .stdout("spanner store test commit\n");
}
