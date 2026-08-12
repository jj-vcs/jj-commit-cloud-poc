use std::fs;

#[tokio::test]
async fn test_file_snapshot_integration() {
    let workspace = testutils::TestWorkspace::init().await;
    let repo_path = workspace.repo_path();

    // Create a file in the working copy
    fs::write(repo_path.join("hello.txt"), "hello commit cloud!\n").unwrap();

    workspace
        .jj_cmd()
        .args(["describe", "-m", "snapshot test"])
        .assert()
        .success();

    // Run jj log with -T description to only print the commit description and verify string match to the set description above
    workspace
        .jj_cmd()
        .args(["log", "--no-graph", "-r", "@", "-T", "description"])
        .assert()
        .success()
        .stdout("snapshot test\n");
}
