use std::fs;
use testutils::TestWorkspace;

#[tokio::test]
#[should_panic]
async fn test_cc_init_working_copy_type_returns_commit_cloud() {
    let ws = TestWorkspace::init().await;

    let working_copy_type_path = ws.repo_path().join(".jj/working_copy/type");
    assert!(
        working_copy_type_path.exists(),
        "Working copy type file should exist at .jj/working_copy/type"
    );

    let working_copy_type = fs::read_to_string(&working_copy_type_path).unwrap();
    assert_eq!(
        working_copy_type.trim(),
        "commit_cloud",
        "Working copy type should be 'commit_cloud'"
    );
}

#[tokio::test]
#[should_panic]
async fn test_working_copy_snapshot_and_change_detection() {
    let ws = TestWorkspace::init().await;

    // Create a file in working directory and run describe to trigger a snapshot
    let test_file = ws.repo_path().join("hello.txt");
    fs::write(&test_file, "hello from commit cloud working copy").unwrap();

    let mut cmd = ws.jj_cmd();
    cmd.args(["describe", "-m", "add hello.txt"]).assert().success();

    let working_copy_type_path = ws.repo_path().join(".jj/working_copy/type");
    let working_copy_type = fs::read_to_string(&working_copy_type_path).unwrap();
    assert_eq!(
        working_copy_type.trim(),
        "commit_cloud",
        "Working copy type should be 'commit_cloud'"
    );
}

#[tokio::test]
#[should_panic]
async fn test_vfs_commit_cloud_working_copy_succeeds() {
    let ws = TestWorkspace::init().await;

    let cloud_file = ws.repo_path().join("cloud_file.txt");
    fs::write(&cloud_file, "hello from commit cloud vfs\n").unwrap();

    let mut describe_cmd = ws.jj_cmd();
    describe_cmd
        .args(["describe", "-m", "commit cloud vfs test"])
        .assert()
        .success();

    let mut sparse_cmd = ws.jj_cmd();
    sparse_cmd
        .args(["sparse", "set", "--clear"])
        .assert()
        .success();

    assert!(
        !cloud_file.exists(),
        "cloud_file.txt should be removed from disk after sparse clear"
    );

    let vfs = ws.mount_vfs().await;
    let mountpoint = vfs.mountpoint();

    let entries: Vec<String> = fs::read_dir(mountpoint.join("commits"))
        .expect("Failed to read commits dir")
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert!(!entries.is_empty());
}
