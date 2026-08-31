use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use cc_common::workspace::GetWorkspaceRequest;
use std::fs;
use testutils::TestWorkspace;

#[tokio::test]
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
async fn test_cc_init_working_copy_type_local() {
    let server = testutils::spawn_server().await;
    let temp_dir = tempfile::tempdir().unwrap();

    let mut cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    cmd.env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--create",
            "--server",
            server.url(),
            "--working-copy-type",
            "local",
            temp_dir.path().to_str().unwrap(),
        ]);
    cmd.assert().success();

    let working_copy_type_path = temp_dir.path().join(".jj/working_copy/type");
    assert!(working_copy_type_path.exists());
    let working_copy_type = fs::read_to_string(&working_copy_type_path).unwrap();
    assert_eq!(working_copy_type.trim(), "local");

    // Verify commands like describe and log work cleanly with local working copy
    let mut describe_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    describe_cmd
        .current_dir(temp_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["describe", "-m", "local working copy commit"])
        .assert()
        .success();

    let mut log_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    let output = log_cmd
        .current_dir(temp_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["log"])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    assert!(stdout.contains("local working copy commit"));
}

#[tokio::test]
async fn test_working_copy_syncs_state_to_server() {
    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let user = "test.user@example.com".to_string();
    let response = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: user.clone(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to get workspace")
        .into_inner();

    let ws_state = response.workspace.expect("Workspace should be registered on server");
    assert_eq!(ws_state.repo_id, repo_id);
    assert_eq!(ws_state.user, user);
    assert_eq!(ws_state.workspace_name, "default");
    assert!(!ws_state.operation_id.is_empty());
}

#[tokio::test]
async fn test_working_copy_snapshot_and_change_detection() {
    let ws = TestWorkspace::init().await;

    // Create a file in working directory and run describe to trigger a snapshot
    let test_file = ws.repo_path().join("hello.txt");
    fs::write(&test_file, "hello from commit cloud working copy").unwrap();

    let mut cmd = ws.jj_cmd();
    cmd.args(["describe", "-m", "add hello.txt"]).assert().success();

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let user = "test.user@example.com".to_string();
    let response = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: user.clone(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to get workspace")
        .into_inner();

    let ws_state = response.workspace.expect("Workspace should exist on server");
    assert!(!ws_state.tree_id.is_empty(), "Working tree ID should not be empty");
    assert!(!ws_state.commit_id.is_empty(), "commit_id should not be empty");
}

#[tokio::test]
async fn test_working_copy_fails_without_user_identity() {
    let server = testutils::spawn_server().await;
    let temp_dir = tempfile::tempdir().unwrap();

    let mut cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    // Intentionally pass an empty config without user.email or user.name, and clear env vars
    cmd.env_remove("JJ_USER")
        .env_remove("JJ_EMAIL")
        .env_remove("USER")
        .args([
            "cc",
            "init",
            "--create",
            "--server",
            server.url(),
            "--config",
            "user.email=''",
            "--config",
            "user.name=''",
            temp_dir.path().to_str().unwrap(),
        ]);
    let output = cmd.assert().failure();
    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("user.email") || stderr.contains("User email"),
        "stderr should mention missing user.email, got: {stderr}"
    );
}

#[tokio::test]
async fn test_vfs_commit_cloud_working_copy_succeeds() {
    // Initialize a real Commit Cloud repository with jj-cc-server
    let ws = testutils::TestWorkspace::init().await;

    // Create files and make a commit
    let cloud_file = ws.repo_path().join("cloud_file.txt");
    fs::write(&cloud_file, "hello from commit cloud vfs\n").unwrap();
    let src_dir = ws.repo_path().join("src");
    fs::create_dir_all(&src_dir).unwrap();
    let lib_file = src_dir.join("lib.rs");
    fs::write(&lib_file, "pub fn cloud_fn() {}\n").unwrap();

    let mut describe_cmd = ws.jj_cmd();
    describe_cmd
        .args(["describe", "-m", "commit cloud vfs test"])
        .assert()
        .success();

    // Retrieve the commit ID of @
    let mut log_cmd = ws.jj_cmd();
    let log_output = log_cmd
        .args(["log", "-r", "@", "-T", "commit_id", "--no-graph"])
        .assert()
        .success();
    let commit_hex = String::from_utf8_lossy(&log_output.get_output().stdout)
        .trim()
        .to_string();
    assert!(!commit_hex.is_empty(), "commit_id should not be empty");

    // Perform sparse set --clear to verify 0 source files exist on disk
    let mut sparse_cmd = ws.jj_cmd();
    sparse_cmd
        .args(["sparse", "set", "--clear"])
        .assert()
        .success();

    // Verify physical files are no longer on local disk
    assert!(
        !cloud_file.exists(),
        "cloud_file.txt should be removed from disk after sparse clear"
    );
    assert!(
        !src_dir.exists(),
        "src/ directory should be removed from disk after sparse clear"
    );

    // Mount the VFS using the shared testutils helper
    let vfs = ws.mount_vfs().await;
    let mountpoint = vfs.mountpoint();

    // Verify browsing /commits/<commit_hex>/ and reading files through VFS
    let commit_dir = mountpoint.join("commits").join(&commit_hex);

    let vfs_file1 = commit_dir.join("cloud_file.txt");
    assert!(
        vfs_file1.exists(),
        "cloud_file.txt should exist in /commits/<id>/ via VFS"
    );
    let vfs_content1 =
        fs::read_to_string(&vfs_file1).expect("Failed to read cloud_file.txt via VFS");
    assert_eq!(vfs_content1, "hello from commit cloud vfs\n");

    let vfs_lib_file = commit_dir.join("src").join("lib.rs");
    assert!(
        vfs_lib_file.exists(),
        "src/lib.rs should exist in /commits/<id>/ via VFS"
    );
    let vfs_lib_content =
        fs::read_to_string(&vfs_lib_file).expect("Failed to read src/lib.rs via VFS");
    assert_eq!(vfs_lib_content, "pub fn cloud_fn() {}\n");

    // Verify directory listing on /commits/<commit_hex>
    let entries: Vec<String> = fs::read_dir(&commit_dir)
        .expect("Failed to read commit dir")
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries.contains(&"cloud_file.txt".to_string()),
        "Directory listing should contain cloud_file.txt"
    );
    assert!(
        entries.contains(&"src".to_string()),
        "Directory listing should contain src"
    );

    // Verify again that the physical local disk has 0 source files!
    assert!(
        !cloud_file.exists(),
        "Physical disk should still have 0 files"
    );
    assert!(
        !lib_file.exists(),
        "Physical disk should still have 0 files"
    );
}

#[tokio::test]
async fn test_delete_workspace_succeeds() {
    use cc_common::workspace::{DeleteWorkspaceRequest, ListWorkspacesRequest};

    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    let user = "test.user@example.com".to_string();

    // Verify workspace exists
    let response = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: user.clone(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to get workspace")
        .into_inner();
    assert!(response.workspace.is_some());

    // Delete workspace
    let del_resp = client
        .delete_workspace(DeleteWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: user.clone(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to delete workspace")
        .into_inner();
    assert!(del_resp.success);

    // Verify workspace is gone from database
    let response = client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: user.clone(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to query workspace")
        .into_inner();
    assert!(response.workspace.is_none());

    let list_resp = client
        .list_workspaces(ListWorkspacesRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .expect("Failed to list workspaces")
        .into_inner();
    assert!(list_resp.workspaces.is_empty());
}

#[tokio::test]
async fn test_delete_workspace_nonexistent_returns_false() {
    use cc_common::workspace::DeleteWorkspaceRequest;

    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    // Attempt to delete a nonexistent workspace name
    let del_resp = client
        .delete_workspace(DeleteWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "test.user@example.com".to_string(),
            workspace_name: "nonexistent_workspace".to_string(),
        })
        .await
        .expect("Failed to call delete_workspace")
        .into_inner();
    assert!(!del_resp.success, "Deleting a nonexistent workspace should return success = false");

    // Attempt to delete from a nonexistent repository
    let del_resp2 = client
        .delete_workspace(DeleteWorkspaceRequest {
            repo_id: "nonexistent_repo".to_string(),
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to call delete_workspace")
        .into_inner();
    assert!(!del_resp2.success, "Deleting from a nonexistent repository should return success = false");
}

#[tokio::test]
async fn test_check_working_copy_changes_succeeds() {
    use cc_common::workspace::CheckWorkingCopyChangesRequest;

    let ws = TestWorkspace::init().await;

    // Create a commit so the commit exists in the store
    let test_file = ws.repo_path().join("file.txt");
    fs::write(&test_file, "content").unwrap();
    let mut cmd = ws.jj_cmd();
    cmd.args(["describe", "-m", "test commit"]).assert().success();

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    // Query changes on a workspace after snapshot
    let response = client
        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
            repo_id,
            user: "test.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to check working copy changes")
        .into_inner();

    assert!(!response.has_changes, "Workspace should have no un-snapshotted changes after describe");
    assert_eq!(response.current_tree_id, response.commit_tree_id);
}

#[tokio::test]
async fn test_check_working_copy_changes_nonexistent_workspace_fails() {
    use cc_common::workspace::CheckWorkingCopyChangesRequest;

    let ws = TestWorkspace::init().await;

    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    let mut client = WorkspaceServiceClient::connect(ws.server_url().to_string())
        .await
        .expect("Failed to connect to WorkspaceService");

    // Check changes for a nonexistent workspace
    let result = client
        .check_working_copy_changes(CheckWorkingCopyChangesRequest {
            repo_id,
            user: "test.user@example.com".to_string(),
            workspace_name: "nonexistent".to_string(),
        })
        .await;

    assert!(result.is_err(), "Expected error when checking changes for nonexistent workspace");
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_workspace_forget_cli() {
    let ws = TestWorkspace::init().await;

    // Verify workspace list shows default workspace
    let mut list_cmd = ws.jj_cmd();
    let output = list_cmd
        .args(["workspace", "list"])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    assert!(stdout.contains("default"), "Workspace list should include default");

    // Forget the default workspace
    let mut forget_cmd = ws.jj_cmd();
    forget_cmd
        .args(["workspace", "forget", "default"])
        .assert()
        .success();

    // Verify workspace list is now empty or default is removed
    let mut list_cmd2 = ws.jj_cmd();
    let output2 = list_cmd2
        .args(["workspace", "list"])
        .assert()
        .success();
    let stdout2 = String::from_utf8_lossy(&output2.get_output().stdout);
    assert!(!stdout2.contains("default:"), "Workspace list should not show active default workspace");
}

#[tokio::test]
async fn test_workspace_forget_nonexistent_cli_warning() {
    let ws = TestWorkspace::init().await;

    // Attempt to forget a non-existent workspace name via CLI
    let mut cmd = ws.jj_cmd();
    let output = cmd
        .args(["workspace", "forget", "nonexistent_workspace"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("No such workspace: nonexistent_workspace"),
        "stderr should warn about nonexistent workspace, got: {stderr}"
    );
}
