use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::{ReadCommitRequest, RegisterRepositoryRequest, WriteCommitRequest};
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::{ReadOperationRequest, WriteOperationRequest};
use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use cc_common::workspace::{DeleteWorkspaceRequest, GetWorkspaceRequest, UpdateWorkspaceRequest, WorkspaceState};
use hyper_util::rt::TokioIo;
use std::fs;
use testutils::{spawn_daemon, spawn_server, TestWorkspace};
use tokio::net::UnixStream;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;

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
async fn test_daemon_routes_rpc_calls_over_uds() {
    let server = spawn_server().await;
    let daemon = spawn_daemon(server.url()).await;

    let socket_path = daemon.socket_path().to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = socket_path.clone();
            async move {
                let stream = UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .expect("Failed to connect to daemon over UDS");

    let mut backend_client = BackendServiceClient::new(channel.clone());
    let mut op_store_client = OpStoreServiceClient::new(channel.clone());
    let mut workspace_client = WorkspaceServiceClient::new(channel);

    // Test BackendService over daemon UDS
    let reg_resp = backend_client
        .register_repository(RegisterRepositoryRequest {
            name: Some("daemon-test-repo".to_string()),
        })
        .await
        .expect("Failed to register repo via daemon")
        .into_inner();
    let repo_id = reg_resp.repo_id;
    assert!(!repo_id.is_empty());

    let test_commit = cc_common::backend::Commit {
        parent_commit_ids: vec![],
        predecessors: vec![],
        root_tree_id: vec![vec![1, 2, 3]],
        change_id: vec![4, 5, 6],
        description: "Commit through daemon".to_string(),
        author: None,
        committer: None,
        commit_id: vec![10, 11, 12],
        conflict_labels: vec![],
        secure_sig: None,
    };

    let write_commit_resp = backend_client
        .write_commit(WriteCommitRequest {
            repo_id: repo_id.clone(),
            commit: Some(test_commit.clone()),
        })
        .await
        .expect("Failed to write commit via daemon")
        .into_inner();
    assert_eq!(write_commit_resp.commit_id, vec![10, 11, 12]);

    let read_commit_resp = backend_client
        .read_commit(ReadCommitRequest {
            repo_id: repo_id.clone(),
            commit_id: vec![10, 11, 12],
        })
        .await
        .expect("Failed to read commit via daemon")
        .into_inner();
    assert_eq!(
        read_commit_resp.commit.unwrap().description,
        "Commit through daemon"
    );

    // Test OpStoreService over daemon UDS
    let test_op = cc_common::op_store::Operation {
        parents: vec![],
        view_id: vec![20, 21, 22],
        metadata: None,
        commit_predecessors: vec![],
        commit_predecessors_set: false,
    };

    let write_op_resp = op_store_client
        .write_operation(WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(test_op),
        })
        .await
        .expect("Failed to write operation via daemon")
        .into_inner();
    let op_id = write_op_resp.operation_id;
    assert!(!op_id.is_empty());

    let read_op_resp = op_store_client
        .read_operation(ReadOperationRequest {
            repo_id: repo_id.clone(),
            operation_id: op_id,
        })
        .await
        .expect("Failed to read operation via daemon")
        .into_inner();
    assert_eq!(read_op_resp.operation.unwrap().view_id, vec![20, 21, 22]);

    // Test WorkspaceService over daemon UDS
    let ws_state = WorkspaceState {
        repo_id: repo_id.clone(),
        user: "daemon.user@example.com".to_string(),
        workspace_name: "default".to_string(),
        commit_id: vec![10, 11, 12],
        operation_id: vec![30, 31, 32],
        tree_id: vec![1, 2, 3],
    };

    workspace_client
        .update_workspace(UpdateWorkspaceRequest {
            workspace: Some(ws_state.clone()),
        })
        .await
        .expect("Failed to update workspace via daemon");

    let get_ws_resp = workspace_client
        .get_workspace(GetWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "daemon.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to get workspace via daemon")
        .into_inner();
    assert!(get_ws_resp.workspace.is_some());
    assert_eq!(
        get_ws_resp.workspace.unwrap().commit_id,
        vec![10, 11, 12]
    );

    let del_ws_resp = workspace_client
        .delete_workspace(DeleteWorkspaceRequest {
            repo_id: repo_id.clone(),
            user: "daemon.user@example.com".to_string(),
            workspace_name: "default".to_string(),
        })
        .await
        .expect("Failed to delete workspace via daemon")
        .into_inner();
    assert!(del_ws_resp.success);
}

#[tokio::test]
async fn test_daemon_auto_spawn_on_cli_command() {
    let server = spawn_server().await;
    let temp_dir = tempfile::tempdir().unwrap();

    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("auto_spawn.sock");
    assert!(!socket_path.exists());

    let mut init_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    init_cmd
        .current_dir(temp_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args([
            "cc",
            "init",
            "--create-repo",
            "--server",
            server.url(),
            ".",
        ]);
    init_cmd.assert().success();

    // Set custom socket in config.toml to test auto-spawning to that specific path
    let config_path = temp_dir.path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let mut config: toml::Value = toml::from_str(&config_str).unwrap();
    config.as_table_mut().unwrap().insert(
        "daemon_socket".to_string(),
        toml::Value::String(socket_path.to_str().unwrap().to_string()),
    );
    fs::write(&config_path, toml::to_string_pretty(&config).unwrap()).unwrap();

    // Verify socket does not exist yet before any commands run
    assert!(!socket_path.exists());

    // Run describe command - should auto-spawn the daemon at the specified socket
    let test_file = temp_dir.path().join("test.txt");
    fs::write(&test_file, "auto spawned daemon test").unwrap();

    let mut describe_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    describe_cmd
        .current_dir(temp_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["describe", "-m", "auto spawn commit"])
        .assert()
        .success();

    // Verify socket was created and is active
    assert!(socket_path.exists(), "Daemon socket should have been auto-spawned");

    // Run log command to verify it communicates through the auto-spawned daemon
    let mut log_cmd = assert_cmd::Command::cargo_bin("jj").unwrap();
    let output = log_cmd
        .current_dir(temp_dir.path())
        .env("JJ_USER", "Test User")
        .env("JJ_EMAIL", "test.user@example.com")
        .args(["log"])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    assert!(stdout.contains("auto spawn commit"));
}
