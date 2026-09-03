use std::collections::HashSet;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::Barrier;

use cc_common::backend::backend_service_client::BackendServiceClient;
use cc_common::backend::RegisterRepositoryRequest;
use cc_common::op_store::op_store_service_client::OpStoreServiceClient;
use cc_common::op_store::{
    GetOpHeadsRequest, Operation, OperationMetadata, ReadOperationRequest,
    ReconcileOpHeadsRequest, UpdateOpHeadsRequest, WriteOperationRequest,
};
use testutils::{spawn_server, spawn_sqlite_server};

/// Helper to register a new repository and return the repo_id.
async fn register_test_repo(server_url: &str) -> String {
    let mut backend_client = BackendServiceClient::connect(server_url.to_string())
        .await
        .expect("Failed to connect backend client");
    let response = backend_client
        .register_repository(tonic::Request::new(RegisterRepositoryRequest {
            name: None,
        }))
        .await
        .expect("Failed to register repository");
    response.into_inner().repo_id
}


/// Verifies that write races on stale op heads fail with Status::Aborted on SQLite backend.
#[tokio::test]
async fn test_sqlite_concurrent_write_race_fails_on_stale_old_heads() {
    let db_file = NamedTempFile::new().unwrap();
    let server = spawn_sqlite_server(db_file.path()).await;
    let repo_id = register_test_repo(server.url()).await;
    let mut op_client_1 = OpStoreServiceClient::connect(server.url().to_string())
        .await
        .unwrap();
    let mut op_client_2 = OpStoreServiceClient::connect(server.url().to_string())
        .await
        .unwrap();

    let root_head = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();

    // Client 1 advances root -> op_1
    let op_1 = vec![0x11; 20];
    op_client_1
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![root_head.clone()],
            new_op_head_id: op_1.clone(),
        })
        .await
        .unwrap();

    // Client 2 tries to update using stale root_head
    let op_2 = vec![0x22; 20];
    let result = op_client_2
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![root_head.clone()],
            new_op_head_id: op_2.clone(),
        })
        .await;

    assert!(result.is_err(), "Expected SQLite stale op_heads update to fail with CAS conflict");
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::Aborted);
    assert!(status.message().contains("CAS conflict"));
}


/// Stress test: 20 concurrent tasks firing simultaneous UpdateOpHeads RPCs against SQLite backend.
#[tokio::test]
async fn test_stress_concurrent_op_heads_writes_sqlite() {
    let db_file = NamedTempFile::new().unwrap();
    let server = spawn_sqlite_server(db_file.path()).await;
    let repo_id = register_test_repo(server.url()).await;

    const NUM_CONCURRENT_WORKERS: usize = 20;
    let barrier = Arc::new(Barrier::new(NUM_CONCURRENT_WORKERS));

    let mut handles = vec![];
    for worker_idx in 0..NUM_CONCURRENT_WORKERS {
        let server_url = server.url().to_string();
        let repo_id = repo_id.clone();
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            let mut op_client = OpStoreServiceClient::connect(server_url)
                .await
                .expect("Failed to connect client in worker");

            // Synchronize all tasks to hit the SQLite database simultaneously
            barrier.wait().await;

            let worker_op = vec![(worker_idx + 1) as u8; 20];
            let response = op_client
                .update_op_heads(UpdateOpHeadsRequest {
                    repo_id,
                    old_op_head_ids: vec![],
                    new_op_head_id: worker_op.clone(),
                })
                .await;

            assert!(response.is_ok(), "SQLite Worker {} update_op_heads failed: {:?}", worker_idx, response.err());
            worker_op
        }));
    }

    let mut inserted_ops = HashSet::new();
    for handle in handles {
        let op_id = handle.await.expect("Task should not panic");
        inserted_ops.insert(op_id);
    }

    // Verify all 20 heads were safely written into SQLite without lock errors
    let mut op_client = OpStoreServiceClient::connect(server.url().to_string())
        .await
        .unwrap();
    let final_heads = op_client
        .get_op_heads(GetOpHeadsRequest { repo_id })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;

    let final_set: HashSet<Vec<u8>> = final_heads.into_iter().collect();
    for op in &inserted_ops {
        assert!(final_set.contains(op), "Expected op head {:?} in SQLite", op);
    }
}

fn make_test_operation(view_id: Vec<u8>, parents: Vec<Vec<u8>>, description: &str) -> Operation {
    Operation {
        view_id,
        parents,
        metadata: Some(OperationMetadata {
            start_time_millis: 1000,
            end_time_millis: 1000,
            description: description.to_string(),
            is_snapshot: false,
            workspace_name: None,
            hostname: "test-host".to_string(),
            username: "test-user".to_string(),
            attributes: std::collections::HashMap::new(),
        }),
        commit_predecessors: vec![],
        commit_predecessors_set: true,
    }
}

/// Verifies that ReconcileOpHeads is a no-op fast-path when only a single op head exists.
#[tokio::test]
async fn test_reconcile_op_heads_idempotent_on_single_head() {
    let server = spawn_server().await;
    let repo_id = register_test_repo(server.url()).await;
    let mut op_client = OpStoreServiceClient::connect(server.url().to_string())
        .await
        .expect("Failed to connect op store client");

    let root_head = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();

    // On single root head, ReconcileOpHeads returns root immediately
    let response = op_client
        .reconcile_op_heads(ReconcileOpHeadsRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.op_head, root_head);
}

/// Verifies that running a jj CLI command (e.g. `jj op log`) in a repo with
/// divergent server op heads automatically triggers reconciliation and succeeds without error.
#[tokio::test]
async fn test_cli_command_auto_reconciles_divergent_op_heads() {
    let workspace = testutils::TestWorkspace::init().await;
    let config = cc_lib::util::CommitCloudConfig::load_from_store(
        &workspace.repo_path().join(".jj/repo/store"),
    )
    .expect("Failed to load repo config");

    let repo_id = config.repo_id;
    let server_url = config.server_url;
    let mut op_client = OpStoreServiceClient::connect(server_url)
        .await
        .expect("Failed to connect op client");

    // Fetch current single head
    let current_heads = op_client
        .get_op_heads(GetOpHeadsRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;
    assert_eq!(current_heads.len(), 1);
    let base_head = current_heads[0].clone();

    // Read base operation to get its view_id
    let base_op = op_client
        .read_operation(ReadOperationRequest {
            repo_id: repo_id.clone(),
            operation_id: base_head.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .expect("Base operation should exist");

    let base_view_id = base_op.view_id;

    // Create and write two divergent operations pointing to base_head
    let op_a_proto = make_test_operation(
        base_view_id.clone(),
        vec![base_head.clone()],
        "concurrent cli branch a",
    );
    let op_branch_a = op_client
        .write_operation(WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(op_a_proto),
        })
        .await
        .unwrap()
        .into_inner()
        .operation_id;

    let op_b_proto = make_test_operation(
        base_view_id.clone(),
        vec![base_head.clone()],
        "concurrent cli branch b",
    );
    let op_branch_b = op_client
        .write_operation(WriteOperationRequest {
            repo_id: repo_id.clone(),
            operation: Some(op_b_proto),
        })
        .await
        .unwrap()
        .into_inner()
        .operation_id;

    // Inject divergent heads on server:
    // Branch A replaces base_head with op_branch_a
    op_client
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![base_head.clone()],
            new_op_head_id: op_branch_a.clone(),
        })
        .await
        .unwrap();

    // Branch B adds op_branch_b concurrently
    op_client
        .update_op_heads(UpdateOpHeadsRequest {
            repo_id: repo_id.clone(),
            old_op_head_ids: vec![],
            new_op_head_id: op_branch_b.clone(),
        })
        .await
        .unwrap();

    // Verify the server store now has 2 divergent heads
    let raw_heads = op_client
        .get_op_heads(GetOpHeadsRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;
    assert_eq!(raw_heads.len(), 2);

    // Run a jj CLI command (`jj op log --no-graph -T description`)
    // The CLI loads the repo via RepoLoader, which invokes CommitCloudOpHeadsStore::get_op_heads().
    // Seeing 2 heads, CommitCloudOpHeadsStore calls the ReconcileOpHeads RPC on the server.
    workspace
        .jj_cmd()
        .args(["op", "log", "--no-graph", "-T", "description"])
        .assert()
        .success();

    // Verify that the server's op heads were automatically collapsed down to 1 merged head
    let post_heads = op_client
        .get_op_heads(GetOpHeadsRequest {
            repo_id: repo_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .op_head_ids;
    assert_eq!(post_heads.len(), 1);
    let final_head = &post_heads[0];
    assert_ne!(final_head, &op_branch_a);
    assert_ne!(final_head, &op_branch_b);

    // Verify the merged operation in the server operation log
    let _merged_op = op_client
        .read_operation(ReadOperationRequest {
            repo_id: repo_id.clone(),
            operation_id: final_head.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .expect("Merged op should exist");
}
