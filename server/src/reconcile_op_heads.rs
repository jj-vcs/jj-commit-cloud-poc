#![allow(elided_lifetimes_in_paths)]

use std::sync::Arc;

use jj_lib::object_id::ObjectId as _;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_store::OpStore;
use jj_lib::repo::RepoLoader;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::store::Store as JjStore;
use jj_lib::tree_merge::MergeOptions;

use crate::jj_lib_adapters::{
    ServerBackend, ServerIndexStore, ServerOpHeadsStore, ServerOpStore, ServerSubmoduleStore,
};
use crate::store::Store;

static RECONCILER_ENV: std::sync::LazyLock<(UserSettings, MergeOptions)> =
    std::sync::LazyLock::new(|| {
        let user_settings =
            UserSettings::from_config(jj_lib::config::StackedConfig::with_defaults())
                .expect("failed to load default user settings");
        let merge_options = MergeOptions::from_settings(&user_settings)
            .expect("failed to create merge options from settings");
        (user_settings, merge_options)
    });

pub async fn reconcile_repo_op_heads(
    store: Arc<dyn Store>,
    repo_id: &str,
) -> Result<Vec<u8>, tonic::Status> {
    const MAX_RETRIES: usize = 3;
    const BASE_BACKOFF_MS: u64 = 25;
    const MAX_JITTER_MS: u64 = 15;

    for attempt in 0..MAX_RETRIES {
        let heads = store
            .get_op_heads(repo_id)
            .await?
            .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);

        if heads.len() <= 1 {
            let head = heads
                .into_iter()
                .next()
                .unwrap_or_else(|| cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
            return Ok(head);
        }

        let store_clone = store.clone();
        let repo_id_owned = repo_id.to_string();

        // Offload reconciliation to Tokio's blocking thread pool (`spawn_blocking`).
        // Why: Reconciling divergent operation heads involves CPU-heavy graph merges and hashing.
        // Running this directly on Tokio's async worker threads could stall other incoming gRPC requests.
        // Inside `spawn_blocking`, we use `pollster::block_on` to run the async `load_at_head()`
        // method synchronously on the dedicated blocking thread without spawning a nested Tokio runtime.
        let reconcile_result = tokio::task::spawn_blocking(move || {
            pollster::block_on(async move {
                let (ref user_settings, ref merge_options) = *RECONCILER_ENV;
                let signer = Signer::from_settings(user_settings)
                    .map_err(|e| format!("failed to initialize signer: {e}"))?;
                let index_store = Arc::new(ServerIndexStore);
                let submodule_store = Arc::new(ServerSubmoduleStore);

                let server_backend =
                    Box::new(ServerBackend::new(store_clone.clone(), repo_id_owned.clone()));
                let jj_store =
                    JjStore::new(server_backend, signer, merge_options.clone());
                let op_store: Arc<dyn OpStore> =
                    Arc::new(ServerOpStore::new(store_clone.clone(), repo_id_owned.clone()));
                let op_heads_store: Arc<dyn OpHeadsStore> =
                    Arc::new(ServerOpHeadsStore::new(store_clone.clone(), repo_id_owned.clone()));

                // TODO: optimize shared jj-lib adapter trait construction to not create new ones for every retry
                let repo_loader = RepoLoader::new(
                    user_settings.clone(),
                    jj_store,
                    op_store,
                    op_heads_store,
                    index_store,
                    submodule_store,
                );

                // This is where the core reconciliation happens. Calling `load_at_head()` invokes
                // `jj_lib::op_heads_store::resolve_op_heads()`, which identifies divergent operation heads,
                // performs a 3-way view merge, creates a new merge operation, and eventually invokes
                // the `Store` trait (`crate::store::Store::update_op_heads` via `ServerOpHeadsStore`)
                // with strict exact CAS verification.
                let repo = repo_loader.load_at_head().await.map_err(|e| e.to_string())?;
                Ok::<Vec<u8>, String>(repo.op_id().as_bytes().to_vec())
            })
        })
        .await
        .map_err(|e| tonic::Status::internal(format!("Reconciliation task panicked: {e}")))?;

        // If reconciliation fails (e.g. CAS conflict because another concurrent client or reconciliation
        // updated op_heads in the meantime), retry up to MAX_RETRIES times using exponential backoff
        // (BASE_BACKOFF_MS << attempt, i.e. 2^attempt * 25ms) plus randomized bounded jitter in [0, MAX_JITTER_MS)
        // (upper-bounded strictly below 15ms) to desynchronize concurrent reconciliation attempts on the same repo.
        // On each retry, the loop re-fetches the latest op heads and recalculates the resolved merge operation
        // using jj-lib.
        match reconcile_result {
            Ok(op_id) => {
                return Ok(op_id);
            }
            Err(_e) if attempt + 1 < MAX_RETRIES => {
                let jitter = rand::random::<u64>() % MAX_JITTER_MS;
                let sleep_duration =
                    std::time::Duration::from_millis((BASE_BACKOFF_MS << attempt) + jitter);
                tokio::time::sleep(sleep_duration).await;
            }
            Err(e) => {
                return Err(tonic::Status::internal(format!(
                    "Failed to reconcile op heads for repo {repo_id}: {e}"
                )));
            }
        }
    }

    let final_heads = store
        .get_op_heads(repo_id)
        .await?
        .unwrap_or_else(|| vec![cc_common::ROOT_OPERATION_ID_BYTES.to_vec()]);
    Ok(final_heads
        .into_iter()
        .next()
        .unwrap_or_else(|| cc_common::ROOT_OPERATION_ID_BYTES.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memorystore::MemoryStore;
    use cc_common::conversions::op_store::{operation_from_proto, view_from_proto};
    use cc_common::op_store::{
        Operation as ProtoOperation, OperationMetadata as ProtoOperationMetadata,
        View as ProtoView,
    };
    use jj_lib::op_store::OperationId;
    use std::collections::HashSet;

    fn make_test_proto_op(view_id: Vec<u8>, parents: Vec<Vec<u8>>, desc: &str) -> ProtoOperation {
        ProtoOperation {
            view_id,
            parents,
            metadata: Some(ProtoOperationMetadata {
                start_time_millis: 1000,
                end_time_millis: 1000,
                description: desc.to_string(),
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

    #[tokio::test]
    async fn test_reconcile_repo_op_heads_single_head_is_noop() {
        let store = Arc::new(MemoryStore::new());
        let repo_id = "test-single-head-repo";
        store.register_repo(repo_id.to_string(), None).await.unwrap();

        let root_op_bytes = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let result = reconcile_repo_op_heads(store.clone(), repo_id)
            .await
            .unwrap();
        assert_eq!(result, root_op_bytes);
    }

    #[tokio::test]
    async fn test_reconcile_op_heads_matches_jj_lib_resolution() {
        use std::collections::HashMap;

        let store = Arc::new(MemoryStore::new());
        let repo_id_jj = "repo-jj-direct";
        let repo_id_server = "repo-server-reconcile";
        store.register_repo(repo_id_jj.to_string(), None).await.unwrap();
        store.register_repo(repo_id_server.to_string(), None).await.unwrap();

        let root_op_bytes = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();

        // Branch A introduces "branch-a" bookmark
        let mut bookmarks_a = HashMap::new();
        bookmarks_a.insert(
            "branch-a".to_string(),
            cc_common::op_store::RefTarget {
                removes: vec![],
                adds: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: vec![0x01; 20],
                }],
            },
        );
        let view_a_proto = ProtoView {
            head_ids: vec![vec![0x01; 20]],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: bookmarks_a,
            remote_bookmarks: HashMap::new(),
        };
        let view_a_bytes = crate::hash_utils::hash_view(&view_a_proto);
        store
            .put_view(repo_id_jj.to_string(), view_a_bytes.clone(), view_a_proto.clone())
            .await
            .unwrap();
        store
            .put_view(repo_id_server.to_string(), view_a_bytes.clone(), view_a_proto)
            .await
            .unwrap();

        // Branch B introduces "branch-b" bookmark
        let mut bookmarks_b = HashMap::new();
        bookmarks_b.insert(
            "branch-b".to_string(),
            cc_common::op_store::RefTarget {
                removes: vec![],
                adds: vec![cc_common::op_store::RefTargetTerm {
                    commit_id: vec![0x02; 20],
                }],
            },
        );
        let view_b_proto = ProtoView {
            head_ids: vec![vec![0x02; 20]],
            wc_commit_ids: HashMap::new(),
            local_bookmarks: bookmarks_b,
            remote_bookmarks: HashMap::new(),
        };
        let view_b_bytes = crate::hash_utils::hash_view(&view_b_proto);
        store
            .put_view(repo_id_jj.to_string(), view_b_bytes.clone(), view_b_proto.clone())
            .await
            .unwrap();
        store
            .put_view(repo_id_server.to_string(), view_b_bytes.clone(), view_b_proto)
            .await
            .unwrap();

        // Operation A
        let op_a_bytes = vec![0x0A; 20];
        let op_a_proto = make_test_proto_op(view_a_bytes.clone(), vec![root_op_bytes.clone()], "branch a");
        store
            .put_operation(repo_id_jj.to_string(), op_a_bytes.clone(), op_a_proto.clone())
            .await
            .unwrap();
        store
            .put_operation(repo_id_server.to_string(), op_a_bytes.clone(), op_a_proto)
            .await
            .unwrap();

        // Operation B
        let op_b_bytes = vec![0x0B; 20];
        let op_b_proto = make_test_proto_op(view_b_bytes.clone(), vec![root_op_bytes.clone()], "branch b");
        store
            .put_operation(repo_id_jj.to_string(), op_b_bytes.clone(), op_b_proto.clone())
            .await
            .unwrap();
        store
            .put_operation(repo_id_server.to_string(), op_b_bytes.clone(), op_b_proto)
            .await
            .unwrap();

        // Set divergent op heads [op_a, op_b] in both repositories
        store
            .update_op_heads_append_row(repo_id_jj.to_string(), &[root_op_bytes.clone()], op_a_bytes.clone())
            .await
            .unwrap();
        store
            .update_op_heads_append_row(repo_id_jj.to_string(), &[], op_b_bytes.clone())
            .await
            .unwrap();

        store
            .update_op_heads_append_row(repo_id_server.to_string(), &[root_op_bytes.clone()], op_a_bytes.clone())
            .await
            .unwrap();
        store
            .update_op_heads_append_row(repo_id_server.to_string(), &[], op_b_bytes.clone())
            .await
            .unwrap();

        // Direct jj-lib resolution on repo_id_jj
        let index_store = Arc::new(ServerIndexStore);
        let submodule_store = Arc::new(ServerSubmoduleStore);
        let user_settings = UserSettings::from_config(jj_lib::config::StackedConfig::with_defaults()).unwrap();
        let signer = Signer::from_settings(&user_settings).unwrap();
        let merge_options = MergeOptions::from_settings(&user_settings).unwrap();

        let jj_backend = Box::new(ServerBackend::new(store.clone(), repo_id_jj.to_string()));
        let jj_store = JjStore::new(jj_backend, signer, merge_options);
        let jj_op_store: Arc<dyn OpStore> = Arc::new(ServerOpStore::new(store.clone(), repo_id_jj.to_string()));
        let jj_op_heads_store: Arc<dyn OpHeadsStore> = Arc::new(ServerOpHeadsStore::new(store.clone(), repo_id_jj.to_string()));

        let repo_loader = RepoLoader::new(
            user_settings,
            jj_store,
            jj_op_store,
            jj_op_heads_store,
            index_store,
            submodule_store,
        );

        let jj_lib_repo = repo_loader.load_at_head().await.expect("jj-lib direct resolution failed");
        let jj_lib_op = jj_lib_repo.operation().clone();
        let jj_lib_view = jj_lib_repo.view().store_view().clone();

        // Server reconciliation logic on repo_id_server
        let server_merged_op_id = reconcile_repo_op_heads(store.clone(), repo_id_server)
            .await
            .expect("Server reconciliation failed");

        let server_op_proto = store
            .get_operation(repo_id_server, &server_merged_op_id)
            .await
            .unwrap()
            .expect("Server merged operation not found in store");
        let server_op = operation_from_proto(server_op_proto);

        let server_view_proto = store
            .get_view(repo_id_server, server_op.view_id.as_bytes())
            .await
            .unwrap()
            .expect("Server merged view not found in store");
        let server_view = view_from_proto(server_view_proto);

        // Verify exact parity between both resolution methods
        // Parent operation sets must match exactly between jj-lib and server resolution
        let server_parent_set: HashSet<_> = server_op.parents.iter().cloned().collect();
        let jj_lib_parent_set: HashSet<_> = jj_lib_op.parent_ids().iter().cloned().collect();
        assert_eq!(server_parent_set, jj_lib_parent_set);
        assert_eq!(
            server_parent_set,
            HashSet::from([
                OperationId::from_bytes(&op_a_bytes),
                OperationId::from_bytes(&op_b_bytes),
            ])
        );

        // Merged View IDs must match exactly
        assert_eq!(server_op.view_id, *jj_lib_op.view_id());

        // Merged View contents must match exactly
        assert_eq!(server_view.head_ids, jj_lib_view.head_ids);
        assert_eq!(server_view.local_bookmarks, jj_lib_view.local_bookmarks);
        assert_eq!(server_view.wc_commit_ids, jj_lib_view.wc_commit_ids);

        // Verify that both divergent bookmarks were successfully resolved in the merged view
        assert!(server_view.local_bookmarks.contains_key(&jj_lib::ref_name::RefNameBuf::from("branch-a")));
        assert!(server_view.local_bookmarks.contains_key(&jj_lib::ref_name::RefNameBuf::from("branch-b")));

        // Operation metadata description matches standard jj-lib reconciliation message
        assert_eq!(server_op.metadata.description, jj_lib_op.metadata().description);
        assert_eq!(server_op.metadata.description, "reconcile divergent operations");

        // The server repo op heads store now holds strictly the single merged operation head
        let final_heads = store.get_op_heads(repo_id_server).await.unwrap().unwrap();
        assert_eq!(final_heads, vec![server_merged_op_id]);
    }

    /// This test verifies that when divergent operation heads cannot be resolved due to
    /// missing or corrupted underlying store objects (in this case, an operation head is registered
    /// in op_heads but its corresponding Operation record is absent from the op store),
    /// `reconcile_repo_op_heads` attempts all `MAX_RETRIES` (3 retries with backoff) and ultimately
    /// fails with a `tonic::Status::internal` error describing the missing object.
    #[tokio::test]
    async fn test_reconcile_repo_op_heads_fails_after_exhausting_retries_on_missing_operation() {
        let store = Arc::new(MemoryStore::new());
        let repo_id = "test-reconcile-failure-repo";
        store.register_repo(repo_id.to_string(), None).await.unwrap();

        let root_op_bytes = cc_common::ROOT_OPERATION_ID_BYTES.to_vec();
        let root_view_bytes = cc_common::ROOT_VIEW_ID_BYTES.to_vec();

        // Write operation A into store
        let op_a_bytes = vec![0x0A; 20];
        let op_a_proto = make_test_proto_op(
            root_view_bytes.clone(),
            vec![root_op_bytes.clone()],
            "branch a",
        );
        store
            .put_operation(repo_id.to_string(), op_a_bytes.clone(), op_a_proto)
            .await
            .unwrap();

        // Do NOT put operation B into the store (missing operation object)
        let op_b_bytes = vec![0x0B; 20];

        // Register divergent op heads: [op_a, op_b]
        store
            .update_op_heads_append_row(
                repo_id.to_string(),
                &[root_op_bytes.clone()],
                op_a_bytes.clone(),
            )
            .await
            .unwrap();
        store
            .update_op_heads_append_row(repo_id.to_string(), &[], op_b_bytes.clone())
            .await
            .unwrap();

        let initial_heads = store.get_op_heads(repo_id).await.unwrap().unwrap();
        assert_eq!(initial_heads.len(), 2);

        // Call reconcile_repo_op_heads - should fail all 3 retries because op_b is missing
        let result = reconcile_repo_op_heads(store.clone(), repo_id).await;

        let status = result.expect_err("Reconciliation should fail when operation object is missing");
        assert_eq!(status.code(), tonic::Code::Internal);

        // Verify that op heads were NOT modified since reconciliation failed
        let post_heads = store.get_op_heads(repo_id).await.unwrap().unwrap();
        assert_eq!(post_heads, initial_heads);
    }
}
