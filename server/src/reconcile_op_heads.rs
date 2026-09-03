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
