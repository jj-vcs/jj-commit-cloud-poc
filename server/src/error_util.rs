use crate::store::Store;

pub async fn ensure_repo_registered_error(
    store: &dyn Store,
    repo_id: &str,
    action: &str,
) -> Result<(), tonic::Status> {
    if !store.is_repo_registered(repo_id).await {
        return Err(tonic::Status::not_found(format!(
            "repository should have been registered before {action}: {repo_id}"
        )));
    }
    Ok(())
}
