use crate::store::{Store, StoreError};

// Map a storage failure onto a gRPC status so it is surfaced to the client
// instead of being silently swallowed. Read/write/task failures are server-side
// a decode failure means the stored bytes are corrupt (`data_loss`).
impl From<StoreError> for tonic::Status {
    fn from(err: StoreError) -> Self {
        let msg = err.to_string();
        match err {
            StoreError::Decode(_) => tonic::Status::data_loss(msg),
            StoreError::Read(_)
            | StoreError::Write(_)
            | StoreError::Encode(_)
            | StoreError::Task(_) => tonic::Status::internal(msg),
        }
    }
}

pub async fn ensure_repo_registered_error(
    store: &dyn Store,
    repo_id: &str,
    action: &str,
) -> Result<(), tonic::Status> {
    if !store.is_repo_registered(repo_id).await? {
        return Err(tonic::Status::not_found(format!(
            "repository should have been registered before {action}: {repo_id}"
        )));
    }
    Ok(())
}
