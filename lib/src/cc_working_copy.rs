use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;

use cc_common::workspace::workspace_service_client::WorkspaceServiceClient;
use cc_common::workspace::{UpdateWorkspaceRequest, WorkspaceState};
use jj_lib::commit::Commit;
use jj_lib::local_working_copy::LocalWorkingCopyFactory;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::{WorkspaceName, WorkspaceNameBuf};
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use jj_lib::working_copy::{
    CheckoutError, CheckoutStats, LockedWorkingCopy, ResetError, SnapshotError, SnapshotOptions,
    SnapshotStats, WorkingCopy, WorkingCopyFactory, WorkingCopyStateError,
};

use crate::util::{run_async, CommitCloudConfig};

pub struct CommitCloudWorkingCopy {
    inner: Box<dyn WorkingCopy>,
    config: CommitCloudConfig,
    state_path: PathBuf,
    user: String,
}

impl CommitCloudWorkingCopy {
    pub fn new(
        inner: Box<dyn WorkingCopy>,
        config: CommitCloudConfig,
        state_path: PathBuf,
        user: String,
    ) -> Self {
        Self {
            inner,
            config,
            state_path,
            user,
        }
    }

    pub fn config(&self) -> &CommitCloudConfig {
        &self.config
    }

    pub fn user(&self) -> &str {
        &self.user
    }
}

#[async_trait(?Send)]
impl WorkingCopy for CommitCloudWorkingCopy {
    fn name(&self) -> &str {
        "commit_cloud"
    }

    fn workspace_name(&self) -> &WorkspaceName {
        self.inner.workspace_name()
    }

    fn operation_id(&self) -> &OperationId {
        self.inner.operation_id()
    }

    fn tree(&self) -> Result<&MergedTree, WorkingCopyStateError> {
        self.inner.tree()
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        self.inner.sparse_patterns()
    }

    async fn start_mutation(&self) -> Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError> {
        let locked_inner = self.inner.start_mutation().await?;
        let current_commit_id = std::fs::read(self.state_path.join("commit_id")).ok();
        Ok(Box::new(LockedCommitCloudWorkingCopy {
            locked_inner,
            config: self.config.clone(),
            state_path: self.state_path.clone(),
            user: self.user.clone(),
            current_commit_id,
        }))
    }
}

pub struct LockedCommitCloudWorkingCopy {
    locked_inner: Box<dyn LockedWorkingCopy>,
    config: CommitCloudConfig,
    state_path: PathBuf,
    user: String,
    current_commit_id: Option<Vec<u8>>,
}

#[async_trait]
impl LockedWorkingCopy for LockedCommitCloudWorkingCopy {
    fn old_operation_id(&self) -> &OperationId {
        self.locked_inner.old_operation_id()
    }

    fn old_tree(&self) -> &MergedTree {
        self.locked_inner.old_tree()
    }

    async fn snapshot(
        &mut self,
        options: &SnapshotOptions,
    ) -> Result<(MergedTree, SnapshotStats), SnapshotError> {
        self.locked_inner.snapshot(options).await
    }

    async fn check_out(&mut self, commit: &Commit) -> Result<CheckoutStats, CheckoutError> {
        let commit_id_bytes = commit.id().as_bytes().to_vec();
        let _ = std::fs::write(self.state_path.join("commit_id"), &commit_id_bytes);
        self.current_commit_id = Some(commit_id_bytes);
        self.locked_inner.check_out(commit).await
    }

    fn rename_workspace(&mut self, new_workspace_name: WorkspaceNameBuf) {
        self.locked_inner.rename_workspace(new_workspace_name);
    }

    async fn reset(&mut self, commit: &Commit) -> Result<(), ResetError> {
        let commit_id_bytes = commit.id().as_bytes().to_vec();
        let _ = std::fs::write(self.state_path.join("commit_id"), &commit_id_bytes);
        self.current_commit_id = Some(commit_id_bytes);
        self.locked_inner.reset(commit).await
    }

    async fn recover(&mut self, commit: &Commit) -> Result<(), ResetError> {
        let commit_id_bytes = commit.id().as_bytes().to_vec();
        let _ = std::fs::write(self.state_path.join("commit_id"), &commit_id_bytes);
        self.current_commit_id = Some(commit_id_bytes);
        self.locked_inner.recover(commit).await
    }

    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        self.locked_inner.sparse_patterns()
    }

    async fn set_sparse_patterns(
        &mut self,
        new_sparse_patterns: Vec<RepoPathBuf>,
    ) -> Result<CheckoutStats, CheckoutError> {
        self.locked_inner
            .set_sparse_patterns(new_sparse_patterns)
            .await
    }

    async fn finish(
        self: Box<Self>,
        new_operation_id: OperationId,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let config = self.config.clone();
        let state_path = self.state_path.clone();
        let user = self.user.clone();
        let current_commit_id = self.current_commit_id.clone();
        let finished_inner = self.locked_inner.finish(new_operation_id.clone()).await?;

        let ws_name = finished_inner.workspace_name().as_str().to_string();
        let op_id_bytes = new_operation_id.as_bytes().to_vec();
        let tree_id_bytes = finished_inner
            .tree()
            .ok()
            .and_then(|t| t.tree_ids().as_resolved().map(|id| id.as_bytes().to_vec()))
            .unwrap_or_default();

        let commit_id_bytes = current_commit_id
            .or_else(|| std::fs::read(state_path.join("commit_id")).ok())
            .unwrap_or_default();

        // Sync workspace state to jj-cc-server
        let server_url = config.server_url.clone();
        let repo_id = config.repo_id.clone();
        let user_clone = user.clone();
        let _ = run_async(move || async move {
            let mut client = WorkspaceServiceClient::connect(server_url).await?;
            let _ = client
                .update_workspace(UpdateWorkspaceRequest {
                    workspace: Some(WorkspaceState {
                        repo_id,
                        user: user_clone,
                        workspace_name: ws_name,
                        commit_id: commit_id_bytes,
                        operation_id: op_id_bytes,
                        tree_id: tree_id_bytes,
                    }),
                })
                .await?;
            Ok(())
        });

        Ok(Box::new(CommitCloudWorkingCopy::new(
            finished_inner,
            config,
            state_path,
            user,
        )))
    }
}

pub struct CommitCloudWorkingCopyFactory;

impl CommitCloudWorkingCopyFactory {
    pub fn new() -> Self {
        Self
    }
}

fn resolve_user(settings: &UserSettings) -> Result<String, WorkingCopyStateError> {
    let email = settings.user_email();
    if !email.is_empty() {
        Ok(email.to_string())
    } else {
        Err(WorkingCopyStateError {
            message: "User email (user.email) is not configured in user settings. Run `jj config set --user user.email <email>`".to_string(),
            err: "User email missing".into(),
        })
    }
}

impl WorkingCopyFactory for CommitCloudWorkingCopyFactory {
    fn init_working_copy(
        &self,
        store: Arc<Store>,
        working_copy_path: PathBuf,
        state_path: PathBuf,
        operation_id: OperationId,
        workspace_name: WorkspaceNameBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let user = resolve_user(settings)?;
        let local_factory = LocalWorkingCopyFactory {};
        let local_wc = local_factory.init_working_copy(
            store,
            working_copy_path,
            state_path.clone(),
            operation_id.clone(),
            workspace_name.clone(),
            settings,
        )?;

        let config = CommitCloudConfig::load_from_store(&state_path).map_err(|e| {
            WorkingCopyStateError {
                message: format!("Failed to load config: {}", e),
                err: e,
            }
        })?;

        let ws_name = workspace_name.as_str().to_string();
        let op_id_bytes = operation_id.as_bytes().to_vec();
        let tree_id_bytes = local_wc
            .tree()
            .ok()
            .and_then(|t| t.tree_ids().as_resolved().map(|id| id.as_bytes().to_vec()))
            .unwrap_or_default();
        let commit_id_bytes = std::fs::read(state_path.join("commit_id")).ok().unwrap_or_default();

        let server_url = config.server_url.clone();
        let repo_id = config.repo_id.clone();
        let user_clone = user.clone();
        let _ = run_async(move || async move {
            let mut client = WorkspaceServiceClient::connect(server_url).await?;
            let _ = client
                .update_workspace(UpdateWorkspaceRequest {
                    workspace: Some(WorkspaceState {
                        repo_id,
                        user: user_clone,
                        workspace_name: ws_name,
                        commit_id: commit_id_bytes,
                        operation_id: op_id_bytes,
                        tree_id: tree_id_bytes,
                    }),
                })
                .await?;
            Ok(())
        });

        Ok(Box::new(CommitCloudWorkingCopy::new(
            local_wc, config, state_path, user,
        )))
    }

    fn load_working_copy(
        &self,
        store: Arc<Store>,
        working_copy_path: PathBuf,
        state_path: PathBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let user = resolve_user(settings)?;
        let local_factory = LocalWorkingCopyFactory {};
        let local_wc = local_factory.load_working_copy(
            store,
            working_copy_path,
            state_path.clone(),
            settings,
        )?;

        let config = CommitCloudConfig::load_from_store(&state_path).map_err(|e| {
            WorkingCopyStateError {
                message: format!("Failed to load config: {}", e),
                err: e,
            }
        })?;

        Ok(Box::new(CommitCloudWorkingCopy::new(
            local_wc, config, state_path, user,
        )))
    }
}
