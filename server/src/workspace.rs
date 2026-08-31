use cc_common::workspace::workspace_service_server::WorkspaceService;
use cc_common::workspace::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::store::Store;

#[derive(Clone)]
pub struct CommitCloudWorkspaceService {
    store: Arc<dyn Store>,
}

impl CommitCloudWorkspaceService {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl WorkspaceService for CommitCloudWorkspaceService {
    async fn get_workspace(
        &self,
        request: Request<GetWorkspaceRequest>,
    ) -> Result<Response<GetWorkspaceResponse>, Status> {
        let req = request.into_inner();
        info!(
            "Get workspace for repo_id '{}', user '{}', workspace '{}'",
            req.repo_id, req.user, req.workspace_name
        );
        let ws = self
            .store
            .get_workspace(&req.repo_id, &req.user, &req.workspace_name)
            .await?;
        Ok(Response::new(GetWorkspaceResponse { workspace: ws }))
    }

    async fn update_workspace(
        &self,
        request: Request<UpdateWorkspaceRequest>,
    ) -> Result<Response<UpdateWorkspaceResponse>, Status> {
        let req = request.into_inner();
        let ws = req
            .workspace
            .ok_or_else(|| Status::invalid_argument("Workspace object is required"))?;
        info!(
            "Update workspace for repo_id '{}', user '{}', workspace '{}'",
            ws.repo_id, ws.user, ws.workspace_name
        );
        self.store.put_workspace(ws).await?;
        Ok(Response::new(UpdateWorkspaceResponse { success: true }))
    }

    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        let req = request.into_inner();
        info!("List workspaces for repo_id '{}'", req.repo_id);
        let workspaces = self.store.list_workspaces(&req.repo_id).await?;
        Ok(Response::new(ListWorkspacesResponse { workspaces }))
    }

    async fn check_working_copy_changes(
        &self,
        request: Request<CheckWorkingCopyChangesRequest>,
    ) -> Result<Response<CheckWorkingCopyChangesResponse>, Status> {
        let req = request.into_inner();
        info!(
            "Check working copy changes for repo_id '{}', user '{}', workspace '{}'",
            req.repo_id, req.user, req.workspace_name
        );

        let ws = self
            .store
            .get_workspace(&req.repo_id, &req.user, &req.workspace_name)
            .await?
            .ok_or_else(|| Status::not_found("Workspace not found"))?;

        let commit = self
            .store
            .get_commit(&req.repo_id, &ws.commit_id)
            .await?
            .ok_or_else(|| Status::not_found("Commit not found"))?;

        let commit_tree_id = commit.root_tree_id.first().cloned().unwrap_or_default();
        let has_changes = ws.tree_id != commit_tree_id;

        Ok(Response::new(CheckWorkingCopyChangesResponse {
            has_changes,
            current_tree_id: ws.tree_id,
            commit_tree_id,
        }))
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        let req = request.into_inner();
        info!(
            "Delete workspace for repo_id '{}', user '{}', workspace '{}'",
            req.repo_id, req.user, req.workspace_name
        );
        let success = self
            .store
            .delete_workspace(&req.repo_id, &req.user, &req.workspace_name)
            .await?;
        Ok(Response::new(DeleteWorkspaceResponse { success }))
    }
}
