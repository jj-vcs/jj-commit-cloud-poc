use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::key::{Key, KeySet};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;
use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{info, warn};

use super::{CommitId, Store, StoreError, StoreResult};

#[derive(Clone)]
pub struct SpannerStore {
    db_client: Arc<DatabaseClient>,
}

#[derive(prost::Message)]
struct TreeEntryList {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<TreeEntry>,
}

impl SpannerStore {
    pub async fn connect(database_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let spanner = Spanner::builder()
            .build()
            .await
            .map_err(|e| format!("Failed to build Spanner client: {e}"))?;

        if let Ok(admin_client) = spanner.database_admin_builder().build().await {
            let schema_str = include_str!("../../db/schema_spanner.sql");
            let ddl_statements: Vec<String> = schema_str
                .split(';')
                .map(|s| {
                    s.lines()
                        .filter(|line| !line.trim().starts_with("--"))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if !ddl_statements.is_empty() {
                info!(
                    "Ensuring Spanner database schema tables exist for '{}'...",
                    database_name
                );
                let res = admin_client
                    .update_database_ddl()
                    .set_database(database_name)
                    .set_statements(ddl_statements)
                    .send()
                    .await;
                if let Err(e) = res {
                    warn!("Spanner auto-schema initialization notice: {e}");
                }
            }
        }

        let db_client = spanner
            .database_client(database_name)
            .build()
            .await
            .map_err(|e| format!("Failed to connect to Spanner database: {e}"))?;

        Ok(Self {
            db_client: Arc::new(db_client),
        })
    }
}

#[async_trait]
impl Store for SpannerStore {
    async fn is_project_registered(&self, project_id: &str) -> StoreResult<bool> {
        let stmt = Statement::builder("SELECT 1 FROM projects WHERE project_id = @project_id")
            .add_param("project_id", project_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        Ok(rs.next().await.is_some())
    }

    async fn register_project(&self, project_id: String, name: Option<String>) -> StoreResult<()> {
        let mut builder = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id);
        if let Some(n) = name {
            builder = builder.set("name").to(&n);
        }
        let mutation = builder.build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn is_repo_registered(&self, repo_id: &str) -> StoreResult<bool> {
        let stmt = Statement::builder("SELECT 1 FROM repos WHERE repo_id = @repo_id")
            .add_param("repo_id", repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        Ok(rs.next().await.is_some())
    }

    async fn register_repo(
        &self,
        repo_id: String,
        project_id: String,
        name: Option<String>,
    ) -> StoreResult<()> {
        let proj_mutation = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id)
            .build();

        let mut repo_builder = Mutation::new_insert_or_update_builder("repos")
            .set("repo_id")
            .to(&repo_id)
            .set("project_id")
            .to(&project_id);
        if let Some(n) = name {
            repo_builder = repo_builder.set("name").to(&n);
        }
        let repo_mutation = repo_builder.build();

        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![proj_mutation, repo_mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_repo_project_id(&self, repo_id: &str) -> StoreResult<Option<String>> {
        let stmt = Statement::builder("SELECT project_id FROM repos WHERE repo_id = @repo_id")
            .add_param("repo_id", repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let project_id: String = row.get("project_id");
            Ok(Some(project_id))
        } else {
            Ok(None)
        }
    }

    async fn get_commit(&self, project_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        let stmt = Statement::builder(
            "SELECT data FROM commits WHERE project_id = @project_id AND commit_id = @commit_id",
        )
        .add_param("project_id", project_id)
        .add_param("commit_id", commit_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let commit =
                Commit::decode(&data[..]).map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(commit))
        } else {
            Ok(None)
        }
    }

    async fn list_project_commit_ids(&self, project_id: &str) -> StoreResult<Vec<CommitId>> {
        let stmt =
            Statement::builder("SELECT commit_id FROM commits WHERE project_id = @project_id")
                .add_param("project_id", project_id)
                .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let commit_id: Vec<u8> = row.get("commit_id");
            result.push(commit_id);
        }
        Ok(result)
    }

    async fn put_commit(
        &self,
        project_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()> {
        let mut encoded = Vec::new();
        commit
            .encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let proj_mutation = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id)
            .build();
        let commit_mutation = Mutation::new_insert_or_update_builder("commits")
            .set("project_id")
            .to(&project_id)
            .set("commit_id")
            .to(&commit_id)
            .set("data")
            .to(&encoded)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![proj_mutation, commit_mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_tree(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let stmt = Statement::builder(
            "SELECT data FROM trees WHERE project_id = @project_id AND tree_id = @tree_id",
        )
        .add_param("project_id", project_id)
        .add_param("tree_id", tree_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let list =
                TreeEntryList::decode(&data[..]).map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(list.entries))
        } else {
            Ok(None)
        }
    }

    async fn put_tree(
        &self,
        project_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()> {
        let list = TreeEntryList { entries };
        let mut encoded = Vec::new();
        list.encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let proj_mutation = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id)
            .build();
        let tree_mutation = Mutation::new_insert_or_update_builder("trees")
            .set("project_id")
            .to(&project_id)
            .set("tree_id")
            .to(&tree_id)
            .set("data")
            .to(&encoded)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![proj_mutation, tree_mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_file(&self, project_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        let stmt = Statement::builder(
            "SELECT data FROM files WHERE project_id = @project_id AND file_id = @file_id",
        )
        .add_param("project_id", project_id)
        .add_param("file_id", file_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    async fn put_file(
        &self,
        project_id: String,
        file_id: Vec<u8>,
        content: Vec<u8>,
    ) -> StoreResult<()> {
        let proj_mutation = Mutation::new_insert_or_update_builder("projects")
            .set("project_id")
            .to(&project_id)
            .build();
        let file_mutation = Mutation::new_insert_or_update_builder("files")
            .set("project_id")
            .to(&project_id)
            .set("file_id")
            .to(&file_id)
            .set("data")
            .to(&content)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![proj_mutation, file_mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        let stmt = Statement::builder(
            "SELECT data FROM operations WHERE repo_id = @repo_id AND op_id = @op_id",
        )
        .add_param("repo_id", repo_id)
        .add_param("op_id", op_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let op =
                Operation::decode(&data[..]).map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(op))
        } else {
            Ok(None)
        }
    }

    async fn put_operation(
        &self,
        repo_id: String,
        op_id: Vec<u8>,
        op: Operation,
    ) -> StoreResult<()> {
        let mut encoded = Vec::new();
        op.encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let mutation = Mutation::new_insert_or_update_builder("operations")
            .set("repo_id")
            .to(&repo_id)
            .set("op_id")
            .to(&op_id)
            .set("data")
            .to(&encoded)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_view(&self, repo_id: &str, view_id: &[u8]) -> StoreResult<Option<View>> {
        let stmt = Statement::builder(
            "SELECT data FROM views WHERE repo_id = @repo_id AND view_id = @view_id",
        )
        .add_param("repo_id", repo_id)
        .add_param("view_id", view_id.to_vec())
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let view = View::decode(&data[..]).map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(view))
        } else {
            Ok(None)
        }
    }

    async fn put_view(&self, repo_id: String, view_id: Vec<u8>, view: View) -> StoreResult<()> {
        let mut encoded = Vec::new();
        view.encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let mutation = Mutation::new_insert_or_update_builder("views")
            .set("repo_id")
            .to(&repo_id)
            .set("view_id")
            .to(&view_id)
            .set("data")
            .to(&encoded)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_op_heads(&self, repo_id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let stmt = Statement::builder("SELECT op_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let op_id: Vec<u8> = row.get("op_id");
            result.push(op_id);
        }
        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    async fn update_op_heads_append_new_remove_old(
        &self,
        repo_id: String,
        ids_to_remove: &[Vec<u8>],
        id_to_add: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let stmt = Statement::builder("SELECT op_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", &repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut current_heads = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let op_id: Vec<u8> = row.get("op_id");
            current_heads.push(op_id);
        }
        if current_heads.is_empty() {
            current_heads.push(cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
        }

        if !ids_to_remove.is_empty() {
            for old in ids_to_remove {
                if !current_heads.contains(old) {
                    return Err(StoreError::CasConflict(format!(
                        "write race detected on stale old_op_head_ids: head {} no longer active in repo {}",
                        hex::encode(old),
                        repo_id
                    )));
                }
            }
        }

        let mut mutations = Vec::new();
        for old in ids_to_remove {
            let key = Key::new(vec![repo_id.clone().into(), old.clone().into()]);
            let key_set = KeySet::builder().add_key(key).build();
            mutations.push(Mutation::delete("op_heads", key_set));
        }
        mutations.push(
            Mutation::new_insert_or_update_builder("op_heads")
                .set("repo_id")
                .to(&repo_id)
                .set("op_id")
                .to(&id_to_add)
                .build(),
        );

        self.db_client
            .write_only_transaction()
            .build()
            .write(mutations)
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;

        let stmt = Statement::builder("SELECT op_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", &repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let op_id: Vec<u8> = row.get("op_id");
            result.push(op_id);
        }
        Ok(result)
    }

    async fn update_op_heads_compare_and_swap(
        &self,
        repo_id: String,
        expected_exact_heads: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let stmt = Statement::builder("SELECT op_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", &repo_id)
            .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut current_heads = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let op_id: Vec<u8> = row.get("op_id");
            current_heads.push(op_id);
        }
        if current_heads.is_empty() {
            current_heads.push(cc_common::ROOT_OPERATION_ID_BYTES.to_vec());
        }

        let current_set: HashSet<Vec<u8>> = current_heads.into_iter().collect();
        let expected_set: HashSet<Vec<u8>> = expected_exact_heads.iter().cloned().collect();

        if current_set != expected_set {
            return Err(StoreError::CasConflict(format!(
                "reconciliation CAS conflict for repo {repo_id}: expected heads {expected_set:?}, found {current_set:?}"
            )));
        }

        let mut mutations = Vec::new();
        for old in expected_exact_heads {
            let key = Key::new(vec![repo_id.clone().into(), old.clone().into()]);
            let key_set = KeySet::builder().add_key(key).build();
            mutations.push(Mutation::delete("op_heads", key_set));
        }
        mutations.push(
            Mutation::new_insert_or_update_builder("op_heads")
                .set("repo_id")
                .to(&repo_id)
                .set("op_id")
                .to(&new_id)
                .build(),
        );

        self.db_client
            .write_only_transaction()
            .build()
            .write(mutations)
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;

        Ok(vec![new_id])
    }

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let stmt = Statement::builder(
            "SELECT commit_id, operation_id, tree_id FROM workspaces WHERE repo_id = @repo_id AND user = @user AND workspace_name = @workspace_name",
        )
        .add_param("repo_id", repo_id)
        .add_param("user", user)
        .add_param("workspace_name", workspace_name)
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let commit_id: Vec<u8> = row.get("commit_id");
            let operation_id: Vec<u8> = row.get("operation_id");
            let tree_id: Vec<u8> = row.get("tree_id");
            Ok(Some(WorkspaceState {
                repo_id: repo_id.to_string(),
                user: user.to_string(),
                workspace_name: workspace_name.to_string(),
                commit_id,
                operation_id,
                tree_id,
            }))
        } else {
            Ok(None)
        }
    }

    async fn put_workspace(&self, workspace: WorkspaceState) -> StoreResult<()> {
        let mutation = Mutation::new_insert_or_update_builder("workspaces")
            .set("repo_id")
            .to(&workspace.repo_id)
            .set("user")
            .to(&workspace.user)
            .set("workspace_name")
            .to(&workspace.workspace_name)
            .set("commit_id")
            .to(&workspace.commit_id)
            .set("operation_id")
            .to(&workspace.operation_id)
            .set("tree_id")
            .to(&workspace.tree_id)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn delete_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<bool> {
        let existing = self.get_workspace(repo_id, user, workspace_name).await?;
        if existing.is_none() {
            return Ok(false);
        }
        let key = Key::new(vec![
            repo_id.to_string().into(),
            user.to_string().into(),
            workspace_name.to_string().into(),
        ]);
        let key_set = KeySet::builder().add_key(key).build();
        let mutation = Mutation::delete("workspaces", key_set);
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(true)
    }

    async fn list_workspaces(&self, repo_id: &str) -> StoreResult<Vec<WorkspaceState>> {
        let stmt = Statement::builder(
            "SELECT user, workspace_name, commit_id, operation_id, tree_id FROM workspaces WHERE repo_id = @repo_id",
        )
        .add_param("repo_id", repo_id)
        .build();
        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| StoreError::Read(e.to_string()))?;
        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| StoreError::Read(e.to_string()))?;
            let user: String = row.get("user");
            let workspace_name: String = row.get("workspace_name");
            let commit_id: Vec<u8> = row.get("commit_id");
            let operation_id: Vec<u8> = row.get("operation_id");
            let tree_id: Vec<u8> = row.get("tree_id");
            result.push(WorkspaceState {
                repo_id: repo_id.to_string(),
                user,
                workspace_name,
                commit_id,
                operation_id,
                tree_id,
            });
        }
        Ok(result)
    }
}
