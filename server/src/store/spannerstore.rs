use async_trait::async_trait;
use cc_common::backend::*;
use cc_common::op_store::*;
use cc_common::workspace::WorkspaceState;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::key::{Key, KeySet};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;
use prost::Message;
use std::sync::Arc;
use tracing::{info, warn};

use super::{Store, StoreError, StoreResult};

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
                .map(|s| s.trim())
                .filter(|s| !s.is_empty() && !s.starts_with("--"))
                .map(|s| s.to_string())
                .collect();

            if !ddl_statements.is_empty() {
                info!("Ensuring Spanner database schema tables exist for '{}'...", database_name);
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

    async fn register_repo(&self, repo_id: String, name: Option<String>) -> StoreResult<()> {
        let mut builder = Mutation::new_insert_or_update_builder("repos")
            .set("repo_id")
            .to(&repo_id);
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

    async fn get_commit(&self, repo_id: &str, commit_id: &[u8]) -> StoreResult<Option<Commit>> {
        let stmt = Statement::builder("SELECT data FROM commits WHERE repo_id = @repo_id AND commit_id = @commit_id")
            .add_param("repo_id", repo_id)
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
            let commit = Commit::decode(&data[..])
                .map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(commit))
        } else {
            Ok(None)
        }
    }

    async fn put_commit(
        &self,
        repo_id: String,
        commit_id: Vec<u8>,
        commit: Commit,
    ) -> StoreResult<()> {
        let mut encoded = Vec::new();
        commit
            .encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let mutation = Mutation::new_insert_or_update_builder("commits")
            .set("repo_id")
            .to(&repo_id)
            .set("commit_id")
            .to(&commit_id)
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

    async fn get_tree(&self, repo_id: &str, tree_id: &[u8]) -> StoreResult<Option<Vec<TreeEntry>>> {
        let stmt = Statement::builder("SELECT data FROM trees WHERE repo_id = @repo_id AND tree_id = @tree_id")
            .add_param("repo_id", repo_id)
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
            let list = TreeEntryList::decode(&data[..])
                .map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(list.entries))
        } else {
            Ok(None)
        }
    }

    async fn put_tree(
        &self,
        repo_id: String,
        tree_id: Vec<u8>,
        entries: Vec<TreeEntry>,
    ) -> StoreResult<()> {
        let list = TreeEntryList { entries };
        let mut encoded = Vec::new();
        list.encode(&mut encoded)
            .map_err(|e| StoreError::Encode(e.to_string()))?;
        let mutation = Mutation::new_insert_or_update_builder("trees")
            .set("repo_id")
            .to(&repo_id)
            .set("tree_id")
            .to(&tree_id)
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

    async fn get_file(&self, repo_id: &str, file_id: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        let stmt = Statement::builder("SELECT data FROM files WHERE repo_id = @repo_id AND file_id = @file_id")
            .add_param("repo_id", repo_id)
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

    async fn put_file(&self, repo_id: String, file_id: Vec<u8>, content: Vec<u8>) -> StoreResult<()> {
        let mutation = Mutation::new_insert_or_update_builder("files")
            .set("repo_id")
            .to(&repo_id)
            .set("file_id")
            .to(&file_id)
            .set("data")
            .to(&content)
            .build();
        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| StoreError::Write(e.to_string()))?;
        Ok(())
    }

    async fn get_operation(&self, repo_id: &str, op_id: &[u8]) -> StoreResult<Option<Operation>> {
        let stmt = Statement::builder("SELECT data FROM operations WHERE repo_id = @repo_id AND op_id = @op_id")
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
            let op = Operation::decode(&data[..])
                .map_err(|e| StoreError::Decode(e.to_string()))?;
            Ok(Some(op))
        } else {
            Ok(None)
        }
    }

    async fn put_operation(&self, repo_id: String, op_id: Vec<u8>, op: Operation) -> StoreResult<()> {
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
        let stmt = Statement::builder("SELECT data FROM views WHERE repo_id = @repo_id AND view_id = @view_id")
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
            let view = View::decode(&data[..])
                .map_err(|e| StoreError::Decode(e.to_string()))?;
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

    async fn update_op_heads(
        &self,
        repo_id: String,
        old_ids: &[Vec<u8>],
        new_id: Vec<u8>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let mut mutations = Vec::new();
        for old in old_ids {
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

    async fn get_workspace(
        &self,
        repo_id: &str,
        user: &str,
        workspace_name: &str,
    ) -> StoreResult<Option<WorkspaceState>> {
        let stmt = Statement::builder("SELECT commit_id, operation_id, tree_id FROM workspaces WHERE repo_id = @repo_id AND user = @user AND workspace_name = @workspace_name")
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
        let stmt = Statement::builder("SELECT user, workspace_name, commit_id, operation_id, tree_id FROM workspaces WHERE repo_id = @repo_id")
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
