use super::db_store::{DatabaseStore, DbError};
use cc_proto::backend::{Commit, TreeEntry};
use cc_proto::op_store::{Operation, View};
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::key::{Key, KeySet};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;
use prost::Message;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

pub struct SpannerStore {
    db_client: Arc<DatabaseClient>,
}

impl SpannerStore {
    pub async fn connect(database_name: &str) -> Result<Self, DbError> {
        let spanner = Spanner::builder()
            .build()
            .await
            .map_err(|e| DbError::Internal(format!("Failed to build Spanner client: {e}")))?;

        // Automatically ensure schema tables exist using embedded schema_spanner.sql
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
            .map_err(|e| DbError::Internal(format!("Failed to connect to Spanner database: {e}")))?;

        Ok(Self {
            db_client: Arc::new(db_client),
        })
    }
}

#[tonic::async_trait]
impl DatabaseStore for SpannerStore {
    async fn register_repository(
        &self,
        requested_repo_id: Option<&str>,
    ) -> Result<String, DbError> {
        let repo_id = requested_repo_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let mutation = Mutation::new_insert_or_update_builder("repositories")
            .set("repo_id")
            .to(&repo_id)
            .build();

        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| DbError::Internal(format!("Spanner register_repository failed: {e}")))?;

        Ok(repo_id)
    }

    async fn read_commit(
        &self,
        repo_id: &str,
        commit_id: &[u8],
    ) -> Result<Option<Commit>, DbError> {
        let stmt = Statement::builder("SELECT data FROM commits WHERE repo_id = @repo_id AND commit_id = @commit_id")
            .add_param("repo_id", repo_id)
            .add_param("commit_id", commit_id.to_vec())
            .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| DbError::Internal(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let commit = Commit::decode(&data[..])
                .map_err(|e| DbError::InvalidData(format!("Failed to decode commit: {e}")))?;
            Ok(Some(commit))
        } else {
            Ok(None)
        }
    }

    async fn write_commit(&self, repo_id: &str, mut commit: Commit) -> Result<Vec<u8>, DbError> {
        let commit_id_bytes = if commit.commit_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            hasher.update(repo_id.as_bytes());
            hasher.update(&commit.change_id);
            hasher.update(&commit.root_tree_id);
            for parent in &commit.parent_commit_ids {
                hasher.update(parent);
            }
            hasher.update(commit.description.as_bytes());
            hasher.finalize().as_bytes()[..20].to_vec()
        } else {
            commit.commit_id.clone()
        };

        commit.commit_id = commit_id_bytes.clone();
        let mut encoded = Vec::new();
        commit
            .encode(&mut encoded)
            .map_err(|e| DbError::Internal(format!("Failed to encode commit: {e}")))?;

        let mutation = Mutation::new_insert_or_update_builder("commits")
            .set("repo_id")
            .to(&repo_id)
            .set("commit_id")
            .to(&commit_id_bytes)
            .set("data")
            .to(&encoded)
            .build();

        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| DbError::Internal(format!("Spanner write_commit failed: {e}")))?;

        Ok(commit_id_bytes)
    }

    async fn read_tree(
        &self,
        repo_id: &str,
        tree_id: &[u8],
    ) -> Result<Option<Vec<TreeEntry>>, DbError> {
        let stmt = Statement::builder("SELECT data FROM trees WHERE repo_id = @repo_id AND tree_id = @tree_id")
            .add_param("repo_id", repo_id)
            .add_param("tree_id", tree_id.to_vec())
            .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| DbError::Internal(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let entries = cc_proto::backend::ReadTreeResponse::decode(&data[..])
                .map_err(|e| DbError::InvalidData(format!("Failed to decode tree: {e}")))?
                .entries;
            Ok(Some(entries))
        } else {
            Ok(None)
        }
    }

    async fn write_tree(
        &self,
        repo_id: &str,
        entries: Vec<TreeEntry>,
    ) -> Result<Vec<u8>, DbError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(repo_id.as_bytes());
        for entry in &entries {
            hasher.update(entry.name.as_bytes());
            hasher.update(&entry.entry_id);
            hasher.update(&(entry.entry_type as i32).to_le_bytes());
        }
        let tree_id_bytes = hasher.finalize().as_bytes()[..20].to_vec();

        let wrapper = cc_proto::backend::ReadTreeResponse {
            tree_id: tree_id_bytes.clone(),
            entries,
        };
        let mut encoded = Vec::new();
        wrapper
            .encode(&mut encoded)
            .map_err(|e| DbError::Internal(format!("Failed to encode tree: {e}")))?;

        let mutation = Mutation::new_insert_or_update_builder("trees")
            .set("repo_id")
            .to(&repo_id)
            .set("tree_id")
            .to(&tree_id_bytes)
            .set("data")
            .to(&encoded)
            .build();

        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| DbError::Internal(format!("Spanner write_tree failed: {e}")))?;

        Ok(tree_id_bytes)
    }

    async fn read_file(
        &self,
        repo_id: &str,
        file_id: &[u8],
    ) -> Result<Option<Vec<u8>>, DbError> {
        let stmt = Statement::builder("SELECT data FROM files WHERE repo_id = @repo_id AND file_id = @file_id")
            .add_param("repo_id", repo_id)
            .add_param("file_id", file_id.to_vec())
            .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| DbError::Internal(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    async fn write_file(&self, repo_id: &str, content: &[u8]) -> Result<Vec<u8>, DbError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(content);
        let file_id_bytes = hasher.finalize().as_bytes()[..20].to_vec();

        let mutation = Mutation::new_insert_or_update_builder("files")
            .set("repo_id")
            .to(&repo_id)
            .set("file_id")
            .to(&file_id_bytes)
            .set("data")
            .to(&content.to_vec())
            .build();

        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| DbError::Internal(format!("Spanner write_file failed: {e}")))?;

        Ok(file_id_bytes)
    }

    async fn read_symlink(
        &self,
        _repo_id: &str,
        _symlink_id: &[u8],
    ) -> Result<Option<String>, DbError> {
        Ok(None)
    }

    async fn write_symlink(&self, _repo_id: &str, _target: &str) -> Result<Vec<u8>, DbError> {
        Err(DbError::Internal("Symlinks not implemented".into()))
    }

    async fn read_operation(
        &self,
        repo_id: &str,
        op_id: &[u8],
    ) -> Result<Option<Operation>, DbError> {
        let stmt = Statement::builder("SELECT data FROM operations WHERE repo_id = @repo_id AND op_id = @op_id")
            .add_param("repo_id", repo_id)
            .add_param("op_id", op_id.to_vec())
            .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| DbError::Internal(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let op = Operation::decode(&data[..])
                .map_err(|e| DbError::InvalidData(format!("Failed to decode operation: {e}")))?;
            Ok(Some(op))
        } else {
            Ok(None)
        }
    }

    async fn write_operation(&self, repo_id: &str, mut op: Operation) -> Result<Vec<u8>, DbError> {
        let op_id_bytes = if op.operation_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            hasher.update(repo_id.as_bytes());
            hasher.update(&op.view_id);
            for parent in &op.parent_op_ids {
                hasher.update(parent);
            }
            if let Some(meta) = &op.metadata {
                hasher.update(meta.description.as_bytes());
            }
            hasher.finalize().as_bytes()[..20].to_vec()
        } else {

            op.operation_id.clone()
        };

        op.operation_id = op_id_bytes.clone();
        let mut encoded = Vec::new();
        op.encode(&mut encoded)
            .map_err(|e| DbError::Internal(format!("Failed to encode operation: {e}")))?;

        let mutation = Mutation::new_insert_or_update_builder("operations")
            .set("repo_id")
            .to(&repo_id)
            .set("op_id")
            .to(&op_id_bytes)
            .set("data")
            .to(&encoded)
            .build();

        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| DbError::Internal(format!("Spanner write_operation failed: {e}")))?;

        Ok(op_id_bytes)
    }

    async fn read_view(&self, repo_id: &str, view_id: &[u8]) -> Result<Option<View>, DbError> {
        let stmt = Statement::builder("SELECT data FROM views WHERE repo_id = @repo_id AND view_id = @view_id")
            .add_param("repo_id", repo_id)
            .add_param("view_id", view_id.to_vec())
            .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;

        if let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| DbError::Internal(e.to_string()))?;
            let data: Vec<u8> = row.get("data");
            let view = View::decode(&data[..])
                .map_err(|e| DbError::InvalidData(format!("Failed to decode view: {e}")))?;
            Ok(Some(view))
        } else {
            Ok(None)
        }
    }

    async fn write_view(&self, repo_id: &str, mut view: View) -> Result<Vec<u8>, DbError> {
        let view_id_bytes = if view.view_id.is_empty() {
            let mut hasher = blake3::Hasher::new();
            hasher.update(repo_id.as_bytes());
            for head in &view.head_commit_ids {
                hasher.update(head);
            }
            hasher.finalize().as_bytes()[..20].to_vec()
        } else {
            view.view_id.clone()
        };

        view.view_id = view_id_bytes.clone();
        let mut encoded = Vec::new();
        view.encode(&mut encoded)
            .map_err(|e| DbError::Internal(format!("Failed to encode view: {e}")))?;

        let mutation = Mutation::new_insert_or_update_builder("views")
            .set("repo_id")
            .to(&repo_id)
            .set("view_id")
            .to(&view_id_bytes)
            .set("data")
            .to(&encoded)
            .build();

        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| DbError::Internal(format!("Spanner write_view failed: {e}")))?;

        Ok(view_id_bytes)
    }

    async fn get_op_heads(&self, repo_id: &str) -> Result<Vec<Vec<u8>>, DbError> {
        let stmt = Statement::builder("SELECT op_head_id FROM op_heads WHERE repo_id = @repo_id")
            .add_param("repo_id", repo_id)
            .build();

        let tx = self.db_client.single_use().build();
        let mut rs = tx
            .execute_query(stmt)
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?;

        let mut result = Vec::new();
        while let Some(row_res) = rs.next().await {
            let row = row_res.map_err(|e| DbError::Internal(e.to_string()))?;
            let op_head_id: Vec<u8> = row.get("op_head_id");
            result.push(op_head_id);
        }

        Ok(result)
    }

    async fn add_op_head(&self, repo_id: &str, op_id: &[u8]) -> Result<Vec<Vec<u8>>, DbError> {
        let mutation = Mutation::new_insert_or_update_builder("op_heads")
            .set("repo_id")
            .to(&repo_id)
            .set("op_head_id")
            .to(&op_id.to_vec())
            .build();

        self.db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await
            .map_err(|e| DbError::Internal(format!("Spanner add_op_head failed: {e}")))?;

        self.get_op_heads(repo_id).await
    }

    async fn remove_op_head(&self, repo_id: &str, op_id: &[u8]) -> Result<Vec<Vec<u8>>, DbError> {
        let key = Key::new(vec![repo_id.to_string().into(), op_id.to_vec().into()]);
        let key_set = KeySet::builder().add_key(key).build();
        let mutation = Mutation::delete("op_heads", key_set);

        let _ = self
            .db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await;

        self.get_op_heads(repo_id).await
    }
}
