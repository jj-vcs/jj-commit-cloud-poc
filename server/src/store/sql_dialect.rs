pub trait SqlDialect: Send + Sync + 'static {
    fn is_repo_registered_query(&self) -> &'static str;
    fn register_repo_query(&self) -> &'static str;

    fn get_commit_query(&self) -> &'static str;
    fn put_commit_query(&self) -> &'static str;

    fn get_tree_query(&self) -> &'static str;
    fn put_tree_query(&self) -> &'static str;

    fn get_file_query(&self) -> &'static str;
    fn put_file_query(&self) -> &'static str;

    fn get_operation_query(&self) -> &'static str;
    fn put_operation_query(&self) -> &'static str;

    fn get_view_query(&self) -> &'static str;
    fn put_view_query(&self) -> &'static str;

    fn get_op_heads_query(&self) -> &'static str;
    fn delete_op_head_query(&self) -> &'static str;
    fn insert_op_head_query(&self) -> &'static str;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn is_repo_registered_query(&self) -> &'static str {
        "SELECT 1 FROM repos WHERE repo_id = ?1"
    }

    fn register_repo_query(&self) -> &'static str {
        "INSERT OR IGNORE INTO repos (repo_id, name) VALUES (?1, ?2)"
    }

    fn get_commit_query(&self) -> &'static str {
        "SELECT data FROM commits WHERE repo_id = ?1 AND commit_id = ?2"
    }

    fn put_commit_query(&self) -> &'static str {
        "INSERT OR REPLACE INTO commits (repo_id, commit_id, data) VALUES (?1, ?2, ?3)"
    }

    fn get_tree_query(&self) -> &'static str {
        "SELECT data FROM trees WHERE repo_id = ?1 AND tree_id = ?2"
    }

    fn put_tree_query(&self) -> &'static str {
        "INSERT OR REPLACE INTO trees (repo_id, tree_id, data) VALUES (?1, ?2, ?3)"
    }

    fn get_file_query(&self) -> &'static str {
        "SELECT content FROM files WHERE repo_id = ?1 AND file_id = ?2"
    }

    fn put_file_query(&self) -> &'static str {
        "INSERT OR REPLACE INTO files (repo_id, file_id, content) VALUES (?1, ?2, ?3)"
    }

    fn get_operation_query(&self) -> &'static str {
        "SELECT data FROM operations WHERE repo_id = ?1 AND op_id = ?2"
    }

    fn put_operation_query(&self) -> &'static str {
        "INSERT OR REPLACE INTO operations (repo_id, op_id, data) VALUES (?1, ?2, ?3)"
    }

    fn get_view_query(&self) -> &'static str {
        "SELECT data FROM views WHERE repo_id = ?1 AND view_id = ?2"
    }

    fn put_view_query(&self) -> &'static str {
        "INSERT OR REPLACE INTO views (repo_id, view_id, data) VALUES (?1, ?2, ?3)"
    }

    fn get_op_heads_query(&self) -> &'static str {
        "SELECT op_id FROM op_heads WHERE repo_id = ?1"
    }

    fn delete_op_head_query(&self) -> &'static str {
        "DELETE FROM op_heads WHERE repo_id = ?1 AND op_id = ?2"
    }

    fn insert_op_head_query(&self) -> &'static str {
        "INSERT OR REPLACE INTO op_heads (repo_id, op_id) VALUES (?1, ?2)"
    }
}
