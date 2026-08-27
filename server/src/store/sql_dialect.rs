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
