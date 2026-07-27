pub mod backend {
    tonic::include_proto!("commit_cloud.backend");
}

pub mod op_store {
    tonic::include_proto!("commit_cloud.op_store");
}

pub mod op_heads_store {
    tonic::include_proto!("commit_cloud.op_heads_store");
}
