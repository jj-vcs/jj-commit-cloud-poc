use crate::cc_backend::CommitCloudBackend;
use crate::cc_op_heads_store::CommitCloudOpHeadsStore;
use crate::cc_op_store::CommitCloudOpStore;
use jj_lib::backend::{Backend, BackendLoadError};
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_store::OpStore;
use jj_lib::repo::StoreFactories;

pub trait StoreFactoriesExt {
    fn add_commit_cloud(&mut self);
}

impl StoreFactoriesExt for StoreFactories {
    fn add_commit_cloud(&mut self) {
        self.add_backend(
            "commit_cloud",
            Box::new(|_settings, store_path| {
                let backend = CommitCloudBackend::load(store_path).map_err(|e| BackendLoadError(e.into()))?;
                Ok(Box::new(backend) as Box<dyn Backend>)
            }),
        );

        self.add_op_store(
            "commit_cloud",
            Box::new(|_settings, store_path, _root_op_data| {
                let op_store =
                    CommitCloudOpStore::load(store_path).map_err(|e| BackendLoadError(e.into()))?;
                Ok(Box::new(op_store) as Box<dyn OpStore>)
            }),
        );

        self.add_op_heads_store(
            "commit_cloud",
            Box::new(|_settings, store_path| {
                let op_heads_store = CommitCloudOpHeadsStore::load(store_path)
                    .map_err(|e| BackendLoadError(e.into()))?;
                Ok(Box::new(op_heads_store) as Box<dyn OpHeadsStore>)
            }),
        );
    }
}
