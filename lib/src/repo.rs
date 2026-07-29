use crate::cc_backend::CommitCloudBackend;
use jj_lib::backend::{Backend, BackendLoadError};
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
    }
}
