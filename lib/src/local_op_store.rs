// Copyright 2024-2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::fmt::Debug;
use std::path::Path;
use std::time::SystemTime;
use async_trait::async_trait;
use jj_lib::backend::BackendInitError;
use jj_lib::object_id::{HexPrefix, PrefixResolution};
use jj_lib::op_store::{
    OpStore, OpStoreResult, Operation, OperationId, RootOperationData, View, ViewId,
};
use jj_lib::op_heads_store::{OpHeadsStore, OpHeadsStoreError, OpHeadsStoreLock};

#[derive(Debug)]
pub struct SqliteOpStore {}

impl SqliteOpStore {
    pub fn init(_store_path: &Path, _root_data: RootOperationData) -> Result<Self, BackendInitError> {
        todo!("SqliteOpStore::init not implemented")
    }
    pub fn load(_store_path: &Path, _root_data: RootOperationData) -> Result<Self, jj_lib::backend::BackendLoadError> {
        todo!("SqliteOpStore::load not implemented")
    }
}

#[async_trait]
impl OpStore for SqliteOpStore {
    fn name(&self) -> &str { "sqlite" }
    fn root_operation_id(&self) -> &OperationId { todo!() }
    async fn read_view(&self, _id: &ViewId) -> OpStoreResult<View> { todo!() }
    async fn write_view(&self, _view: &View) -> OpStoreResult<ViewId> { todo!() }
    async fn read_operation(&self, _id: &OperationId) -> OpStoreResult<Operation> { todo!() }
    async fn write_operation(&self, _operation: &Operation) -> OpStoreResult<OperationId> { todo!() }
    async fn resolve_operation_id_prefix(&self, _prefix: &HexPrefix) -> OpStoreResult<PrefixResolution<OperationId>> { todo!() }
    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> { Ok(()) }
}

#[derive(Debug)]
pub struct SqliteOpHeadsStore {}

impl SqliteOpHeadsStore {
    pub fn init(_store_path: &Path, _root_op_id: &OperationId) -> Result<Self, BackendInitError> {
        todo!("SqliteOpHeadsStore::init not implemented")
    }
    pub fn load(_store_path: &Path) -> Result<Self, jj_lib::backend::BackendLoadError> {
        todo!("SqliteOpHeadsStore::load not implemented")
    }
}



#[async_trait]
impl OpHeadsStore for SqliteOpHeadsStore {
    fn name(&self) -> &str { "sqlite" }
    async fn update_op_heads(&self, _old_ids: &[OperationId], _new_id: &OperationId) -> Result<(), OpHeadsStoreError> { todo!() }
    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> { todo!() }
    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> { todo!() }
}
