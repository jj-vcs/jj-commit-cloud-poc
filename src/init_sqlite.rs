// Copyright 2024 The Jujutsu Authors
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

use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;

use jj_lib::file_util;
use jj_lib::workspace::{Workspace, WorkspaceInitError, default_working_copy_factory};
use jj_lib::repo::{ReadonlyRepo, OpStoreInitializer, OpHeadsStoreInitializer, BackendInitializer};
use jj_lib::signing::Signer;
use jj_lib::ref_name::WorkspaceName;
use crate::local_backend::SqliteBackend;
use crate::local_op_store::SqliteOpStore;
use crate::local_op_store::SqliteOpHeadsStore;
use tracing::instrument;

use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::cli_error;
use jj_cli::command_error::user_error_with_message;
use jj_cli::ui::Ui;

/// Create a new repo in the given directory using the proof-of-concept sqlite
/// backend
///
/// The sqlite backend does not support cloning, fetching, or pushing.
#[derive(clap::Args, Clone, Debug)]
pub(crate) struct DebugInitSqliteArgs {
    /// The destination directory
    #[arg(default_value = ".", value_hint = clap::ValueHint::DirPath)]
    destination: String,
}

async fn init_sqlite(
    user_settings: &jj_lib::settings::UserSettings,
    workspace_root: &Path,
) -> Result<(Workspace, Arc<ReadonlyRepo>), WorkspaceInitError> {
    let backend_initializer: &BackendInitializer =
        &|_settings, store_path| Ok(Box::new(SqliteBackend::init(store_path)));
    let op_store_initializer: &OpStoreInitializer =
        &|_settings, store_path, root_data| Ok(Box::new(SqliteOpStore::init(store_path, root_data)?));
    let op_heads_store_initializer: &OpHeadsStoreInitializer =
        &|_settings, store_path, root_op_id| Ok(Box::new(SqliteOpHeadsStore::init(store_path, root_op_id)?));
    let signer = Signer::from_settings(user_settings)?;
    Workspace::init_with_factories(
        user_settings,
        workspace_root,
        backend_initializer,
        signer,
        op_store_initializer,
        op_heads_store_initializer,
        ReadonlyRepo::default_index_store_initializer(),
        ReadonlyRepo::default_submodule_store_initializer(),
        &*default_working_copy_factory(),
        WorkspaceName::DEFAULT.to_owned(),
    )
    .await
}

#[instrument(skip_all)]
pub(crate) async fn cmd_debug_init_sqlite(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &DebugInitSqliteArgs,
) -> Result<(), CommandError> {
    if command.global_args().no_integrate_operation {
        return Err(cli_error("--no-integrate-operation is not respected"));
    }
    if command.global_args().ignore_working_copy {
        return Err(cli_error("--ignore-working-copy is not respected"));
    }
    if command.global_args().at_operation.is_some() {
        return Err(cli_error("--at-op is not respected"));
    }
    let cwd = command.cwd();
    let wc_path = cwd.join(&args.destination);
    let wc_path = file_util::create_or_reuse_dir(&wc_path)
        .and_then(|_| dunce::canonicalize(wc_path))
        .map_err(|e| user_error_with_message("Failed to create workspace", e))?;

    init_sqlite(
        &command.settings_for_new_workspace(ui, &wc_path)?.0,
        &wc_path,
    )
    .await?;

    let relative_wc_path = file_util::relative_path(cwd, &wc_path);
    writeln!(
        ui.status(),
        "Initialized repo in \"{}\"",
        relative_wc_path.display()
    )?;
    Ok(())
}
