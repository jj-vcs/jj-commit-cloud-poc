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

use clap::Args;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::{cli_error, user_error_with_message, CommandError};
use jj_cli::ui::Ui;

#[derive(Args, Clone, Debug)]
pub(crate) struct DebugInitSqliteArgs {
    #[arg(default_value = ".")]
    pub destination: String,
}

pub(crate) async fn cmd_debug_init_sqlite(
    _ui: &mut Ui,
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
    let _wc_path = jj_lib::file_util::create_or_reuse_dir(&wc_path)
        .and_then(|_| dunce::canonicalize(wc_path))
        .map_err(|e| user_error_with_message("Failed to create workspace", e))?;

    todo!("init_sqlite: implement SQLite repository initialization")
}
