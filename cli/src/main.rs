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

use clap::{FromArgMatches, Subcommand};
use jj_cli::cli_util::{CliRunner, CommandHelper};
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::repo::StoreFactories;

mod init_sqlite;

#[derive(Subcommand, Clone, Debug)]
enum CustomCommand {
    #[command(subcommand)]
    Sqlite(SqliteCommand),
}

#[derive(Subcommand, Clone, Debug)]
enum SqliteCommand {
    Init(init_sqlite::DebugInitSqliteArgs),
}

fn create_store_factories() -> StoreFactories {
    StoreFactories::empty()
}

async fn run_custom_command(
    ui: &mut Ui,
    command: &CommandHelper,
    subcommand: CustomCommand,
) -> Result<(), CommandError> {
    match subcommand {
        CustomCommand::Sqlite(SqliteCommand::Init(args)) => {
            init_sqlite::cmd_debug_init_sqlite(ui, command, &args).await
        }
    }
}

fn main() -> std::process::ExitCode {
    CliRunner::init()
        .add_store_factories(create_store_factories())
        .add_subcommand(run_custom_command)
        .run()
        .into()
}
