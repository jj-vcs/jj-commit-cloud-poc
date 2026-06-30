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

mod local_backend;
mod local_op_store;
mod init_sqlite;
mod proto_helpers;

use jj_cli::cli_util::CliRunner;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::repo::StoreFactories;
use crate::local_backend::SqliteBackend;
use crate::local_op_store::SqliteOpStore;
use crate::local_op_store::SqliteOpHeadsStore;

// The top-level command group: "jj sqlite"
#[derive(clap::Subcommand, Clone, Debug)]
enum CustomCommand {
    /// SQLite backend commands
    Sqlite(SqliteArgs),
}

#[derive(clap::Args, Clone, Debug)]
struct SqliteArgs {
    #[command(subcommand)]
    command: SqliteSubcommand,
}

// The nested subcommands: "jj sqlite init"
#[derive(clap::Subcommand, Clone, Debug)]
enum SqliteSubcommand {
    /// Initialize a repo using the sqlite backend
    Init(crate::init_sqlite::DebugInitSqliteArgs),
}

fn create_store_factories() -> StoreFactories {
    let mut store_factories = StoreFactories::empty();
    
    // Register sqlite backend
    store_factories.add_backend(
        "sqlite",
        Box::new(|_settings, store_path| Ok(Box::new(SqliteBackend::load(store_path)))),
    );
    
    // Register sqlite op store
    store_factories.add_op_store(
        "sqlite",
        Box::new(|_settings, store_path, root_data| {
            Ok(Box::new(SqliteOpStore::load(store_path, root_data)?))
        }),
    );
    
    // Register sqlite op heads store
    store_factories.add_op_heads_store(
        "sqlite",
        Box::new(|_settings, store_path| Ok(Box::new(SqliteOpHeadsStore::load(store_path)?))),
    );
    
    store_factories
}

async fn run_custom_command(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    command: CustomCommand,
) -> Result<(), CommandError> {
    match command {
        CustomCommand::Sqlite(sqlite_args) => match sqlite_args.command {
            SqliteSubcommand::Init(args) => {
                crate::init_sqlite::cmd_debug_init_sqlite(ui, command_helper, &args).await
            }
        },
    }
}

fn main() -> std::process::ExitCode {
    CliRunner::init()
        .add_store_factories(create_store_factories())
        .add_subcommand(run_custom_command)
        .run()
        .into()
}
