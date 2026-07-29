use jj_cli::cli_util::{CliRunner, CommandHelper};
use jj_cli::command_error::CommandError;
use jj_lib::repo::StoreFactories;
use std::process::ExitCode;
use cc_lib::StoreFactoriesExt;

mod commands;

#[derive(clap::Subcommand, Clone, Debug)]
enum CustomCommands {
    Cc {
        #[command(subcommand)]
        subcommand: CcCommands,
    },
}

#[derive(clap::Subcommand, Clone, Debug)]
enum CcCommands {
    /// Initialize a remote Commit Cloud repository and local working copy
    Init(commands::init::CcInitArgs),
}

fn create_store_factories() -> StoreFactories {
    let mut factories = StoreFactories::empty();
    factories.add_commit_cloud();
    factories
}

async fn run_custom_command(
    _ui: &mut jj_cli::ui::Ui,
    _command_helper: &CommandHelper,
    command: CustomCommands,
) -> Result<(), CommandError> {
    match command {
        CustomCommands::Cc { subcommand } => match subcommand {
            CcCommands::Init(args) => commands::init::cmd_cc_init(&args).await,
        },
    }
}

fn main() -> ExitCode {
    CliRunner::init()
        .add_store_factories(create_store_factories())
        .add_subcommand(run_custom_command)
        .run()
        .into()
}
