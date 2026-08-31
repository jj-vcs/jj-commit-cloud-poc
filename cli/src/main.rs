use cc_lib::cc_working_copy::CommitCloudWorkingCopyFactory;
use cc_lib::StoreFactoriesExt;
use jj_cli::cli_util::{CliRunner, CommandHelper};
use jj_cli::command_error::CommandError;
use jj_lib::repo::StoreFactories;
use jj_lib::workspace::WorkingCopyFactories;
use std::collections::HashMap;
use std::process::ExitCode;

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

fn create_working_copy_factories() -> WorkingCopyFactories {
    let mut factories = HashMap::new();
    factories.insert(
        "commit_cloud".to_string(),
        Box::new(CommitCloudWorkingCopyFactory::new())
            as Box<dyn jj_lib::working_copy::WorkingCopyFactory>,
    );
    factories
}

async fn run_custom_command(
    _ui: &mut jj_cli::ui::Ui,
    command_helper: &CommandHelper,
    command: CustomCommands,
) -> Result<(), CommandError> {
    match command {
        CustomCommands::Cc { subcommand } => match subcommand {
            CcCommands::Init(args) => commands::init::cmd_cc_init(command_helper, &args).await,
        },
    }
}

fn main() -> ExitCode {
    CliRunner::init()
        .add_store_factories(create_store_factories())
        .add_working_copy_factories(create_working_copy_factories())
        .add_subcommand(run_custom_command)
        .run()
        .into()
}
