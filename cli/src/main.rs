use clap::{Parser, Subcommand};
use std::process::ExitCode;

mod commands;

#[derive(Parser)]
#[command(name = "jj", about = "Jujutsu CLI with Commit Cloud backend")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Cc {
        #[command(subcommand)]
        subcommand: CcCommands,
    },
}

#[derive(Subcommand)]
enum CcCommands {
    /// Initialize a remote Commit Cloud repository and local working copy
    Init(commands::init::CcInitArgs),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Cc { subcommand } => match subcommand {
            CcCommands::Init(args) => commands::init::cmd_cc_init(&args).await,
        },
    }
}
