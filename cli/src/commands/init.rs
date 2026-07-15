use clap::Parser;
use std::process::ExitCode;

#[derive(Parser, Clone, Debug)]
pub struct CcInitArgs {
    /// Remote gRPC server URL (e.g. http://localhost:8080)
    #[arg(long)]
    pub server: String,

    //TODO: Add server side guard for repository creation with --create
    /// Explicitly create a new remote repository if it does not exist yet
    #[arg(long)]
    pub create: bool,


    /// Destination directory for the local workspace
    #[arg(default_value = ".")]
    pub destination: String,
}

pub async fn cmd_cc_init(args: &CcInitArgs) -> ExitCode {
    eprintln!(
        "Error: CommitCloudBackend is not yet implemented."
    );
    eprintln!(
        "Requested init: server={}, create={}, dest={}",
        args.server, args.create, args.destination
    );
    ExitCode::FAILURE
}
