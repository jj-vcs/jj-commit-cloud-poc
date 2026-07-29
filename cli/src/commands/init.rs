use cc_lib::cc_backend::CommitCloudBackend;
use clap::Parser;
use jj_cli::command_error::{user_error, CommandError};
use jj_lib::backend::BackendInitError;
use jj_lib::config::StackedConfig;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::workspace::Workspace;
use std::path::Path;

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

pub async fn cmd_cc_init(args: &CcInitArgs) -> Result<(), CommandError> {
    let _ = args.create;
    let dest_path = Path::new(&args.destination);

    // Load default user settings and signer (required by Jujutsu engine)
    let user_settings = UserSettings::from_config(StackedConfig::with_defaults())
        .map_err(|e| user_error(format!("Failed to initialize settings: {:?}", e)))?;
    
    let signer = Signer::from_settings(&user_settings)
        .map_err(|e| user_error(format!("Failed to initialize signature signer: {:?}", e)))?;

    // Define the backend initializer closure for Workspace
    let backend_initializer = |_settings: &UserSettings, store_path: &Path| {
        let backend = CommitCloudBackend::init(
            store_path,
            &args.server,
        )
        .map_err(BackendInitError)?;
        Ok(Box::new(backend) as Box<dyn jj_lib::backend::Backend>)
    };

    // Delegate workspace creation to Jujutsu workspace engine
    Workspace::init_with_backend(
        &user_settings,
        dest_path,
        &backend_initializer,
        signer,
    )
    .await
    .map_err(|e| user_error(format!("Failed to initialize workspace: {:?}", e)))?;

    println!("Initialized remote Commit Cloud repository");
    Ok(())
}
