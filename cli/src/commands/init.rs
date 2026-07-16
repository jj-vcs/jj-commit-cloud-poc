use cc_lib::cc_backend::CommitCloudBackend;
use clap::Parser;
use jj_lib::config::StackedConfig;
use jj_lib::backend::BackendInitError;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::workspace::Workspace;
use std::path::Path;
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
    let _ = args.create;
    let dest_path = Path::new(&args.destination);

    // Load default user settings and signer (required by Jujutsu engine)
    let user_settings = match UserSettings::from_config(StackedConfig::with_defaults()) {
        Ok(settings) => settings,
        Err(e) => {
            eprintln!("Error: Failed to initialize settings: {:?}", e);
            return ExitCode::FAILURE;
        }
    };
    
    let signer = match Signer::from_settings(&user_settings) {
        Ok(signer) => signer,
        Err(e) => {
            eprintln!("Error: Failed to initialize signature signer: {:?}", e);
            return ExitCode::FAILURE;
        }
    };

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
    match Workspace::init_with_backend(
        &user_settings,
        dest_path,
        &backend_initializer,
        signer,
    )
    .await
    {
        Ok(_) => {
            println!("Initialized remote Commit Cloud repository");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Error: Failed to initialize workspace: {:?}", e);
            return ExitCode::FAILURE;
        }
    }
}
