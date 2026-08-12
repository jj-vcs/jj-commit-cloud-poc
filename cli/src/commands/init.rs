use cc_lib::cc_backend::CommitCloudBackend;
use cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore;
use cc_lib::cc_op_store::CommitCloudOpStore;
use clap::Parser;
use jj_cli::command_error::{user_error, CommandError};
use jj_lib::backend::BackendInitError;
use jj_lib::config::StackedConfig;
use jj_lib::op_store::RootOperationData;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::workspace::{default_working_copy_factory, Workspace};
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

    let op_store_initializer = |_settings: &UserSettings, store_path: &Path, _root_op_data: RootOperationData| {
        let op_store = CommitCloudOpStore::load(store_path).map_err(BackendInitError)?;
        Ok(Box::new(op_store) as Box<dyn jj_lib::op_store::OpStore>)
    };

    let op_heads_store_initializer = |_settings: &UserSettings, store_path: &Path, _root_op_id: &jj_lib::op_store::OperationId| {
        let op_heads_store = CommitCloudOpHeadsStore::load(store_path).map_err(BackendInitError)?;
        Ok(Box::new(op_heads_store) as Box<dyn jj_lib::op_heads_store::OpHeadsStore>)
    };

    // Delegate workspace creation to Jujutsu workspace engine
    Workspace::init_with_factories(
        &user_settings,
        dest_path,
        &backend_initializer,
        signer,
        &op_store_initializer,
        &op_heads_store_initializer,
        ReadonlyRepo::default_index_store_initializer(),
        ReadonlyRepo::default_submodule_store_initializer(),
        &*default_working_copy_factory(),
        WorkspaceName::DEFAULT.to_owned(),
    )
    .await
    .map_err(|e| user_error(format!("Failed to initialize workspace: {:?}", e)))?;

    println!("Initialized remote Commit Cloud repository");
    Ok(())
}
