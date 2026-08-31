use cc_lib::cc_backend::CommitCloudBackend;
use cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore;
use cc_lib::cc_op_store::CommitCloudOpStore;
use cc_lib::cc_working_copy::CommitCloudWorkingCopyFactory;
use clap::Parser;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::{user_error, CommandError};
use jj_lib::backend::BackendInitError;
use jj_lib::op_store::RootOperationData;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::ReadonlyRepo;
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

    /// Workspace name (defaults to "default")
    #[arg(long, default_value = "default")]
    pub workspace_name: String,

    /// Working copy type ("commit_cloud" or "local")
    #[arg(long, default_value = "commit_cloud")]
    pub working_copy_type: String,

    /// Destination directory for the local workspace
    #[arg(default_value = ".")]
    pub destination: String,
}

pub async fn cmd_cc_init(
    command_helper: &CommandHelper,
    args: &CcInitArgs,
) -> Result<(), CommandError> {
    let _ = args.create;
    let dest_path = Path::new(&args.destination);

    let user_settings = command_helper.settings();
    let signer = Signer::from_settings(user_settings)
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

    let op_store_initializer =
        |_settings: &UserSettings, store_path: &Path, _root_op_data: RootOperationData| {
            let op_store = CommitCloudOpStore::load(store_path).map_err(BackendInitError)?;
            Ok(Box::new(op_store) as Box<dyn jj_lib::op_store::OpStore>)
        };

    let op_heads_store_initializer =
        |_settings: &UserSettings,
         store_path: &Path,
         _root_op_id: &jj_lib::op_store::OperationId| {
            let op_heads_store =
                CommitCloudOpHeadsStore::load(store_path).map_err(BackendInitError)?;
            Ok(Box::new(op_heads_store) as Box<dyn jj_lib::op_heads_store::OpHeadsStore>)
        };

    let working_copy_factory: Box<dyn jj_lib::working_copy::WorkingCopyFactory> =
        match args.working_copy_type.as_str() {
            "commit_cloud" => Box::new(CommitCloudWorkingCopyFactory::new()),
            "local" => Box::new(jj_lib::local_working_copy::LocalWorkingCopyFactory {}),
            other => {
                return Err(user_error(format!(
                    "Unsupported working copy type: '{other}' (expected 'commit_cloud' or 'local')"
                )))
            }
        };
    let workspace_name = WorkspaceNameBuf::from(args.workspace_name.clone());

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
        working_copy_factory.as_ref(),
        workspace_name,
    )
    .await
    .map_err(|e| user_error(format!("Failed to initialize workspace: {:?}", e)))?;

    println!("Initialized remote Commit Cloud repository");
    Ok(())
}
