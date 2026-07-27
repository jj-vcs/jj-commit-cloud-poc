use cc_lib::cc_backend::CommitCloudBackend;
use cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore;
use cc_lib::cc_op_store::CommitCloudOpStore;
use clap::Parser;
use jj_lib::backend::BackendInitError;
use jj_lib::config::StackedConfig;
use jj_lib::local_working_copy::LocalWorkingCopyFactory;
use jj_lib::op_store::{OperationId, RootOperationData};
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::workspace::Workspace;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Parser, Clone, Debug)]
pub struct CcInitArgs {
    /// Remote gRPC server URL (e.g. http://localhost:8080)
    #[arg(long)]
    pub server: String,

    /// Direct remote repository ID to connect to
    #[arg(long = "repo-id", alias = "repo_id")]
    pub repo_id: Option<String>,

    /// Explicitly create a new remote repository if it does not exist yet
    #[arg(long)]
    pub create: bool,

    /// Destination directory for the local workspace
    #[arg(default_value = ".")]
    pub destination: String,
}


fn get_config_path(store_path: &Path) -> PathBuf {
    let direct = store_path.join("config.toml");
    if direct.exists() {
        return direct;
    }
    if let Some(parent) = store_path.parent() {
        let in_store = parent.join("store").join("config.toml");
        if in_store.exists() {
            return in_store;
        }
        let in_parent = parent.join("config.toml");
        if in_parent.exists() {
            return in_parent;
        }
    }
    direct
}

pub async fn cmd_cc_init(args: &CcInitArgs) -> ExitCode {
    let _ = args.create;
    let dest_path = Path::new(&args.destination);

    if !dest_path.exists() {
        if let Err(e) = std::fs::create_dir_all(dest_path) {
            eprintln!("Error: Failed to create destination directory: {:?}", e);
            return ExitCode::FAILURE;
        }
    }

    let mut server_url = args.server.clone();
    if !server_url.starts_with("http://") && !server_url.starts_with("https://") {
        server_url = format!("http://{}", server_url);
    }

    // 1. Load default user settings and signer (required by Jujutsu engine)
    let mut config = StackedConfig::with_defaults();
    let _ = config.add_layer(
        jj_lib::config::ConfigLayer::parse(
            jj_lib::config::ConfigSource::User,
            "snapshot.auto-update-stale = true\n",
        )
        .unwrap(),
    );

    let user_settings = match UserSettings::from_config(config) {
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

    // 2. Define backend, op store, and op heads store initializers
    let server_url_for_closure = server_url.clone();
    let explicit_repo_id_closure = args.repo_id.clone();
    let backend_initializer = move |_settings: &UserSettings, store_path: &Path| {
        let backend = CommitCloudBackend::init(
            store_path,
            &server_url_for_closure,
            explicit_repo_id_closure.as_deref(),
        )
        .map_err(BackendInitError)?;
        Ok(Box::new(backend) as Box<dyn jj_lib::backend::Backend>)
    };


    let server_url_for_op_heads = server_url.clone();
    let op_heads_store_initializer = move |_settings: &UserSettings, store_path: &Path, _root_op_id: &OperationId| {
        let config_path = get_config_path(store_path);
        let config_str = fs::read_to_string(&config_path)
            .map_err(|e| BackendInitError(e.into()))?;
        let repo_id = config_str
            .lines()
            .find(|l| l.starts_with("repo_id ="))
            .and_then(|l| l.split('"').nth(1))
            .unwrap_or("default")
            .to_string();

        let op_heads_store = CommitCloudOpHeadsStore::new(repo_id, server_url_for_op_heads.clone());
        Ok(Box::new(op_heads_store) as Box<dyn jj_lib::op_heads_store::OpHeadsStore>)
    };

    let server_url_for_op = server_url.clone();
    let op_store_initializer = move |_settings: &UserSettings, store_path: &Path, _root_op_data: RootOperationData| {
        let config_path = get_config_path(store_path);
        let config_str = fs::read_to_string(&config_path)
            .map_err(|e| BackendInitError(e.into()))?;
        let repo_id = config_str
            .lines()
            .find(|l| l.starts_with("repo_id ="))
            .and_then(|l| l.split('"').nth(1))
            .unwrap_or("default")
            .to_string();

        let op_store = CommitCloudOpStore::new(repo_id, server_url_for_op.clone());
        Ok(Box::new(op_store) as Box<dyn jj_lib::op_store::OpStore>)
    };

    let working_copy_factory = LocalWorkingCopyFactory {};

    let ws_name_str = dest_path
        .canonicalize()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
        .unwrap_or_else(|| "default".to_string());

    // 3. Delegate workspace creation to Jujutsu workspace engine
    match Workspace::init_with_factories(
        &user_settings,
        dest_path,
        &backend_initializer,
        signer,
        &op_store_initializer,
        &op_heads_store_initializer,
        ReadonlyRepo::default_index_store_initializer(),
        ReadonlyRepo::default_submodule_store_initializer(),
        &working_copy_factory,
        WorkspaceNameBuf::from(ws_name_str),
    )
    .await
    {
        Ok(_) => {
            let config_path = dest_path.join(".jj/repo/config.toml");
            if let Ok(mut existing) = fs::read_to_string(&config_path) {
                existing.push_str("\n[snapshot]\nauto-update-stale = true\n");
                let _ = fs::write(&config_path, existing);
            } else {
                let _ = fs::write(&config_path, "[snapshot]\nauto-update-stale = true\n");
            }
            println!("Initialized remote Commit Cloud repository");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Error: Failed to initialize workspace: {:?}", e);
            ExitCode::FAILURE
        }
    }
}
