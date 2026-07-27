use cc_lib::cc_backend::CommitCloudBackend;
use cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore;
use cc_lib::cc_op_store::CommitCloudOpStore;
use clap::Parser;
use jj_cli::cli_util::CliRunner;
use jj_lib::backend::BackendLoadError;
use jj_lib::repo::StoreFactories;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

mod commands;

#[derive(Parser)]
#[command(name = "jj", about = "Jujutsu CLI with Commit Cloud backend")]
struct CcCli {
    #[command(subcommand)]
    command: CcSubcommand,
}

#[derive(clap::Subcommand)]
enum CcSubcommand {
    Cc {
        #[command(subcommand)]
        subcommand: CcCommands,
    },
}

#[derive(clap::Subcommand)]
enum CcCommands {
    /// Initialize a remote Commit Cloud repository and local working copy
    Init(commands::init::CcInitArgs),
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

fn parse_config(config_str: &str) -> Result<(String, String), std::io::Error> {
    let repo_id = config_str
        .lines()
        .find(|l| l.starts_with("repo_id ="))
        .and_then(|l| l.split('"').nth(1))
        .unwrap_or("default")
        .to_string();

    let server_url = config_str
        .lines()
        .find(|l| l.starts_with("server_url ="))
        .and_then(|l| l.split('"').nth(1))
        .unwrap_or("http://localhost:8080")
        .to_string();

    Ok((server_url, repo_id))
}

fn main() -> ExitCode {
    // If the first argument is "cc", handle cc subcommands
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "cc" {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let cc_cli = CcCli::parse();
        match cc_cli.command {
            CcSubcommand::Cc { subcommand } => match subcommand {
                CcCommands::Init(ref init_args) => {
                    return rt.block_on(commands::init::cmd_cc_init(init_args));
                }
            },
        }
    }

    // Register CommitCloudBackend, CommitCloudOpStore, and CommitCloudOpHeadsStore into StoreFactories
    let mut store_factories = StoreFactories::empty();
    store_factories.add_backend(CommitCloudBackend::name(), Box::new(|_settings, store_path| {
        let backend = CommitCloudBackend::load(store_path).map_err(BackendLoadError)?;
        Ok(Box::new(backend) as Box<dyn jj_lib::backend::Backend>)
    }));

    store_factories.add_op_store(CommitCloudOpStore::name(), Box::new(|_settings, store_path, _root_op_id| {
        let config_path = get_config_path(store_path);
        let config_str = fs::read_to_string(&config_path)
            .map_err(|e| BackendLoadError(e.into()))?;
        let (server_url, repo_id) = parse_config(&config_str)
            .map_err(|e| BackendLoadError(e.into()))?;
        let op_store = CommitCloudOpStore::new(repo_id, server_url);
        Ok(Box::new(op_store) as Box<dyn jj_lib::op_store::OpStore>)
    }));

    store_factories.add_op_heads_store(CommitCloudOpHeadsStore::name(), Box::new(|_settings, store_path| {
        let config_path = get_config_path(store_path);
        let config_str = fs::read_to_string(&config_path)
            .map_err(|e| BackendLoadError(e.into()))?;
        let (server_url, repo_id) = parse_config(&config_str)
            .map_err(|e| BackendLoadError(e.into()))?;
        let op_heads_store = CommitCloudOpHeadsStore::new(repo_id, server_url);
        Ok(Box::new(op_heads_store) as Box<dyn jj_lib::op_heads_store::OpHeadsStore>)
    }));

    let runner = CliRunner::init().add_store_factories(store_factories);
    let code = runner.run();
    ExitCode::from(code)
}
