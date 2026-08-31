use cc_lib::git_importer::GitImporter;
use clap::Parser;
use jj_cli::command_error::{user_error, CommandError};
use std::path::PathBuf;

#[derive(Parser, Clone, Debug)]
pub struct ImportGitArgs {
    /// Path to the existing .git directory (or project root containing .git)
    #[arg(long)]
    pub git_dir: PathBuf,

    /// Target Repository ID in Commit Cloud (must already exist on server)
    #[arg(long)]
    pub repo_id: String,

    /// Remote Commit Cloud server URL (e.g. http://localhost:8080)
    #[arg(long)]
    pub server: String,
}

pub async fn cmd_import_git(args: &ImportGitArgs) -> Result<(), CommandError> {
    let work_dir = if args.git_dir.file_name() == Some(std::ffi::OsStr::new(".git")) {
        args.git_dir
            .parent()
            .unwrap_or(&args.git_dir)
            .to_path_buf()
    } else {
        args.git_dir.clone()
    };

    println!(
        "Opening Git repository at {:?} for Target Repo ID '{}'...",
        work_dir, args.repo_id
    );
    let importer = GitImporter::new(work_dir, args.repo_id.clone(), args.server.clone());

    let repo_id = cc_lib::util::run_async(move || async move { importer.run().await })
        .map_err(|e| user_error(format!("Git import failed: {e}")))?;

    println!("\n✅ Git repository imported successfully!");
    println!("Cloud Repository ID: {}", repo_id);
    println!("\nTo connect a workspace to this cloud repository, run:");
    println!("jj cc init --server {} --create .", args.server);

    Ok(())
}
