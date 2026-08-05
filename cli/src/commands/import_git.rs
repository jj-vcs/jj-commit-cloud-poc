use cc_lib::git_importer::GitImporter;
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
pub struct ImportGitArgs {
    /// Path to the existing .git directory (or project root containing .git)
    #[arg(long)]
    pub git_dir: PathBuf,

    /// Target Repository ID in Commit Cloud
    #[arg(long)]
    pub repo_id: String,

    /// Remote Commit Cloud server URL (e.g. https://jj-commit-cloud-server-827228433919.us-central1.run.app)
    #[arg(long)]
    pub server: String,
}

pub async fn cmd_import_git(args: &ImportGitArgs) -> Result<(), Box<dyn std::error::Error>> {
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

    let repo_id = importer.run().await.map_err(|e| format!("{e}"))?;

    println!("\n✅ Git repository imported successfully!");
    println!("Cloud Repository ID: {}", repo_id);
    println!("\nTo connect a workspace to this cloud repository, run:");
    println!(
        "sjj cc init --server {} --repo-id {}",
        args.server, repo_id
    );

    Ok(())
}
