use std::fs;
use std::process::Command;
use tempfile::tempdir;
use testutils::spawn_server;

#[tokio::test]
async fn test_import_git_repository_succeeds() {
    // 1. Initialize a TestWorkspace
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    // 2. Create a real Git repository with commits
    let git_temp_dir = tempdir().expect("Failed to create temporary git repo dir");
    let git_path = git_temp_dir.path();

    let init_status = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status()
        .expect("Failed to execute git init");
    assert!(init_status.success());

    Command::new("git")
        .args(["checkout", "-b", "main"])
        .current_dir(git_path)
        .status()
        .unwrap();

    Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(git_path)
        .status()
        .unwrap();
    Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(git_path)
        .status()
        .unwrap();

    let file_path = git_path.join("README.md");
    fs::write(&file_path, "# Commit Cloud Git Import Test\n").unwrap();

    Command::new("git")
        .args(["add", "README.md"])
        .current_dir(git_path)
        .status()
        .unwrap();

    let commit_status = Command::new("git")
        .args(["commit", "-m", "initial git commit"])
        .current_dir(git_path)
        .status()
        .unwrap();
    assert!(commit_status.success());

    // 3. Run `jj cc import-git` with required --repo-id
    let mut cmd = assert_cmd::Command::cargo_bin("jj").expect("jj binary should build");
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);

    cmd.assert().success();

    // 4. Verify `jj log` in the workspace succeeds without panic
    let mut log_cmd = ws.jj_cmd();
    log_cmd.args(["log"]).assert().success();
}

#[tokio::test]
async fn test_import_git_preserves_existing_unrelated_workspace_history() {
    let ws = testutils::TestWorkspace::init().await;
    let config_path = ws.repo_path().join(".jj/repo/store/config.toml");
    let config_str = fs::read_to_string(&config_path).unwrap();
    let config: toml::Value = toml::from_str(&config_str).unwrap();
    let repo_id = config.get("repo_id").unwrap().as_str().unwrap().to_string();

    // 1. Create existing local commit in the workspace
    let local_file = ws.repo_path().join("local.txt");
    fs::write(&local_file, "hello from existing local commit\n").unwrap();
    let mut describe_cmd = ws.jj_cmd();
    describe_cmd
        .args(["describe", "-m", "my local workspace commit"])
        .assert()
        .success();

    // 2. Create a separate Git repository with unrelated history
    let git_temp_dir = tempdir().expect("Failed to create temporary git repo dir");
    let git_path = git_temp_dir.path();

    let init_status = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status()
        .expect("Failed to execute git init");
    assert!(init_status.success());

    Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(git_path)
        .status()
        .unwrap();
    Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(git_path)
        .status()
        .unwrap();

    let file_path = git_path.join("git_file.txt");
    fs::write(&file_path, "hello from git commit\n").unwrap();

    Command::new("git")
        .args(["add", "git_file.txt"])
        .current_dir(git_path)
        .status()
        .unwrap();

    let commit_status = Command::new("git")
        .args(["commit", "-m", "unrelated imported git commit"])
        .current_dir(git_path)
        .status()
        .unwrap();
    assert!(commit_status.success());

    // 3. Import the Git repository
    let mut cmd = assert_cmd::Command::cargo_bin("jj").expect("jj binary should build");
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--repo-id",
        &repo_id,
        "--server",
        ws.server_url(),
    ]);
    cmd.assert().success();

    // 4. Check `jj log` shows both independent branches
    let mut log_cmd = ws.jj_cmd();
    let output = log_cmd.args(["log", "-r", "all()"]).assert().success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);

    assert!(
        stdout.contains("my local workspace commit"),
        "Log output should contain existing workspace commit:\n{}",
        stdout
    );
    assert!(
        stdout.contains("unrelated imported git commit"),
        "Log output should contain imported git commit:\n{}",
        stdout
    );
    assert!(
        stdout.contains("main"),
        "Log output should contain main bookmark:\n{}",
        stdout
    );
}

#[tokio::test]
async fn test_import_git_fails_on_unregistered_repo() {
    let server = spawn_server().await;

    let git_temp_dir = tempdir().expect("Failed to create temporary git repo dir");
    let git_path = git_temp_dir.path();

    let _ = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(git_path)
        .status();

    let mut cmd = assert_cmd::Command::cargo_bin("jj").expect("jj binary should build");
    cmd.args([
        "cc",
        "import-git",
        "--git-dir",
        git_path.to_str().unwrap(),
        "--repo-id",
        "non-existent-repo-id",
        "--server",
        server.url(),
    ]);

    cmd.assert().failure();
}
