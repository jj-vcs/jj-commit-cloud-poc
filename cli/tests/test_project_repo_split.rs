use assert_cmd::Command;
use std::fs;
use tempfile::tempdir;
use testutils::spawn_server;

#[tokio::test]
async fn test_multiple_repos_share_project_cas_with_isolated_oplog_and_undo() {
    let server = spawn_server().await;
    let server_url = server.url();

    let client_1_dir = tempdir().unwrap();
    let client_2_dir = tempdir().unwrap();

    // Initialize repo-client-1 and repo-client-2 under the same project ("shared-proj")
    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_1_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server_url,
            "--project-id",
            "shared-proj",
            "--repo-id",
            "repo-client-1",
            "--create",
            ".",
        ])
        .assert()
        .success();

    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_2_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server_url,
            "--project-id",
            "shared-proj",
            "--repo-id",
            "repo-client-2",
            "--create",
            ".",
        ])
        .assert()
        .success();

    // Client 1 creates a file and describes commit
    fs::write(
        client_1_dir.path().join("client_1_secret.txt"),
        "shared project content from client 1",
    )
    .unwrap();

    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_1_dir.path())
        .args(["describe", "-m", "client 1 initial feature"])
        .assert()
        .success();

    // Get client 1's commit ID
    let output = Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_1_dir.path())
        .args(["log", "-r", "@", "-T", "commit_id", "--no-graph"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let client_1_commit_id = String::from_utf8(output.stdout).unwrap().trim().to_string();
    assert!(!client_1_commit_id.is_empty());

    // Verify client 2's op log and view are isolated (client 2's default log does not show client 1's commit)
    let client_2_log_output = Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_2_dir.path())
        .args(["log", "-T", "description", "--no-graph"])
        .output()
        .unwrap();
    let client_2_log_str = String::from_utf8(client_2_log_output.stdout).unwrap();
    assert!(
        !client_2_log_str.contains("client 1 initial feature"),
        "Client 2's view should not automatically include client 1's unreferenced commit"
    );

    // Because they share project_id="shared-proj", CommitCloudIndexStore automatically indexes
    // commits in the shared project CAS so client 2 can directly inspect and check out client 1's commit
    let client_2_show = Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_2_dir.path())
        .args(["show", &client_1_commit_id])
        .output()
        .unwrap();
    assert!(
        client_2_show.status.success(),
        "Client 2 should be able to run jj show on client 1's commit directly. stderr: {}",
        String::from_utf8_lossy(&client_2_show.stderr)
    );
    let client_2_show_stdout = String::from_utf8(client_2_show.stdout).unwrap();
    assert!(client_2_show_stdout.contains("client 1 initial feature"));
    assert!(client_2_show_stdout.contains("client_1_secret.txt"));

    // Client 2 creates a new working copy commit on top of client 1's commit directly via CLI
    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_2_dir.path())
        .args(["new", &client_1_commit_id, "-m", "client 2 building on client 1"])
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(client_2_dir.path().join("client_1_secret.txt")).unwrap(),
        "shared project content from client 1"
    );

    // Verify `jj undo` in client 1's repo only affects client 1's operation log and does NOT affect client 2
    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_1_dir.path())
        .args(["undo"])
        .assert()
        .success();

    let client_1_desc_after_undo = Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_1_dir.path())
        .args(["log", "-r", "@", "-T", "description", "--no-graph"])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8(client_1_desc_after_undo.stdout)
            .unwrap()
            .trim(),
        ""
    );

    // Client 2's working copy and op log remain completely intact after client 1's `jj undo`
    let client_2_desc_after_client_1_undo = Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_2_dir.path())
        .args(["log", "-r", "@", "-T", "description", "--no-graph"])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8(client_2_desc_after_client_1_undo.stdout)
            .unwrap()
            .trim(),
        "client 2 building on client 1"
    );
}

#[tokio::test]
async fn test_cross_project_isolation_blocks_access_to_other_projects_commits() {
    let server = spawn_server().await;
    let server_url = server.url();

    let client_1_dir = tempdir().unwrap();
    let client_3_dir = tempdir().unwrap();

    // Initialize client 1 in "project-alpha" and client 3 in "project-beta"
    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_1_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server_url,
            "--project-id",
            "project-alpha",
            "--repo-id",
            "repo-client-1",
            "--create",
            ".",
        ])
        .assert()
        .success();

    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_3_dir.path())
        .args([
            "cc",
            "init",
            "--server",
            server_url,
            "--project-id",
            "project-beta",
            "--repo-id",
            "repo-client-3",
            "--create",
            ".",
        ])
        .assert()
        .success();

    fs::write(client_1_dir.path().join("alpha.txt"), "alpha secret").unwrap();
    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_1_dir.path())
        .args(["describe", "-m", "alpha commit"])
        .assert()
        .success();

    let output = Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_1_dir.path())
        .args(["log", "-r", "@", "-T", "commit_id", "--no-graph"])
        .output()
        .unwrap();
    let alpha_commit_id = String::from_utf8(output.stdout).unwrap().trim().to_string();

    // Client 3 (in project-beta) attempts to inspect client 1's commit via CLI -> must fail!
    Command::cargo_bin("jj")
        .unwrap()
        .current_dir(client_3_dir.path())
        .args(["show", &alpha_commit_id])
        .assert()
        .failure();
}
