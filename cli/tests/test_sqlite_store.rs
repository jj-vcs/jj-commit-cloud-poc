use std::fs;

#[tokio::test]
async fn test_sqlite_store_init_and_snapshot_succeeds() {
    let workspace = testutils::TestWorkspace::init_sqlite().await;
    let repo_path = workspace.repo_path();

    // Create a file in the working copy and snapshot it
    fs::write(repo_path.join("sqlite_test.txt"), "hello sqlite store!\n").unwrap();

    workspace
        .jj_cmd()
        .args(["describe", "-m", "sqlite store test commit"])
        .assert()
        .success();

    // Verify commit description can be read back from SQLite store
    workspace
        .jj_cmd()
        .args(["log", "--no-graph", "-r", "@", "-T", "description"])
        .assert()
        .success()
        .stdout("sqlite store test commit\n");
}

#[tokio::test]
async fn test_cli_fails_when_table_does_not_exist() {
    let workspace = testutils::TestWorkspace::init_sqlite().await;
    let repo_path = workspace.repo_path();

    // Create a file in the working copy and snapshot it
    fs::write(repo_path.join("sqlite_test.txt"), "hello sqlite store!\n").unwrap();
    workspace
        .jj_cmd()
        .args(["describe", "-m", "sqlite store test commit"])
        .assert()
        .success();

    // Drop the commits table in the SQLite database to simulate missing table
    {
        let conn = rusqlite::Connection::open(workspace.db_path()).unwrap();
        conn.execute("DROP TABLE commits", []).unwrap();
    }

    // Attempting to read commits via `jj log` fails
    workspace
        .jj_cmd()
        .args(["log", "-r", "@"])
        .assert()
        .failure();
}

#[test]
fn test_server_fails_when_database_parent_path_is_not_a_directory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let blocker_file = temp_dir.path().join("blocking_file");
    fs::write(&blocker_file, "blocking content").unwrap();

    let invalid_db_path = blocker_file.join("store.db");
    let expected_io_err = fs::create_dir_all(&blocker_file).unwrap_err();
    let expected_stderr = format!(
        "Error: Failed to create parent directory '{}' for SQLite database: {}\n",
        blocker_file.display(),
        expected_io_err
    );

    let mut cmd = assert_cmd::Command::cargo_bin("jj-cc-server").unwrap();
    cmd.args([
        "--port=0",
        "--store-type=sqlite",
        &format!("--sqlite-path={}", invalid_db_path.display()),
    ]);

    cmd.assert()
        .failure()
        .stderr(predicates::ord::eq(expected_stderr.as_str()));
}

#[test]
fn test_server_fails_when_sqlite_database_file_permission_denied() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = tempfile::tempdir().unwrap();
    let readonly_db = temp_dir.path().join("readonly.db");
    fs::write(&readonly_db, b"").unwrap();
    fs::set_permissions(&readonly_db, fs::Permissions::from_mode(0o000)).unwrap();

    let expected_rusqlite_err = rusqlite::Connection::open(&readonly_db).unwrap_err();
    let expected_stderr = format!(
        "Error: Failed to open SQLite database at '{}': {}\n",
        readonly_db.display(),
        expected_rusqlite_err
    );

    let mut cmd = assert_cmd::Command::cargo_bin("jj-cc-server").unwrap();
    cmd.args([
        "--port=0",
        "--store-type=sqlite",
        &format!("--sqlite-path={}", readonly_db.display()),
    ]);

    cmd.assert()
        .failure()
        .stderr(predicates::ord::eq(expected_stderr.as_str()));
}

#[test]
fn test_server_fails_when_home_directory_is_unwritable_for_default_sqlite_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let blocker_file = temp_dir.path().join("fake_home_file");
    fs::write(&blocker_file, "blocking").unwrap();

    let expected_default_dir = blocker_file.join(".jj-cc-server");
    let expected_io_err = fs::create_dir_all(&expected_default_dir).unwrap_err();
    let expected_stderr = format!(
        "Error: Failed to create default SQLite directory '{}': {}\n",
        expected_default_dir.display(),
        expected_io_err
    );

    let mut cmd = assert_cmd::Command::cargo_bin("jj-cc-server").unwrap();
    cmd.env("HOME", blocker_file.to_str().unwrap());
    cmd.args(["--port=0", "--store-type=sqlite"]);

    cmd.assert()
        .failure()
        .stderr(predicates::ord::eq(expected_stderr.as_str()));
}
