// Copyright 2024-2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use assert_cmd::Command;
use tempfile::tempdir;

#[test]
fn test_sqlite_backend_basics() {
    let temp_dir = tempdir().unwrap();
    // 1. Initialize the SQLite repository (triggers cli init stub)
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(temp_dir.path())
        .args(["sqlite", "init", "repo"])
        .assert()
        .success();

    let repo_path = temp_dir.path().join("repo");
    assert!(repo_path.exists());
    assert!(repo_path.join(".jj").exists());

    // 2. Write a file in the workspace and check status
    fs::write(repo_path.join("file.txt"), "hello sqlite").unwrap();
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(&repo_path)
        .args(["status"])
        .assert()
        .success();

    // 3. Create a commit (triggers backend write_file, write_tree, write_commit)
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(&repo_path)
        .args(["describe", "-m", "first commit"])
        .assert()
        .success();

    // 4. Create a new change (triggers reading parent commit and writing new commit)
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(&repo_path)
        .args(["new", "-m", "second commit"])
        .assert()
        .success();

    // 5. Check log (triggers backend read_commit, read_tree)
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(&repo_path)
        .args(["log"])
        .assert()
        .success();

    // 6. Check operation log
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(&repo_path)
        .args(["op", "log"])
        .assert()
        .success();

    // 7. Test undo (rollback operation)
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(&repo_path)
        .args(["undo"])
        .assert()
        .success();
}

#[test]
fn test_sqlite_init_error_conditions() {
    let temp_dir = tempdir().unwrap();

    // 1. Test passing unsupported CLI flags (--no-integrate-operation)
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(temp_dir.path())
        .args(["sqlite", "init", "--no-integrate-operation", "bad_repo"])
        .assert()
        .failure();

    // 2. Test passing unsupported CLI flags (--ignore-working-copy)
    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(temp_dir.path())
        .args(["sqlite", "init", "--ignore-working-copy", "bad_repo2"])
        .assert()
        .failure();

    // 3. Test initializing on an invalid filesystem path (a plain file instead of directory)
    let file_path = temp_dir.path().join("plain_file.txt");
    fs::write(&file_path, "not a directory").unwrap();

    let mut cmd = Command::cargo_bin("jj").unwrap();
    cmd.current_dir(temp_dir.path())
        .args(["sqlite", "init", "plain_file.txt/subrepo"])
        .assert()
        .failure();
}
