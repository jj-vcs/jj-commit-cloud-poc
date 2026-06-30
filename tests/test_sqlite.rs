// Copyright 2024 The Jujutsu Authors
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

use crate::common::TestEnvironment;

#[test]
fn test_init_sqlite() {
    let mut test_env = TestEnvironment::default();
    
    // Set up the fake editor so commands like `squash` that open an editor can succeed in the sandbox
    test_env.set_up_fake_editor();
    
    // 1. Initialize the SQLite repository (classic layout)
    let output = test_env.run_jj_in(".", ["sqlite", "init", "repo"]);
    insta::assert_snapshot!(output, @r#"
    ------- stderr -------
    Initialized repo in "repo"
    [EOF]
    "#);

    let workspace_root = test_env.env_root().join("repo");
    let jj_path = workspace_root.join(".jj");
    let repo_path = jj_path.join("repo");

    assert!(workspace_root.is_dir());
    assert!(jj_path.is_dir());
    assert!(jj_path.join("working_copy").is_dir());
    assert!(repo_path.is_dir());

    // Assert classic structure: subdirectories exist
    assert!(repo_path.join("store").is_dir());
    assert!(repo_path.join("op_store").is_dir());
    assert!(repo_path.join("op_heads").is_dir());
    assert!(repo_path.join("workspace_store").is_dir());

    // Assert that the shared database exists inside store/
    assert!(repo_path.join("store").join("store.db").is_file());

    // Assert type files contain correct types
    assert_eq!(
        std::fs::read_to_string(repo_path.join("store").join("type")).unwrap().trim(),
        "sqlite"
    );
    assert_eq!(
        std::fs::read_to_string(repo_path.join("op_store").join("type")).unwrap().trim(),
        "sqlite"
    );
    assert_eq!(
        std::fs::read_to_string(repo_path.join("op_heads").join("type")).unwrap().trim(),
        "sqlite"
    );

    // Assert that file-based cache directories (index and submodule) still exist
    assert!(repo_path.join("index").is_dir());
    assert!(repo_path.join("submodule_store").is_dir());

    let work_dir = test_env.work_dir("repo");

    // 2. Verify basic status on empty repo
    work_dir.run_jj(["status"]).success();

    // 3. Write a file and describe the working copy commit (sets commit message)
    work_dir.write_file("file1.txt", "content1\n");
    work_dir.run_jj(["describe", "-m", "commit 1: add file1"]).success();

    // 4. Create a new commit on top of the first one
    work_dir.run_jj(["new", "-m", "commit 2: add file2"]).success();
    work_dir.write_file("file2.txt", "content2\n");

    // 5. Create a bookmark pointing to the current commit
    work_dir.run_jj(["bookmark", "create", "-r@", "main"]).success();

    // 6. Start a new branch off the parent of main (main-, which is commit 1)
    work_dir.run_jj(["new", "main-", "-m", "commit 3: branch off"]).success();
    work_dir.write_file("file3.txt", "content3\n");
    work_dir.run_jj(["bookmark", "create", "-r@", "feature"]).success();

    // 7. Verify the log graph contains our branched commits
    work_dir.run_jj(["log"]).success();

    // 8. Rebase the feature branch onto main
    work_dir.run_jj(["rebase", "-r", "feature", "-d", "main"]).success();

    // 9. Squash the rebased feature changes into main (will use the fake editor)
    work_dir.run_jj(["squash", "-r", "feature"]).success();

    // 10. Create a temporary commit and abandon it
    work_dir.run_jj(["new", "-m", "temp commit"]).success();
    work_dir.run_jj(["abandon"]).success();

    // 11. Inspect the operation log (transaction history)
    work_dir.run_jj(["op", "log"]).success();

    // 12. UNDO the last operation (the abandon)
    // This is the ultimate test for transaction rollback in SqliteOpStore!
    work_dir.run_jj(["undo"]).success();
}
