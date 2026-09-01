use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../server/src/");
    println!("cargo:rerun-if-changed=../server/Cargo.toml");

    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    
    // OUT_DIR is target/<profile>/build/testutils-<hash>/out/
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR should be set");
    let out_path = Path::new(&out_dir);

    // Set a different target directory for the child Cargo build to avoid deadlocks!
    let server_target_dir = out_path.join("server_target");
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR should be set");
    let workspace_manifest = Path::new(&manifest_dir).parent().unwrap().join("Cargo.toml");

    let mut cmd = Command::new(cargo);
    cmd.args([
        "build",
        "--manifest-path",
        workspace_manifest.to_str().unwrap(),
        "-p",
        "jj-commit-cloud-server",
        "--bin",
        "jj-cc-server",
    ]);
    cmd.env("CARGO_TARGET_DIR", &server_target_dir);

    let status = cmd
        .status()
        .expect("Failed to execute cargo build for jj-cc-server");

    if !status.success() {
        panic!("Failed to build jj-cc-server helper binary");
    }

    // Resolve the main target profile directory (e.g. target/debug/)
    // out_path is: target/<profile>/build/testutils-<hash>/out/
    let profile_dir = out_path
        .parent().unwrap() // testutils-<hash>
        .parent().unwrap() // build
        .parent().unwrap(); // <profile> (e.g. debug or release)

    let profile_name = profile_dir.file_name().unwrap().to_str().unwrap();

    // The binary is compiled under server_target_dir/<profile_name>/jj-cc-server
    let compiled_binary_path = server_target_dir
        .join(profile_name)
        .join("jj-cc-server");

    let dest_binary_path = profile_dir.join("jj-cc-server");

    let _ = fs::remove_file(&dest_binary_path);
    fs::copy(&compiled_binary_path, &dest_binary_path)
        .expect("Failed to copy compiled jj-cc-server binary to target profile directory");
}
