fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .compile_protos(
            &[
                "src/backend.proto",
                "src/op_store.proto",
                "src/op_heads_store.proto",
            ],
            &["src"],
        )?;
    Ok(())
}
