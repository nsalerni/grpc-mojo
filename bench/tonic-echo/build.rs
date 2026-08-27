fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/echo.proto");
    tonic_build::compile_protos(proto)?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
