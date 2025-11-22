fn main() -> Result<(), Box<dyn std::error::Error>> {
    // For now, skip protoc requirement - generate stubs manually
    println!("cargo:rerun-if-changed=../proto/waffledb.proto");
    Ok(())
}
