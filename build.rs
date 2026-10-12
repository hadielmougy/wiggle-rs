// Generates the gRPC stubs from the vendored proto (proto/wiggle.proto) -- the wire contract with the
// Wiggle server. Keep it in sync with the canonical copy in the engine repo
// (wiggle: proto/src/main/proto/wiggle.proto). Needs `protoc` on the PATH (or PROTOC set).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/wiggle.proto");
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["proto/wiggle.proto"], &["proto"])?;
    Ok(())
}
