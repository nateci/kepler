//! Kepler server binary — placeholder. Real entry point will:
//!   1. Parse CLI flags (node id, peers, data dir, listen addr)
//!   2. Open the LSM `Engine` (or `MemEngine` in --dev)
//!   3. Build the `RaftStorage` from a dedicated WAL + log file
//!   4. Construct `Node`, wire to `GrpcTransport`
//!   5. Spawn the driver loop (tick / step / propose / ready / advance)
//!   6. Serve the gRPC Kv service, routing proposals into `Node::propose`

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    tracing::info!("kepler-server starting (TODO: implement)");
    Ok(())
}
