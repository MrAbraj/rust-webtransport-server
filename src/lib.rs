//! Minimal WebTransport whiteboard collaboration server.

pub mod handler;
pub mod room;
pub mod server;
pub mod types;
pub mod utils;
use anyhow::Result;
pub use server::WebTransportServer;
pub use types::*;

/// Initialize the server with default configuration
pub async fn init_server() -> Result<WebTransportServer> {
    let identity = utils::gencert::load_or_generate_identity().await?;

    WebTransportServer::new(identity)
}
