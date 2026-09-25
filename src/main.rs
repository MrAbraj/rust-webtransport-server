use anyhow::Result;
use rust_webtransport_server::init_server;

#[tokio::main]
async fn main() -> Result<()> {
    let server = init_server().await?;
    if let Err(error) = server.serve().await {
        eprintln!("WebTransport server stopped: {error:?}");
    }

    Ok(())
}
