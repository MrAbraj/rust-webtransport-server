//! Main WebTransport server implementation
use crate::handler::connection_handler::ConnectionHandler;
use crate::room::RoomRegistry;
use anyhow::Result;
use std::time::Duration;
use wtransport::endpoint::endpoint_side::Server;
use wtransport::{Endpoint, Identity, ServerConfig};

pub struct WebTransportServer {
    endpoint: Endpoint<Server>,
    rooms: std::sync::Arc<RoomRegistry>,
}

impl WebTransportServer {
    pub fn new(identity: Identity) -> Result<Self> {
        let config = ServerConfig::builder()
            .with_bind_default(4433)
            .with_identity(identity)
            .keep_alive_interval(Some(Duration::from_secs(3)))
            .build();

        let endpoint = Endpoint::server(config)?;

        Ok(Self {
            endpoint,
            rooms: RoomRegistry::new(),
        })
    }

    pub fn local_port(&self) -> u16 {
        self.endpoint.local_addr().unwrap().port()
    }

    pub async fn serve(self) -> Result<()> {
        println!("WebTransport server running on port {}", self.local_port());

        loop {
            println!("Waiting for an incoming session...");

            let incoming_session = self.endpoint.accept().await;

            println!("Incoming session accepted");

            let rooms = self.rooms.clone();

            tokio::spawn(async move {
                let handler = ConnectionHandler::new(incoming_session, rooms);
                if let Err(error) = handler.run().await {
                    eprintln!("Connection handler error: {error:?}");
                }
            });
        }
    }
}
