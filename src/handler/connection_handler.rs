use crate::room::RoomRegistry;
use crate::types::{
    CursorUpdate, Element, JoinMetadata, RoomCommand, RoomEvent, RoomJoinResponse, RoomSnapshot,
};
use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast;
use wtransport::Connection;
use wtransport::endpoint::IncomingSession;

pub struct ConnectionHandler {
    incoming_session: IncomingSession,
    rooms: std::sync::Arc<RoomRegistry>,
}

impl ConnectionHandler {
    pub fn new(incoming_session: IncomingSession, rooms: std::sync::Arc<RoomRegistry>) -> Self {
        Self {
            incoming_session,
            rooms,
        }
    }

    pub async fn run(self) -> Result<()> {
        let rooms = self.rooms;
        let session_request = self.incoming_session.await?;
        let room_id = match session_request
            .path()
            .strip_prefix("/rooms/")
            .filter(|room_id| !room_id.is_empty() && !room_id.contains('/'))
        {
            Some(room_id) => room_id.to_owned(),
            None => {
                eprintln!(
                    "rejecting WebTransport session with invalid path: {}",
                    session_request.path()
                );
                anyhow::bail!("WebTransport path must be /rooms/<room_id>");
            }
        };

        let connection = session_request.accept().await?;
        let room = rooms.get_or_create(&room_id);
        let join_stream = connection.accept_uni().await?;
        let metadata = read_join_metadata(join_stream).await?;
        let user_id = metadata
            .user_id
            .clone()
            .filter(|user_id| !user_id.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let events = room.event_tx.subscribe();
        let join_response = join_room(&room, user_id.clone(), metadata.clone()).await?;

        println!("client {user_id} joined room {room_id}");
        if let Err(error) = send_snapshot(&connection, join_response.snapshot).await {
            eprintln!("failed to send snapshot to client {user_id}: {error:?}");
            return Err(error);
        }
        let cursors = room.cursor_tx.subscribe();
        let writer = tokio::spawn(write_events(
            connection.clone(),
            events,
            join_response.existing_members,
            user_id.clone(),
        ));
        let cursor_writer =
            tokio::spawn(write_cursors(connection.clone(), cursors, user_id.clone()));

        read_connection(connection.clone(), room.clone(), user_id.clone(), &room_id).await;
        let _ = room
            .command_tx
            .send(RoomCommand::Leave {
                user_id: user_id.clone(),
                metadata,
            })
            .await;
        writer.abort();
        cursor_writer.abort();
        println!("client {user_id} left room");
        Ok(())
    }
}

async fn read_connection(
    connection: Connection,
    room: crate::room::RoomHandle,
    user_id: String,
    room_id: &str,
) {
    loop {
        tokio::select! {
                close_reason = connection.closed() => {
                    println!("client {user_id} disconnected from room {room_id}: {close_reason:?}");
                    break;
                }
                stream = connection.accept_uni() => match stream {
                    Ok(recv) => {
                        let room = room.clone();
                        let user_id = user_id.clone();
                        tokio::spawn(async move {
                            read_element_stream(recv, room, user_id).await;
                        });
                    }
                    Err(error) => { eprintln!("failed to accept client uni stream: {error:?}"); break; }
                },
                datagram = connection.receive_datagram() => match datagram {
                    Ok(bytes) => {
                        match rmp_serde::from_slice::<CursorUpdate>(&bytes) {
                            Ok(mut cursor) => {
                                cursor.user_id = user_id.clone();
                                if let Err(error) = room
                                    .cursor_command_tx
                                    .send(RoomCommand::BroadcastCursor { cursor })
                                    .await
                                {
                                    eprintln!("failed to queue cursor update from {user_id}: {error}");
                                }
                            }
                            Err(error) => {
                                eprintln!("failed to decode cursor datagram from {user_id}: {error}");
                            }
                        }
                    }
                    Err(error) => { eprintln!("datagrams closed: {error:?}"); break; }
                },
        }
    }
}

async fn join_room(
    room: &crate::room::RoomHandle,
    user_id: String,
    metadata: JoinMetadata,
) -> Result<RoomJoinResponse> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    room.command_tx
        .send(RoomCommand::Join {
            user_id,
            metadata,
            response_tx,
        })
        .await?;
    response_rx.await?.map_err(|error| anyhow::anyhow!(error))
}

async fn read_join_metadata(mut stream: wtransport::RecvStream) -> Result<JoinMetadata> {
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let metadata = rmp_serde::from_slice::<JoinMetadata>(&bytes)
        .map_err(|error| anyhow::anyhow!("invalid join metadata MessagePack: {error}"))?;
    println!("received join metadata ({} bytes)", bytes.len());
    Ok(metadata)
}

async fn read_element_stream(
    mut stream: wtransport::RecvStream,
    room: crate::room::RoomHandle,
    user_id: String,
) {
    let mut bytes = Vec::new();
    match stream.read_to_end(&mut bytes).await {
        Ok(_) => match rmp_serde::from_slice::<Element>(&bytes) {
            Ok(element) => {
                if let Err(error) = room
                    .command_tx
                    .send(RoomCommand::UpdateElement { user_id, element })
                    .await
                {
                    eprintln!("failed to queue element update: {error}");
                }
            }
            Err(error) => {
                eprintln!("failed to decode element stream payload: {error}");
            }
        },
        Err(error) => {
            eprintln!("failed to read element stream: {error}");
        }
    }
}

async fn send_snapshot(connection: &Connection, snapshot: RoomSnapshot) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(&snapshot)?;
    let opening = connection.open_uni().await?;
    let mut stream = opening.await?;
    stream.write_all(&bytes).await?;
    stream.finish().await?;
    println!("sent snapshot stream ({} bytes)", bytes.len());
    Ok(())
}

async fn write_events(
    connection: Connection,
    mut events: broadcast::Receiver<RoomEvent>,
    replay: Vec<RoomEvent>,
    user_id: String,
) {
    let opening = match connection.open_uni().await {
        Ok(opening) => opening,
        Err(error) => {
            eprintln!("failed to open event stream: {error:?}");
            return;
        }
    };
    let mut stream = match opening.await {
        Ok(stream) => stream,
        Err(error) => {
            eprintln!("failed to create event stream: {error:?}");
            return;
        }
    };
    for event in replay {
        if let Err(error) = write_event_frame(&mut stream, &event).await {
            eprintln!("failed to write replay event: {error}");
            let _ = stream.finish().await;
            return;
        }
    }
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(error) => {
                eprintln!("event broadcast stopped: {error}");
                break;
            }
        };
        let is_self_event = match &event {
            RoomEvent::UserJoined {
                user_id: event_user_id,
                ..
            }
            | RoomEvent::UserLeft {
                user_id: event_user_id,
                ..
            }
            | RoomEvent::ElementUpdated {
                user_id: event_user_id,
                ..
            } => event_user_id == &user_id,
        };
        if is_self_event {
            continue;
        }
        if let Err(error) = write_event_frame(&mut stream, &event).await {
            eprintln!("failed to write event frame: {error}");
            break;
        }
    }
    let _ = stream.finish().await;
}

async fn write_event_frame(stream: &mut wtransport::SendStream, event: &RoomEvent) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(event)?;
    let length = (bytes.len() as u32).to_be_bytes();
    stream.write_all(&length).await?;
    stream.write_all(&bytes).await?;
    Ok(())
}

async fn write_cursors(
    connection: Connection,
    mut cursors: broadcast::Receiver<CursorUpdate>,
    user_id: String,
) {
    loop {
        let cursor = match cursors.recv().await {
            Ok(cursor) => cursor,
            Err(error) => {
                eprintln!("cursor broadcast stopped: {error}");
                break;
            }
        };
        if cursor.user_id == user_id {
            continue;
        }
        match rmp_serde::to_vec_named(&cursor) {
            Ok(bytes) => {
                if let Err(error) = connection.send_datagram(bytes) {
                    eprintln!("failed to send cursor datagram: {error}");
                    break;
                }
            }
            Err(error) => {
                eprintln!("failed to encode cursor update: {error}");
            }
        }
    }
}
