use crate::types::{
    CursorUpdate, Element, JoinMetadata, RoomCommand, RoomEvent, RoomJoinResponse, RoomSnapshot,
};
use dashmap::{DashMap, mapref::entry::Entry};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

#[derive(Clone)]
pub struct RoomHandle {
    pub command_tx: mpsc::Sender<RoomCommand>,
    pub cursor_command_tx: mpsc::Sender<RoomCommand>,
    pub event_tx: broadcast::Sender<RoomEvent>,
    pub cursor_tx: broadcast::Sender<CursorUpdate>,
}

pub struct RoomRegistry {
    rooms: DashMap<String, RoomHandle>,
}

impl RoomRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            rooms: DashMap::new(),
        })
    }

    pub fn get_or_create(&self, room_id: &str) -> RoomHandle {
        match self.rooms.entry(room_id.to_owned()) {
            Entry::Occupied(room) => room.get().clone(),
            Entry::Vacant(room) => {
                let (command_tx, command_rx) = mpsc::channel(256);
                let (cursor_command_tx, cursor_command_rx) = mpsc::channel(256);
                let (event_tx, _) = broadcast::channel(256);
                let (cursor_tx, _) = broadcast::channel(256);
                let handle = RoomHandle {
                    command_tx,
                    cursor_command_tx,
                    event_tx,
                    cursor_tx,
                };
                room.insert(handle.clone());
                tokio::spawn(
                    RoomActor {
                        canvas_state: HashMap::new(),
                        members: HashMap::new(),
                        command_rx,
                        event_tx: handle.event_tx.clone(),
                    }
                    .run(),
                );
                tokio::spawn(
                    CursorActor {
                        command_rx: cursor_command_rx,
                        cursor_tx: handle.cursor_tx.clone(),
                    }
                    .run(),
                );
                handle
            }
        }
    }
}

struct RoomActor {
    canvas_state: HashMap<String, Element>,
    members: HashMap<String, JoinMetadata>,
    command_rx: mpsc::Receiver<RoomCommand>,
    event_tx: broadcast::Sender<RoomEvent>,
}

impl RoomActor {
    async fn run(mut self) {
        while let Some(command) = self.command_rx.recv().await {
            match command {
                RoomCommand::Join {
                    user_id,
                    metadata,
                    response_tx,
                } => {
                    if self.members.contains_key(&user_id) {
                        let _ = response_tx
                            .send(Err(format!("user {user_id} already has an active session")));
                        continue;
                    }
                    let existing_members = self
                        .members
                        .iter()
                        .map(|(member_id, member_metadata)| RoomEvent::UserJoined {
                            user_id: member_id.clone(),
                            user_name: member_metadata.user_name.clone(),
                            extra_properties: member_metadata.extra_properties.clone(),
                        })
                        .collect();
                    let snapshot = RoomSnapshot {
                        user_id: user_id.clone(),
                        elements: self.canvas_state.values().cloned().collect(),
                    };
                    self.members.insert(user_id.clone(), metadata.clone());
                    let _ = response_tx.send(Ok(RoomJoinResponse {
                        snapshot,
                        existing_members,
                    }));
                    let _ = self.event_tx.send(RoomEvent::UserJoined {
                        user_id,
                        user_name: metadata.user_name,
                        extra_properties: metadata.extra_properties,
                    });
                }
                RoomCommand::UpdateElement { user_id, element } => {
                    self.canvas_state
                        .insert(element.id().to_owned(), element.clone());
                    let _ = self
                        .event_tx
                        .send(RoomEvent::ElementUpdated { user_id, element });
                }
                RoomCommand::Leave { user_id, metadata } => {
                    self.members.remove(&user_id);
                    let _ = self.event_tx.send(RoomEvent::UserLeft {
                        user_id,
                        user_name: metadata.user_name,
                        extra_properties: metadata.extra_properties,
                    });
                }
                RoomCommand::BroadcastCursor { .. } => {}
            }
        }
        eprintln!("room actor stopped because its command channel closed");
    }
}

struct CursorActor {
    command_rx: mpsc::Receiver<RoomCommand>,
    cursor_tx: broadcast::Sender<crate::types::CursorUpdate>,
}

impl CursorActor {
    async fn run(mut self) {
        while let Some(RoomCommand::BroadcastCursor { cursor }) = self.command_rx.recv().await {
            let _ = self.cursor_tx.send(cursor);
        }
    }
}
