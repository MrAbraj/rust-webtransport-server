use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Element {
    pub id: String,
    pub version: u32,
    #[serde(flatten)]
    pub extra_properties: HashMap<String, rmpv::Value>,
}

impl Element {
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorUpdate {
    pub user_id: String,
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomSnapshot {
    pub user_id: String,
    pub elements: Vec<Element>,
}

#[derive(Debug)]
pub struct RoomJoinResponse {
    pub snapshot: RoomSnapshot,
    pub existing_members: Vec<RoomEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JoinMetadata {
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub user_name: String,
    #[serde(flatten)]
    pub extra_properties: HashMap<String, rmpv::Value>,
}

pub enum RoomCommand {
    Join {
        user_id: String,
        metadata: JoinMetadata,
        response_tx: tokio::sync::oneshot::Sender<Result<RoomJoinResponse, String>>,
    },
    UpdateElement {
        user_id: String,
        element: Element,
    },
    Leave {
        user_id: String,
        metadata: JoinMetadata,
    },
    BroadcastCursor {
        cursor: CursorUpdate,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RoomEvent {
    UserJoined {
        user_id: String,
        user_name: String,
        #[serde(flatten)]
        extra_properties: HashMap<String, rmpv::Value>,
    },
    UserLeft {
        user_id: String,
        user_name: String,
        #[serde(flatten)]
        extra_properties: HashMap<String, rmpv::Value>,
    },
    ElementUpdated {
        user_id: String,
        element: Element,
    },
}
