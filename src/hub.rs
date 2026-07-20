//! The room registry: the app's shared, mutable state.
//!
//! ## The Go analogy
//!
//! This is a `map[string]*Room` guarded by a `sync.RWMutex`. In Rust the compiler makes
//! the locking mandatory: the `HashMap` lives *inside* a `RwLock`, so you cannot touch it
//! without taking the lock first. `Arc<Hub>` (set up in `AppState`) is the `*Hub` shared
//! pointer every connection holds a clone of.
//!
//! Every method here is synchronous and finishes quickly — we never `.await` while holding
//! the lock, which is the cardinal rule for not deadlocking an async server.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use tokio::sync::broadcast;

use crate::message::ServerMessage;

/// One chat room: a broadcast channel plus its current members.
struct Room {
    tx: broadcast::Sender<ServerMessage>,
    /// Keyed by connection id (not name) so two users sharing a name stay distinct.
    members: HashMap<u64, String>,
}

/// A room's name and member count, for the lobby API.
#[derive(Debug, Serialize)]
pub struct RoomSummary {
    pub name: String,
    pub count: usize,
}

/// The registry of all live rooms.
pub struct Hub {
    rooms: RwLock<HashMap<String, Room>>,
    next_id: AtomicU64,
    /// Per-room broadcast buffer capacity.
    capacity: usize,
}

impl Hub {
    pub fn new(capacity: usize) -> Self {
        Self {
            rooms: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            capacity,
        }
    }

    /// Hand out a process-unique id for a new connection.
    pub fn next_conn_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Join a room: create it if needed, register this member, and return a subscription
    /// plus the updated roster.
    pub fn join(
        &self,
        room: &str,
        conn_id: u64,
        name: String,
    ) -> (broadcast::Receiver<ServerMessage>, Vec<String>) {
        let capacity = self.capacity;
        let mut rooms = self.rooms.write().unwrap();
        let entry = rooms.entry(room.to_owned()).or_insert_with(|| Room {
            tx: broadcast::channel(capacity).0,
            members: HashMap::new(),
        });
        // Subscribe before returning so the caller won't miss its own join broadcast.
        let rx = entry.tx.subscribe();
        entry.members.insert(conn_id, name);
        (rx, entry.roster())
    }

    /// Remove a member. Returns the updated roster if the room survives, or `None` if it
    /// became empty and was pruned.
    pub fn leave(&self, room: &str, conn_id: u64) -> Option<Vec<String>> {
        let mut rooms = self.rooms.write().unwrap();
        let r = rooms.get_mut(room)?;
        r.members.remove(&conn_id);
        if r.members.is_empty() {
            rooms.remove(room);
            None
        } else {
            Some(r.roster())
        }
    }

    /// Broadcast a message to everyone in a room (a no-op if the room is gone).
    pub fn broadcast(&self, room: &str, msg: ServerMessage) {
        let rooms = self.rooms.read().unwrap();
        if let Some(r) = rooms.get(room) {
            let _ = r.tx.send(msg);
        }
    }

    /// All active rooms with member counts, sorted by name (for the lobby API).
    pub fn rooms_summary(&self) -> Vec<RoomSummary> {
        let rooms = self.rooms.read().unwrap();
        let mut out: Vec<RoomSummary> = rooms
            .iter()
            .map(|(name, r)| RoomSummary {
                name: name.clone(),
                count: r.members.len(),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// The roster of a single room, or `None` if it doesn't exist.
    pub fn room_roster(&self, room: &str) -> Option<Vec<String>> {
        let rooms = self.rooms.read().unwrap();
        rooms.get(room).map(Room::roster)
    }
}

impl Room {
    /// A sorted snapshot of member names.
    fn roster(&self) -> Vec<String> {
        let mut names: Vec<String> = self.members.values().cloned().collect();
        names.sort();
        names
    }
}
