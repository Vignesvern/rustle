//! The room registry: the app's shared, mutable state.
//!
//! ## The Go analogy
//!
//! This is a `map[string]*Room` guarded by a `sync.RWMutex`. In Rust the compiler makes
//! the locking mandatory: the `HashMap` lives *inside* a `RwLock`, so you cannot touch it
//! without taking the lock first. `Arc<Hub>` (set up in main) is the `*Hub` shared pointer
//! every connection holds a clone of.
//!
//! Every method here is synchronous and finishes quickly — we never `.await` while holding
//! the lock, which is the cardinal rule for not deadlocking an async server.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast;

use crate::message::ServerMessage;

/// How many messages a room buffers for a slow subscriber before dropping the oldest.
const ROOM_CAPACITY: usize = 128;

/// One chat room: a broadcast channel plus its current members.
struct Room {
    tx: broadcast::Sender<ServerMessage>,
    /// Keyed by connection id (not name) so two users sharing a name are tracked separately.
    members: HashMap<u64, String>,
}

/// The registry of all live rooms.
#[derive(Default)]
pub struct Hub {
    rooms: RwLock<HashMap<String, Room>>,
    next_id: AtomicU64,
}

impl Hub {
    /// Hand out a process-unique id for a new connection.
    pub fn next_conn_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Join a room: create it if it doesn't exist, register this member, and return a
    /// subscription to the room's broadcast channel plus the updated roster.
    pub fn join(
        &self,
        room: &str,
        conn_id: u64,
        name: String,
    ) -> (broadcast::Receiver<ServerMessage>, Vec<String>) {
        let mut rooms = self.rooms.write().unwrap();
        let entry = rooms.entry(room.to_owned()).or_insert_with(|| Room {
            tx: broadcast::channel(ROOM_CAPACITY).0,
            members: HashMap::new(),
        });
        // Subscribe before returning so the caller won't miss its own join broadcast.
        let rx = entry.tx.subscribe();
        entry.members.insert(conn_id, name);
        (rx, entry.roster())
    }

    /// Remove a member. Returns the updated roster if the room still has members,
    /// or `None` if the room became empty and was pruned.
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
            // `send` errors only when there are no receivers; harmless to ignore.
            let _ = r.tx.send(msg);
        }
    }
}

impl Room {
    /// A sorted snapshot of member names for a presence update.
    fn roster(&self) -> Vec<String> {
        let mut names: Vec<String> = self.members.values().cloned().collect();
        names.sort();
        names
    }
}
