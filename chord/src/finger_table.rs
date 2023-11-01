use crate::chord_rpc::NodeInfo;
use crate::finger::Finger;
use anyhow::Result;
use rand::random;
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use tokio::sync::Mutex;

pub type ChordId = u64;
pub const CHORD_ID_BITSIZE: usize = 64;

pub struct FingerTable {
    own_info: NodeInfo,
    predecessor: Mutex<Option<Finger>>,
    table: Vec<Mutex<Option<Finger>>>,
}

impl FingerTable {
    pub fn new(
        own_info: NodeInfo,
        predecessor: Mutex<Option<Finger>>,
        table: Vec<Mutex<Option<Finger>>>,
    ) -> Self {
        FingerTable {
            own_info,
            predecessor,
            table,
        }
    }

    pub fn for_new_network(own_info: NodeInfo) -> Result<Self> {
        let own_finger = Finger::from_info(own_info.clone())?;
        Ok(Self::from_successor(own_finger, own_info))
    }

    /// # Explanation
    /// This function creates a finger table for the first node in a new network.
    /// (The successor is set to this.)
    pub fn for_new_network_with_random_id(own_addr: SocketAddr) -> Result<Self> {
        let own_id = random();
        log::trace!("The own id is {}.", own_id);
        Self::for_new_network(NodeInfo {
            ip: own_addr.ip().to_string(),
            port: own_addr.port() as u32,
            id: own_id,
        })
    }

    pub async fn from_introducer(introducer_addr: SocketAddr, own_info: NodeInfo) -> Result<Self> {
        let introducer = Finger::new(introducer_addr, 0); // id does not matter
        let successor_info = introducer.find_successor(own_info.id).await?;
        let successor = Finger::from_info(successor_info)?;

        log::trace!(
            "Initialized the finger table with {:?} as the successor.",
            successor.info()
        );

        Ok(Self::from_successor(successor, own_info))
    }

    /// # Explanation
    /// This function creates a finger table from an introducing node in the network this node should join.
    /// The id of this node is randomly select.
    pub async fn from_introducer_with_random_id(
        introducer_addr: SocketAddr,
        own_addr: SocketAddr,
    ) -> Result<Self> {
        let own_id = random();
        log::trace!("The own id is {}.", own_id);
        Self::from_introducer(
            introducer_addr,
            NodeInfo {
                ip: own_addr.ip().to_string(),
                port: own_addr.port() as u32,
                id: own_id,
            },
        )
        .await
    }

    fn from_successor(successor_finger: Finger, own_info: NodeInfo) -> Self {
        let successor = Mutex::new(Some(successor_finger));
        let predecessor = Mutex::new(None);

        let mut table: Vec<Mutex<Option<Finger>>> = std::iter::repeat_with(|| Mutex::new(None))
            .take(CHORD_ID_BITSIZE)
            .collect();
        table[0] = successor;

        FingerTable::new(own_info, predecessor, table)
    }

    /// # Explanation
    /// This function returns this node's id.
    pub fn own_id(&self) -> ChordId {
        self.own_info().id
    }

    pub fn own_info(&self) -> NodeInfo {
        self.own_info.clone()
    }

    pub fn own_addr(&self) -> Result<SocketAddr> {
        Ok(SocketAddr::new(
            self.own_info().ip.parse()?,
            self.own_info().port as u16,
        ))
    }

    /// # Explanation
    /// The check_fingers function performs a check for each node in which it is checked
    /// if the node is alive and returns the correct id.
    ///
    /// Any node that fails the check is removed from the finger table.
    pub async fn check_fingers(&self) {
        let predecessor = self.predecessor().await;
        if !Self::check_finger(predecessor).await {
            self.update_predecessor(None).await;
        }

        for i in 0..CHORD_ID_BITSIZE {
            let finger = self.get_finger(i).await;
            if !Self::check_finger(finger).await {
                self.update_finger(i, None).await;
            }
        }
    }

    async fn check_finger(finger: Option<Finger>) -> bool {
        if let Some(finger) = finger {
            finger.check().await
        } else {
            true
        }
    }

    /// # Explanation
    /// Updates the successor to the passed finger.
    pub async fn update_successor(&self, successor: Option<Finger>) {
        self.update_finger(0, successor).await
    }

    /// # Returns
    /// Returns a mutex guard of the successor finger.
    pub async fn successor(&self) -> Option<Finger> {
        self.get_finger(0).await
    }

    /// # Explanation
    /// Updates the predecessor to the passed finger.
    pub async fn update_predecessor(&self, predecessor: Option<Finger>) {
        let mut old_predecessor = self.predecessor.lock().await;
        let _ = std::mem::replace(&mut *old_predecessor, predecessor);
    }

    /// # Returns
    /// Returns a mutex guard of the predecessor finger.
    pub async fn predecessor(&self) -> Option<Finger> {
        self.predecessor.lock().await.clone()
    }

    /// # Explanation
    /// Updates the ith finger to the passed finger.
    pub async fn update_finger(&self, i: usize, finger: Option<Finger>) {
        let mut old_finger = self.table[i].lock().await;
        let _ = std::mem::replace(&mut *old_finger, finger);
    }

    /// # Returns
    /// Returns a mutex guard of the ith finger (i is not used currently).
    pub async fn get_finger(&self, i: usize) -> Option<Finger> {
        self.table[i].lock().await.clone()
    }
}

/// # Explanation
/// This function computes the id in the chord ring of a string.
/// (make sure that this function is consistent in the entire network)
pub fn compute_chord_id(s: &str) -> ChordId {
    let mut sha1 = Sha1::default();
    sha1.update(s);
    let hash = sha1.finalize();

    ChordId::from_be_bytes([
        hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
    ])
}

/// # Explanation
/// This function checks if x is in the ring interval of left and right.
/// In order to control if the interval borders are open or closed
/// there need to be passed two addtional parameters (exclusive_left and exclusive_right).
pub fn in_ring_interval(
    x: ChordId,
    left: ChordId,
    right: ChordId,
    exclusive_left: bool,
    exclusive_right: bool,
) -> bool {
    let left = left.wrapping_add(exclusive_left as ChordId);
    let right = right.wrapping_sub(exclusive_right as ChordId);

    if left <= right {
        left <= x && x <= right
    } else {
        left <= x || x <= right
    }
}

/// # Explanation
/// This function checks if x is in the open ring interval (left, right).
pub fn in_ring_interval_exclusive(x: ChordId, left: ChordId, right: ChordId) -> bool {
    in_ring_interval(x, left, right, true, true)
}

/// # Explanation
/// This function checks if x is in the closed ring interval [left, right].
pub fn in_ring_interval_inclusive(x: ChordId, left: ChordId, right: ChordId) -> bool {
    in_ring_interval(x, left, right, false, false)
}

/// # Explanation
/// This function checks if x is in the store interval (so the left border is open and the right one is closed).
/// This function can be used to check if a node that has the id right and a predecessor with the id left should store x.
pub fn in_store_interval(x: ChordId, left: ChordId, right: ChordId) -> bool {
    in_ring_interval(x, left, right, true, false)
}
