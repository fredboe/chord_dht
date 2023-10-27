use crate::chord_rpc::node_client::NodeClient;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use anyhow::Result;
use sha1::{Digest, Sha1};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Status};

pub const CHORD_PORT: u16 = 32355;

pub struct ChordConnection {
    ip: IpAddr,
    client: Option<NodeClient<Channel>>,
}

impl ChordConnection {
    pub fn new(ip: IpAddr) -> Self {
        ChordConnection { ip, client: None }
    }

    pub async fn client(&mut self) -> Result<&mut NodeClient<Channel>, Status> {
        if self.client.is_none() {
            let client = Self::create_chord_client(self.ip).await.ok();
            self.client = client;
        }

        self.client.as_mut().ok_or(Status::aborted(
            "Was not able to connect to the chord node.",
        ))
    }

    pub async fn create_chord_client(ip: IpAddr) -> Result<NodeClient<Channel>> {
        let uri = Uri::builder()
            .scheme("http")
            .authority(format!("{}:{}", ip, CHORD_PORT))
            .path_and_query("/")
            .build()?;
        let client = timeout(Duration::from_secs(1), NodeClient::connect(uri)).await??;

        Ok(client)
    }
}

#[derive(Clone)]
pub struct Finger {
    ip: IpAddr,
    id: u64,
    connection: Arc<Mutex<ChordConnection>>,
}

impl Finger {
    pub fn new(ip: IpAddr, id: u64) -> Self {
        let connection = Arc::new(Mutex::new(ChordConnection::new(ip)));
        Finger { ip, id, connection }
    }

    /// # Explanation
    /// This function create the finger from a node info (ip and id).
    /// An error is returned if the given ip address is not a correct one.
    pub fn from_info(info: NodeInfo) -> Result<Self, Status> {
        let ip = info
            .ip
            .parse()
            .map_err(|_| Status::cancelled("Not a correct ip address was given."))?;
        let id = info.id;
        Ok(Self::new(ip, id))
    }

    /// # Returns
    /// Returns the node's id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// # Returns
    /// Returns the node's ip address.
    pub fn ip(&self) -> IpAddr {
        self.ip
    }

    /// # Returns
    /// Returns the node's info (ip as a string and the id).
    pub fn info(&self) -> NodeInfo {
        NodeInfo {
            ip: self.ip.to_string(),
            id: self.id,
        }
    }

    pub async fn check(&self) -> bool {
        let mut connection = self.connection.lock().await;
        if let Ok(client) = connection.client().await {
            let response = client.get_id(Request::new(Empty {})).await;
            response.is_ok() && response.unwrap().into_inner().id == self.id
        } else {
            false
        }
    }

    /// # Explantion
    /// This function performs a find_successor-request on the client.
    pub async fn find_successor(&self, id: u64) -> Result<NodeInfo, Status> {
        let mut connection = self.connection.lock().await;
        let client = connection.client().await?;
        let response = client
            .find_successor(Request::new(Identifier { id }))
            .await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function performs a find_predecessor-request on the client.
    pub async fn find_predecessor(&self, id: u64) -> Result<NodeInfo, Status> {
        let mut connection = self.connection.lock().await;
        let client = connection.client().await?;
        let response = client
            .find_predecessor(Request::new(Identifier { id }))
            .await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function performs a successor-request on the client.
    pub async fn successor(&self) -> Result<NodeInfo, Status> {
        let mut connection = self.connection.lock().await;
        let client = connection.client().await?;
        let response = client.successor(Request::new(Empty {})).await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function performs a predecessor-request on the client.
    pub async fn predecessor(&self) -> Result<NodeInfo, Status> {
        let mut connection = self.connection.lock().await;
        let client = connection.client().await?;
        let response = client.predecessor(Request::new(Empty {})).await?;
        Ok(response.into_inner())
    }

    pub async fn notify(&self, own_id: u64) -> Result<(), Status> {
        let mut connection = self.connection.lock().await;
        let client = connection.client().await?;
        let _ = client
            .notify(Request::new(Identifier { id: own_id }))
            .await?;
        Ok(())
    }
}

pub struct FingerTable {
    predecessor: Mutex<Option<Finger>>,
    table: Vec<Mutex<Option<Finger>>>,
}

impl FingerTable {
    pub fn new(predecessor: Mutex<Option<Finger>>, table: Vec<Mutex<Option<Finger>>>) -> Self {
        FingerTable { predecessor, table }
    }

    pub fn from_successor(successor_info: NodeInfo) -> Result<Self> {
        let successor = Mutex::new(Some(Finger::from_info(successor_info)?));
        let predecessor = Mutex::new(None);

        let mut table: Vec<Mutex<Option<Finger>>> = std::iter::repeat_with(|| Mutex::new(None))
            .take(64)
            .collect();
        table[0] = successor;

        Ok(FingerTable::new(predecessor, table))
    }

    pub async fn check_fingers(&self) {
        let predecessor = self.predecessor().await;
        if !Self::check_finger(predecessor).await {
            self.update_predecessor(None).await;
        }

        for i in 0..64 {
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

    pub async fn update_successor(&self, successor: Option<Finger>) {
        self.update_finger(0, successor).await
    }

    /// # Returns
    /// Returns a mutex guard of the successor finger.
    pub async fn successor(&self) -> Option<Finger> {
        self.get_finger(0).await
    }

    pub async fn update_predecessor(&self, predecessor: Option<Finger>) {
        let mut old_predecessor = self.predecessor.lock().await;
        let _ = std::mem::replace(&mut *old_predecessor, predecessor);
    }

    /// # Returns
    /// Returns a mutex guard of the predecessor finger.
    pub async fn predecessor(&self) -> Option<Finger> {
        self.predecessor.lock().await.clone()
    }

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
pub fn compute_chord_id(s: &str) -> u64 {
    let mut sha1 = Sha1::default();
    sha1.update(s);
    let hash = sha1.finalize();

    u64::from_be_bytes([
        hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
    ])
}

/// # Explanation
/// This function checks if x is in the ring interval of left and right.
/// In order to control if the interval borders are open or closed
/// there need to be passed two addtional parameters (exclusive_left and exclusive_right).
pub fn in_ring_interval(
    x: u64,
    left: u64,
    right: u64,
    exclusive_left: bool,
    exclusive_right: bool,
) -> bool {
    let left = left.wrapping_add(exclusive_left as u64);
    let right = right.wrapping_sub(exclusive_right as u64);

    if left <= right {
        left <= x && x <= right
    } else {
        left <= x || x <= right
    }
}

/// # Explanation
/// This function checks if x is in the open ring interval (left, right).
pub fn in_ring_interval_exclusive(x: u64, left: u64, right: u64) -> bool {
    in_ring_interval(x, left, right, true, true)
}

/// # Explanation
/// This function checks if x is in the closed ring interval [left, right].
pub fn in_ring_interval_inclusive(x: u64, left: u64, right: u64) -> bool {
    in_ring_interval(x, left, right, false, false)
}

/// # Explanation
/// This function checks if x is in the store interval (so the left border is open and the right one is closed).
/// This function can be used to check if a node that has the id right and a predecessor with the id left should store x.
pub fn in_store_interval(x: u64, left: u64, right: u64) -> bool {
    in_ring_interval(x, left, right, true, false)
}
