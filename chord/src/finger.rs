use crate::chord_rpc::node_client::NodeClient;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use anyhow::Result;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Status};

pub const CHORD_PORT: u16 = 32355;

/// This struct represents a chord connection. It is mainly used as a helping struct in the ChordConnectionPool struct.
///
/// It contains of a chord client as well as a sender which sends the client back to the connection pool
/// once this struct gets dropped.
///
/// Please only use the new function for creation and do not modify any of the fields because
/// if the client field is None and the objects deref function is called then the whole program panics.
pub struct ChordConnection {
    send_back: mpsc::Sender<NodeClient<Channel>>,
    client: Option<NodeClient<Channel>>,
}

impl ChordConnection {
    pub fn new(client: NodeClient<Channel>, send_back: mpsc::Sender<NodeClient<Channel>>) -> Self {
        ChordConnection {
            send_back,
            client: Some(client),
        }
    }

    /// # Explanation
    /// This function creates a client to a chord node.
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

impl Deref for ChordConnection {
    type Target = NodeClient<Channel>;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().unwrap()
    }
}

impl DerefMut for ChordConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().unwrap()
    }
}

impl Drop for ChordConnection {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // send the client back to the connection pool
            let send_back = self.send_back.clone();
            tokio::task::spawn(async move {
                send_back.send(client).await.ok();
            });
        }
    }
}

/// # Explanation
/// The ChordConnectionPool is a struct that creates new connections/clients to the specified Chord node only on demand.
/// Each connection is returned to the pool once it is no longer in use.
#[derive(Clone)]
pub struct ChordConnectionPool {
    ip: IpAddr,
    send_back: mpsc::Sender<NodeClient<Channel>>,
    queue: Arc<Mutex<VecDeque<NodeClient<Channel>>>>,
}

impl ChordConnectionPool {
    pub fn new(ip: IpAddr) -> Self {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let (send_back, mut back_receiver) = mpsc::channel(32);

        let queue_clone = queue.clone();
        tokio::task::spawn(async move {
            // wait for clients that are sent back and add these back to the pool
            while let Some(client) = back_receiver.recv().await {
                let mut queue_lock = queue_clone.lock().await;
                if queue_lock.len() < 10 {
                    queue_lock.push_back(client);
                }
            }
        });

        ChordConnectionPool {
            ip,
            send_back,
            queue,
        }
    }

    /// # Explanation
    /// This function either pops one connection from the pool or creates a new one.
    pub async fn get_connection(&self) -> Result<ChordConnection, Status> {
        let optional_client = {
            let mut queue = self.queue.lock().await;
            queue.pop_front()
        };

        let client = if let Some(client) = optional_client {
            client
        } else {
            ChordConnection::create_chord_client(self.ip)
                .await
                .map_err(|_| {
                    Status::aborted(format!(
                        "Was not able to create a connection to the specified ip ({}).",
                        self.ip
                    ))
                })?
        };

        Ok(ChordConnection::new(client, self.send_back.clone()))
    }
}

#[derive(Clone)]
pub struct Finger {
    ip: IpAddr,
    id: u64,
    pool: ChordConnectionPool,
}

impl Finger {
    pub fn new(ip: IpAddr, id: u64) -> Self {
        let pool = ChordConnectionPool::new(ip);
        Finger { ip, id, pool }
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

    /// # Explanation
    /// This function checks if the node still exists and if the correct id is returned.
    pub async fn check(&self) -> bool {
        let connection = self.pool.get_connection().await;
        if let Ok(mut connection) = connection {
            let response = connection.get_id(Request::new(Empty {})).await;
            response.is_ok() && response.unwrap().into_inner().id == self.id
        } else {
            false
        }
    }

    /// # Explanation
    /// This function sends a closest_preceding_finger-request to the node.
    pub async fn closest_preceding_finger(&self, id: u64) -> Result<NodeInfo, Status> {
        let mut connection = self.pool.get_connection().await?;
        let response = connection
            .closest_preceding_finger(Request::new(Identifier { id }))
            .await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function sends a find_successor-request to the node.
    pub async fn find_successor(&self, id: u64) -> Result<NodeInfo, Status> {
        let mut connection = self.pool.get_connection().await?;
        let response = connection
            .find_successor(Request::new(Identifier { id }))
            .await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function sends a successor-request to the node.
    pub async fn successor(&self) -> Result<NodeInfo, Status> {
        let mut connection = self.pool.get_connection().await?;
        let response = connection.successor(Request::new(Empty {})).await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function sends a predecessor-request to the node.
    pub async fn predecessor(&self) -> Result<NodeInfo, Status> {
        let mut connection = self.pool.get_connection().await?;
        let response = connection.predecessor(Request::new(Empty {})).await?;
        Ok(response.into_inner())
    }

    /// # Explanation
    /// This function sends a notify-request to the node.
    pub async fn notify(&self, own_id: u64) -> Result<(), Status> {
        let mut connection = self.pool.get_connection().await?;
        let _ = connection
            .notify(Request::new(Identifier { id: own_id }))
            .await?;
        Ok(())
    }

    /// # Explanation
    /// This function sends a notify_leave-request to the node.
    pub async fn notify_leave(&self, info: NodeInfo) -> Result<(), Status> {
        let mut connection = self.pool.get_connection().await?;
        let _ = connection.notify_leave(Request::new(info)).await?;
        Ok(())
    }
}
