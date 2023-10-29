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
            let send_back = self.send_back.clone();
            tokio::task::spawn(async move {
                send_back.send(client).await.ok();
            });
        }
    }
}

#[derive(Clone)]
pub struct ConnectionPool {
    ip: IpAddr,
    send_back: mpsc::Sender<NodeClient<Channel>>,
    queue: Arc<Mutex<VecDeque<NodeClient<Channel>>>>,
}

impl ConnectionPool {
    pub fn new(ip: IpAddr) -> Self {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let (send_back, mut back_receiver) = mpsc::channel(32);

        let queue_clone = queue.clone();
        tokio::task::spawn(async move {
            while let Some(client) = back_receiver.recv().await {
                queue_clone.lock().await.push_back(client);
            }
        });

        ConnectionPool {
            ip,
            send_back,
            queue,
        }
    }

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
                .map_err(|_| Status::aborted(""))?
        };

        Ok(ChordConnection::new(client, self.send_back.clone()))
    }
}

#[derive(Clone)]
pub struct Finger {
    ip: IpAddr,
    id: u64,
    pool: ConnectionPool,
}

impl Finger {
    pub fn new(ip: IpAddr, id: u64) -> Self {
        let pool = ConnectionPool::new(ip);
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

    pub async fn check(&self) -> bool {
        let connection = self.pool.get_connection().await;
        if let Ok(mut connection) = connection {
            let response = connection.get_id(Request::new(Empty {})).await;
            response.is_ok() && response.unwrap().into_inner().id == self.id
        } else {
            false
        }
    }

    pub async fn closest_preceding_finger(&self, id: u64) -> Result<NodeInfo, Status> {
        let mut connection = self.pool.get_connection().await?;
        let response = connection
            .closest_preceding_finger(Request::new(Identifier { id }))
            .await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function performs a find_successor-request on the client.
    pub async fn find_successor(&self, id: u64) -> Result<NodeInfo, Status> {
        let mut connection = self.pool.get_connection().await?;
        let response = connection
            .find_successor(Request::new(Identifier { id }))
            .await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function performs a successor-request on the client.
    pub async fn successor(&self) -> Result<NodeInfo, Status> {
        let mut connection = self.pool.get_connection().await?;
        let response = connection.successor(Request::new(Empty {})).await?;
        Ok(response.into_inner())
    }

    /// # Explantion
    /// This function performs a predecessor-request on the client.
    pub async fn predecessor(&self) -> Result<NodeInfo, Status> {
        let mut connection = self.pool.get_connection().await?;
        let response = connection.predecessor(Request::new(Empty {})).await?;
        Ok(response.into_inner())
    }

    pub async fn notify(&self, own_id: u64) -> Result<(), Status> {
        let mut connection = self.pool.get_connection().await?;
        let _ = connection
            .notify(Request::new(Identifier { id: own_id }))
            .await?;
        Ok(())
    }

    pub async fn notify_leave(&self, info: NodeInfo) -> Result<(), Status> {
        let mut connection = self.pool.get_connection().await?;
        let _ = connection.notify_leave(Request::new(info)).await?;
        Ok(())
    }
}
