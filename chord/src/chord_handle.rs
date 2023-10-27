use crate::chord_node::ChordNode;
use crate::chord_rpc::node_client::NodeClient;
use crate::chord_rpc::node_server::NodeServer;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use crate::chord_stabilizer::ChordStabilizer;
use crate::finger_table::{compute_chord_id, ChordConnection, Finger, FingerTable, CHORD_PORT};
use crate::notification::chord_notification::{
    ChordNotification, ChordNotifier, TransferNotification,
};
use anyhow::Result;
use rand::random;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio::time::Instant;
use tonic::transport::{Channel, Server};
use tonic::{Request, Status};

/// # Explanation
/// This struct is used to interact with the chord network. At creation it joins/create the network.
/// Then it basically provides one function. The find_node(key) function which returns the successor node of the given key.
/// This struct also provides a leave function.
pub struct ChordHandle {
    chord_client: Mutex<NodeClient<Channel>>,
    notifier: Arc<ChordNotifier>,
    server_shutdown: oneshot::Sender<()>,
    stabilize_shutdown: oneshot::Sender<()>,
}

impl ChordHandle {
    /// # Explanation
    /// This function creates a chord node with the given id and initializes the node with the given finger table.
    pub async fn new_with_finger_table(
        own_id: u64,
        finger_table: FingerTable,
        notifier: Arc<ChordNotifier>,
    ) -> Result<Self> {
        let finger_table = Arc::new(finger_table);
        let server_shutdown = Self::start_server(own_id, finger_table.clone(), notifier.clone());

        tokio::time::sleep(Duration::from_millis(250)).await; // give the server some time to start.

        let stabilize_shutdown = Self::start_stabilize_process(own_id, finger_table);

        let chord_client =
            ChordConnection::create_chord_client(IpAddr::V4(Ipv4Addr::LOCALHOST)).await?;
        let handle = ChordHandle {
            chord_client: Mutex::new(chord_client),
            notifier,
            server_shutdown,
            stabilize_shutdown,
        };
        Ok(handle)
    }

    /// # Explanation
    /// This function creates a new chord network. This works by initializing the finger table entries with the own node.
    /// The node's id is generated randomly.
    pub async fn new_network(own_ip: IpAddr, notifier: Arc<ChordNotifier>) -> Result<Self> {
        let own_id: u64 = random();
        let own_info = NodeInfo {
            ip: own_ip.to_string(),
            id: own_id,
        };
        let finger_table = FingerTable::from_successor(own_info)?;

        Self::new_with_finger_table(own_id, finger_table, notifier).await
    }

    /// # Explanation
    /// This function joins the chord network the introducer is located in. It uses the introducer node to
    /// initialize the finger table. And in the end the other nodes in the network are notified.
    pub async fn join(introducer_addr: IpAddr, notifier: Arc<ChordNotifier>) -> Result<Self> {
        let own_id: u64 = random();
        let mut introducer = Finger::new(introducer_addr, 0); // id does not matter
        let finger_table = Self::init_finger_table(&mut introducer, own_id).await?;

        let handle = Self::new_with_finger_table(own_id, finger_table, notifier).await?;
        Ok(handle)
    }

    /// # Explanation
    /// This function starts the server of this chord node. It returns a channel that can be used to shutdown the server.
    fn start_server(
        own_id: u64,
        finger_table: Arc<FingerTable>,
        notifier: Arc<ChordNotifier>,
    ) -> oneshot::Sender<()> {
        let chord_node = ChordNode::new(own_id, finger_table, notifier);
        let (server_shutdown_sender, server_shutdown_receiver) = oneshot::channel();
        tokio::task::spawn(async move {
            Server::builder()
                .add_service(NodeServer::new(chord_node))
                .serve_with_shutdown(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), CHORD_PORT),
                    async move {
                        server_shutdown_receiver.await.ok();
                    },
                )
                .await
                .ok();
        });

        server_shutdown_sender
    }

    fn start_stabilize_process(own_id: u64, finger_table: Arc<FingerTable>) -> oneshot::Sender<()> {
        let (stabilize_shutdown, mut stabilize_shutdown_receiver) = oneshot::channel();
        let stabilizer = ChordStabilizer::new(own_id, finger_table);
        tokio::task::spawn(async move {
            let interval_duration = Duration::from_millis(250);
            let mut old_time = Instant::now();
            loop {
                let now_time = Instant::now();
                let work_duration = now_time - old_time;
                if work_duration < interval_duration {
                    let sleep_duration = interval_duration - work_duration;
                    tokio::time::sleep(sleep_duration).await;
                    old_time = now_time;
                }

                tokio::select! {
                    _ = &mut stabilize_shutdown_receiver => {
                        break;
                    },
                    else => {
                        stabilizer.stabilize().await.ok();
                        stabilizer.fix_fingers().await.ok();
                    }
                }
            }
        });

        stabilize_shutdown
    }

    /// # Explanation
    /// This function initializes the finger table of a node with the given id and
    /// it uses the introducer node to accomplish that.
    async fn init_finger_table(introducer: &mut Finger, id: u64) -> Result<FingerTable> {
        let successor_info = introducer.find_successor(id).await?;

        FingerTable::from_successor(successor_info)
    }

    /// # Explanation
    /// This function can be used to leave the network. It updates the other nodes in the network,
    /// notifies the data layer that a key transfer needs to happen and shuts the server down.
    pub async fn leave(self) -> Result<()> {
        // instead of updating the other nodes just leave - the finger table needs the r successors

        let successor_ip = self.get_successor_info().await?.ip.parse()?;
        self.notifier
            .notify(ChordNotification::DataTo(TransferNotification::allow_all(
                successor_ip,
            )))
            .await;

        self.notifier.finalize().await;
        self.stabilize_shutdown.send(()).ok();
        self.server_shutdown.send(()).ok();
        Ok(())
    }

    /// # Explanation
    /// This function returns the node that is responsible for storing the key that has the given id.
    /// (It basically returns the successor of id.)
    pub async fn find_node_by_id(&self, id: u64) -> Result<IpAddr> {
        let mut chord_client = self.chord_client.lock().await;
        let response = chord_client
            .find_successor(Request::new(Identifier { id }))
            .await?;
        let store_node = response.into_inner().ip.parse()?;
        Ok(store_node)
    }

    /// # Explanation
    /// This function returns the node that is responsible for storing the given key.
    /// (It basically returns the successor of the key's id.)
    pub async fn find_node(&self, key: &str) -> Result<IpAddr> {
        self.find_node_by_id(compute_chord_id(key)).await
    }

    /// # Explanation
    /// This function returns the info (ip and id) of this node's successor.
    async fn get_successor_info(&self) -> Result<NodeInfo, Status> {
        let mut chord_client = self.chord_client.lock().await;
        let successor_info = chord_client
            .successor(Request::new(Empty {}))
            .await?
            .into_inner();

        Ok(successor_info)
    }
}
