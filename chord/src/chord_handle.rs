use crate::chord_node::ChordNode;
use crate::chord_rpc::node_client::NodeClient;
use crate::chord_rpc::node_server::NodeServer;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use crate::chord_stabilizer::ChordStabilizer;
use crate::finger::{ChordConnection, Finger, CHORD_PORT};
use crate::finger_table::{compute_chord_id, FingerTable};
use crate::notification::chord_notification::{
    ChordNotification, ChordNotifier, TransferNotification,
};
use anyhow::Result;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio::time::interval;
use tonic::transport::{Channel, Server};
use tonic::{Request, Status};

const STABILIZE_INTERVAL: Duration = Duration::from_millis(100);

/// # Explanation
/// This struct is used to interact with the chord network. At creation it joins/create the network.
/// Then it basically provides one function. The find_node(key) function which returns the successor node of the given key.
/// This struct also provides a leave function.
///
/// #### Notes:
/// - use connection pool or finger table instead of chord_client to prevent deadlocks
/// - be more flexible (inceptor, interval duration etc)
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
        finger_table: FingerTable,
        notifier: Arc<ChordNotifier>,
    ) -> Result<Self> {
        let finger_table = Arc::new(finger_table);
        let server_shutdown = Self::start_server(finger_table.clone(), notifier.clone());

        tokio::time::sleep(Duration::from_millis(250)).await; // give the server some time to start.

        let stabilize_shutdown = Self::start_stabilize_process(finger_table);

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
        let finger_table = FingerTable::for_new_network(own_ip);
        Self::new_with_finger_table(finger_table, notifier).await
    }

    /// # Explanation
    /// This function joins the chord network the introducer is located in. It uses the introducer node to
    /// initialize the finger table. And in the end the other nodes in the network are notified.
    pub async fn join(introducer_addr: IpAddr, notifier: Arc<ChordNotifier>) -> Result<Self> {
        let finger_table = FingerTable::from_introducer(introducer_addr).await?;
        let handle = Self::new_with_finger_table(finger_table, notifier).await?;
        Ok(handle)
    }

    /// # Explanation
    /// This function starts the server of this chord node. It returns a channel that can be used to shutdown the server.
    fn start_server(
        finger_table: Arc<FingerTable>,
        notifier: Arc<ChordNotifier>,
    ) -> oneshot::Sender<()> {
        let chord_node = ChordNode::new(finger_table, notifier);
        let (server_shutdown_sender, server_shutdown_receiver) = oneshot::channel();
        tokio::task::spawn(async move {
            Server::builder()
                .add_service(NodeServer::new(chord_node))
                .serve_with_shutdown(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), CHORD_PORT),
                    async move {
                        server_shutdown_receiver.await.ok();
                        log::trace!("The chord server shutdown.");
                    },
                )
                .await
                .ok();
        });

        server_shutdown_sender
    }

    /// # Explanation
    /// This function starts the stabilize process that periodically notifies
    /// the successor of this node's existence and also updates the fingers in the finger table.
    /// This function returns a channel that can be used to shutdown the process.
    fn start_stabilize_process(finger_table: Arc<FingerTable>) -> oneshot::Sender<()> {
        let (stabilize_shutdown, mut stabilize_shutdown_receiver) = oneshot::channel();
        let stabilizer = ChordStabilizer::new(finger_table);
        tokio::task::spawn(async move {
            let mut interval = interval(STABILIZE_INTERVAL);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        stabilizer.stabilize().await.ok();
                        stabilizer.fix_fingers().await.ok();
                    },
                    _ = &mut stabilize_shutdown_receiver => {
                        break;
                    }
                }
            }

            log::trace!("The stabilize process shutdown.");
        });

        stabilize_shutdown
    }

    /// # Explanation
    /// This function can be used to leave the network. It updates the other nodes in the network,
    /// notifies the data layer that a key transfer needs to happen and shuts the server down.
    pub async fn leave(self) -> Result<()> {
        self.update_others_leave().await?;

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

    /// # Explantion
    /// This function notifies the successor and the predecessor that this node wants to leave.
    async fn update_others_leave(&self) -> Result<()> {
        let successor = self.create_successor_client().await?;
        log::trace!("While leaving the successor was {:?}.", successor.info());
        let predecessor = self.create_predecessor_client().await?;
        log::trace!(
            "While leaving the predecessor was {:?}.",
            predecessor.info()
        );
        predecessor.notify_leave(successor.info()).await?;
        successor.notify_leave(predecessor.info()).await?;

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
        let store_ip = response.into_inner().ip.parse()?;
        Ok(store_ip)
    }

    /// # Explanation
    /// This function returns the node that is responsible for storing the given key.
    /// (It basically returns the successor of the key's id.)
    pub async fn find_node(&self, key: &str) -> Result<IpAddr> {
        let key_id = compute_chord_id(key);
        log::trace!("The id of {} is {}.", key, key_id);
        self.find_node_by_id(key_id).await
    }

    /// # Explanation
    /// This function creates a gRPC client to the successor of this node.
    async fn create_successor_client(&self) -> Result<Finger> {
        let mut chord_client = self.chord_client.lock().await;
        let successor_info = chord_client
            .successor(Request::new(Empty {}))
            .await?
            .into_inner();
        let successor = Finger::from_info(successor_info)?;

        Ok(successor)
    }

    /// # Explanation
    /// This function creates a gRPC client to the predecessor of this node.
    async fn create_predecessor_client(&self) -> Result<Finger> {
        let mut chord_client = self.chord_client.lock().await;
        let predecessor_info = chord_client
            .predecessor(Request::new(Empty {}))
            .await?
            .into_inner();
        let predecessor = Finger::from_info(predecessor_info)?;

        Ok(predecessor)
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
