use crate::chord_node::ChordNode;
use crate::chord_rpc::node_client::NodeClient;
use crate::chord_rpc::node_server::NodeServer;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use crate::finger_table::{compute_chord_id, ChordConnection, Finger, FingerTable, CHORD_PORT};
use crate::notification::chord_notification::{
    ChordNotification, ChordNotifier, TransferNotification,
};
use anyhow::{anyhow, Result};
use rand::random;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tonic::transport::{Channel, Server};
use tonic::{transport, Request};

/// # Explanation
/// This struct is used to interact with the chord network. At creation it joins/create the network.
/// Then it basically provides one function. The find_node(key) function which returns the successor node of the given key.
/// This struct also provides a leave function.
pub struct ChordHandle {
    own_id: u64,
    chord_client: Mutex<NodeClient<Channel>>,
    notifier: Arc<ChordNotifier>,
    server_shutdown: oneshot::Sender<()>,
}

impl ChordHandle {
    /// # Explanation
    /// This function creates a chord node with the given id and initializes the node with the given finger table.
    pub async fn new_with_finger_table(
        own_id: u64,
        finger_table: FingerTable,
        notifier: Arc<ChordNotifier>,
    ) -> Result<Self> {
        let server_shutdown = Self::start_server(own_id, finger_table, notifier.clone()).await?;

        tokio::time::sleep(Duration::from_secs(1)).await; // give the server some time to start.
        let chord_client =
            ChordConnection::create_chord_client(IpAddr::V4(Ipv4Addr::LOCALHOST)).await?;
        let handle = ChordHandle {
            own_id,
            chord_client: Mutex::new(chord_client),
            notifier,
            server_shutdown,
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
        let finger_table = FingerTable::from_info_without_connection(own_info.clone(), own_info)?;

        Self::new_with_finger_table(own_id, finger_table, notifier).await
    }

    /// # Explanation
    /// This function joins the chord network the introducer is located in. It uses the introducer node to
    /// initialize the finger table. And in the end the other nodes in the network are notified.
    pub async fn join(introducer_addr: IpAddr, notifier: Arc<ChordNotifier>) -> Result<Self> {
        let own_id: u64 = random();
        let mut introducer = Finger::connect(introducer_addr, 0).await; // id does not matter
        let finger_table = Self::init_finger_table(&mut introducer, own_id).await?;

        notifier
            .notify(ChordNotification::DataFrom(
                TransferNotification::allow_all(
                    finger_table
                        .successor()
                        .await
                        .ok_or(anyhow!("There existis no successor."))?
                        .ip(),
                ),
            ))
            .await; // this should belong to stabilize later

        let handle = Self::new_with_finger_table(own_id, finger_table, notifier).await?;
        handle.update_others_join().await?;
        Ok(handle)
    }

    /// # Explanation
    /// This function starts the server of this chord node. It returns a channel that can be used to shutdown the server.
    async fn start_server(
        own_id: u64,
        finger_table: FingerTable,
        notifier: Arc<ChordNotifier>,
    ) -> Result<oneshot::Sender<()>, transport::Error> {
        let chord_node = ChordNode::new(own_id, finger_table, notifier);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        tokio::task::spawn(async move {
            Server::builder()
                .add_service(NodeServer::new(chord_node))
                .serve_with_shutdown(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), CHORD_PORT),
                    async move {
                        shutdown_receiver.await.ok();
                    },
                )
                .await
                .ok();
        });

        Ok(shutdown_sender)
    }

    /// # Explanation
    /// This function initializes the finger table of a node with the given id and
    /// it uses the introducer node to accomplish that.
    async fn init_finger_table(introducer: &mut Finger, id: u64) -> Result<FingerTable> {
        let successor_info = introducer.find_successor(id).await?;
        let predecessor_info = introducer.find_predecessor(id).await?;

        FingerTable::from_info(successor_info, predecessor_info).await
    }

    /// # Explanation
    /// This function is used to update the other nodes in the network after this node is joined.
    async fn update_others_join(&self) -> Result<()> {
        let mut successor = self.create_successor_client().await?;
        successor.update_predecessor(None, self.own_id).await?;
        let mut predecessor = self.create_predecessor_client().await?;
        predecessor
            .update_finger_table(0, None, self.own_id)
            .await?;

        Ok(())
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
        self.server_shutdown.send(()).ok();
        Ok(())
    }

    /// # Explanation
    /// This function is used to update the other nodes in the network after this node left.
    async fn update_others_leave(&self) -> Result<()> {
        let mut successor = self.create_successor_client().await?;
        let mut predecessor = self.create_predecessor_client().await?;
        successor
            .update_predecessor(Some(predecessor.ip()), predecessor.id())
            .await?;
        predecessor
            .update_finger_table(0, Some(successor.ip()), successor.id())
            .await?;

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
    /// This function creates a gRPC client to the successor of this node.
    async fn create_successor_client(&self) -> Result<Finger> {
        let mut chord_client = self.chord_client.lock().await;
        let successor_info = chord_client
            .successor(Request::new(Empty {}))
            .await?
            .into_inner();
        let successor = Finger::from_info(successor_info).await?;

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
        let predecessor = Finger::from_info(predecessor_info).await?;

        Ok(predecessor)
    }

    /// # Explanation
    /// This function returns the info (ip and id) of this node's successor.
    async fn get_successor_info(&self) -> Result<NodeInfo> {
        let mut chord_client = self.chord_client.lock().await;
        let successor_info = chord_client
            .successor(Request::new(Empty {}))
            .await?
            .into_inner();

        Ok(successor_info)
    }
}
