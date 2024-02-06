use crate::chord_node::ChordNode;
use crate::chord_rpc::node_server::NodeServer;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use crate::chord_stabilizer::ChordStabilizer;
use crate::finger::{ChordConnectionPool, Finger};
use crate::finger_table::{compute_chord_id, ChordId, FingerTable};
use crate::notification::chord_notification::{
    ChordNotification, ChordNotifier, TransferNotification,
};
use anyhow::Result;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::Request;

/// # Explanation
/// Provides an interface for interacting with a chord network, facilitating network participation and key-based node lookup.
///
/// Upon instantiation, this struct either joins an existing chord network or initiates a new one.
/// The primary outside functionality is the `find_node(key)` method, which locates and returns the successor node responsible for the specified key.
/// Most of the interesting stuff is happening in the background where the actual chord node is running.
///
/// Additionally, a `leave` method is provided to gracefully exit the network. This feature allows for the clean shutdown and
/// removal of the current node from the network, ensuring network stability and data integrity during node departures.

pub struct ChordHandle {
    chord_connection: ChordConnectionPool,
    notifier: Arc<ChordNotifier>,
    stabilize_process: ChordStabilizer,
    server_shutdown: oneshot::Sender<()>,
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

        let stabilize_process = ChordStabilizer::start(finger_table.clone());

        let chord_connection = ChordConnectionPool::new(finger_table.own_addr()?);
        let handle = ChordHandle {
            chord_connection,
            notifier,
            stabilize_process,
            server_shutdown,
        };
        Ok(handle)
    }

    pub async fn new_network_with_fixed_id(
        own_addr: SocketAddr,
        own_id: ChordId,
        notifier: Arc<ChordNotifier>,
    ) -> Result<Self> {
        let finger_table = FingerTable::for_new_network(NodeInfo {
            ip: own_addr.ip().to_string(),
            port: own_addr.port() as u32,
            id: own_id,
        })?;
        Self::new_with_finger_table(finger_table, notifier).await
    }

    /// # Explanation
    /// This function creates a new chord network. This works by initializing the finger table entries with the own node.
    /// The node's id is generated randomly.
    pub async fn new_network(own_addr: SocketAddr, notifier: Arc<ChordNotifier>) -> Result<Self> {
        let finger_table = FingerTable::for_new_network_with_random_id(own_addr)?;
        Self::new_with_finger_table(finger_table, notifier).await
    }

    pub async fn join_with_fixed_id(
        introducer_addr: SocketAddr,
        own_addr: SocketAddr,
        own_id: ChordId,
        notifier: Arc<ChordNotifier>,
    ) -> Result<Self> {
        let finger_table = FingerTable::from_introducer(
            introducer_addr,
            NodeInfo {
                ip: own_addr.ip().to_string(),
                port: own_addr.port() as u32,
                id: own_id,
            },
        )
        .await?;
        Self::new_with_finger_table(finger_table, notifier).await
    }

    /// # Explanation
    /// This function joins the chord network the introducer is located in. It uses the introducer node to
    /// initialize the finger table. And in the end the other nodes in the network are notified.
    pub async fn join(
        introducer_addr: SocketAddr,
        own_addr: SocketAddr,
        notifier: Arc<ChordNotifier>,
    ) -> Result<Self> {
        let finger_table =
            FingerTable::from_introducer_with_random_id(introducer_addr, own_addr).await?;
        Self::new_with_finger_table(finger_table, notifier).await
    }

    /// # Explanation
    /// This function starts the server of this chord node. It returns a channel that can be used to shutdown the server.
    fn start_server(
        finger_table: Arc<FingerTable>,
        notifier: Arc<ChordNotifier>,
    ) -> oneshot::Sender<()> {
        let (server_shutdown_sender, server_shutdown_receiver) = oneshot::channel();
        tokio::task::spawn(async move {
            if let Ok(own_addr) = finger_table.own_addr() {
                let chord_node = ChordNode::new(finger_table, notifier);
                Server::builder()
                    .add_service(NodeServer::new(chord_node))
                    .serve_with_shutdown(
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), own_addr.port()),
                        async move {
                            server_shutdown_receiver.await.ok();
                        },
                    )
                    .await
                    .ok();
            }
            log::trace!("The chord server shutdown.");
        });
        server_shutdown_sender
    }

    /// # Explanation
    /// This function can be used to leave the network. It updates the other nodes in the network,
    /// notifies the data layer that a key transfer needs to happen and shuts the server down.
    pub async fn leave(self) -> Result<()> {
        let successor = self.create_successor_client().await;
        let predecessor = self.create_predecessor_client().await;
        // stabilize should stop before other nodes are updated
        self.stabilize_process.stop().await;

        if let Ok(successor) = successor {
            log::trace!("While leaving the successor was {:?}.", successor.info());

            if let Ok(predecessor) = predecessor {
                log::trace!(
                    "While leaving the predecessor was {:?}.",
                    predecessor.info()
                );
                Self::update_others_leave(successor.clone(), predecessor).await?;
            }

            self.notifier
                .notify(ChordNotification::DataTo(TransferNotification::allow_all(
                    successor.ip(),
                )))
                .await;
        }

        self.notifier
            .notify(ChordNotification::StoreRangeUpdate(0..1)) // setting the store range to nothing
            .await;

        self.notifier.finalize().await;
        self.server_shutdown.send(()).ok();
        Ok(())
    }

    /// # Explantion
    /// This function notifies the successor and the predecessor that this node wants to leave.
    async fn update_others_leave(successor: Finger, predecessor: Finger) -> Result<()> {
        predecessor.notify_leave(successor.info()).await?;
        successor.notify_leave(predecessor.info()).await?;

        Ok(())
    }

    /// # Explanation
    /// This function returns the node that is responsible for storing the key that has the given id.
    /// (It basically returns the successor of id.)
    pub async fn find_node_by_id(&self, id: ChordId) -> Result<SocketAddr> {
        let mut chord_client = self.chord_connection.get_connection().await?;
        let response = chord_client
            .find_successor(Request::new(Identifier { id }))
            .await?;

        let node_info = response.into_inner();
        let store_ip = node_info.ip.parse()?;
        let store_port = node_info.port as u16;
        Ok(SocketAddr::new(store_ip, store_port))
    }

    /// # Explanation
    /// This function returns the node that is responsible for storing the given key.
    /// (It basically returns the successor of the key's id.)
    pub async fn find_node(&self, key: &str) -> Result<SocketAddr> {
        let key_id = compute_chord_id(key);
        log::trace!("The id of {} is {}.", key, key_id);
        self.find_node_by_id(key_id).await
    }

    /// # Explanation
    /// This function creates a gRPC client to the successor of this node.
    async fn create_successor_client(&self) -> Result<Finger> {
        let mut chord_client = self.chord_connection.get_connection().await?;
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
        let mut chord_client = self.chord_connection.get_connection().await?;
        let predecessor_info = chord_client
            .predecessor(Request::new(Empty {}))
            .await?
            .into_inner();
        let predecessor = Finger::from_info(predecessor_info)?;

        Ok(predecessor)
    }
}

#[cfg(test)]
mod tests {
    use crate::chord_handle::ChordHandle;
    use crate::chord_stabilizer::STABILIZE_INTERVAL;
    use crate::notification::chord_notification::ChordNotifier;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_with_1_node() {
        let notifier = Arc::new(ChordNotifier::new());
        let localhost_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let node_addr = SocketAddr::new(localhost_ip, 10001);
        let node = ChordHandle::new_network_with_fixed_id(node_addr, 0, notifier.clone())
            .await
            .expect("node: New network creation failed.");

        tokio::time::sleep(STABILIZE_INTERVAL).await;

        let successor_of_1 = node
            .find_node_by_id(1)
            .await
            .expect("The successor of id 1 should exist.");

        assert_eq!(successor_of_1, node_addr);
    }

    #[tokio::test]
    async fn test_with_2_nodes() {
        let notifier = Arc::new(ChordNotifier::new());
        let localhost_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let addr1 = SocketAddr::new(localhost_ip, 20001);
        let node1 = ChordHandle::new_network_with_fixed_id(addr1, 100, notifier.clone())
            .await
            .expect("node1: New network creation failed.");

        let addr2 = SocketAddr::new(localhost_ip, 20002);
        let node2 = ChordHandle::join_with_fixed_id(addr1, addr2, 200, notifier.clone())
            .await
            .expect("node2: Joining the network failed");

        tokio::time::sleep(STABILIZE_INTERVAL).await;

        let successor_of_50 = node1
            .find_node_by_id(50)
            .await
            .expect("Successor of 50 should exist.");
        assert_eq!(successor_of_50, addr1);

        let successor_of_150 = node2
            .find_node_by_id(150)
            .await
            .expect("Successor of 150 should exist.");
        assert_eq!(successor_of_150, addr2);

        let successor_of_250 = node1
            .find_node_by_id(250)
            .await
            .expect("Successor of 250 should exist.");
        assert_eq!(successor_of_250, addr1);

        node1.leave().await.ok();
        node2.leave().await.ok();
    }

    #[tokio::test]
    async fn test_with_8_nodes() {
        let notifier = Arc::new(ChordNotifier::new());
        let localhost_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let addr1 = SocketAddr::new(localhost_ip, 30001);
        let node1 = ChordHandle::new_network_with_fixed_id(addr1, 10_000_000, notifier.clone())
            .await
            .expect("node1: New network creation failed.");

        let addr2 = SocketAddr::new(localhost_ip, 30002);
        let node2 = ChordHandle::join_with_fixed_id(addr1, addr2, 11_000_000, notifier.clone())
            .await
            .expect("node2: Joining the network failed");

        let addr3 = SocketAddr::new(localhost_ip, 30003);
        let node3 = ChordHandle::join_with_fixed_id(addr1, addr3, 12_000_000, notifier.clone())
            .await
            .expect("node3: Joining the network failed");

        let addr4 = SocketAddr::new(localhost_ip, 30004);
        let node4 = ChordHandle::join_with_fixed_id(addr1, addr4, 7_000_000, notifier.clone())
            .await
            .expect("node4: Joining the network failed");

        let addr5 = SocketAddr::new(localhost_ip, 30005);
        let node5 = ChordHandle::join_with_fixed_id(addr1, addr5, 8_000_000, notifier.clone())
            .await
            .expect("node5: Joining the network failed");

        let addr6 = SocketAddr::new(localhost_ip, 30006);
        let node6 = ChordHandle::join_with_fixed_id(addr1, addr6, 9_000_000, notifier.clone())
            .await
            .expect("node6:Joining the network failed");

        let addr7 = SocketAddr::new(localhost_ip, 30007);
        let node7 = ChordHandle::join_with_fixed_id(addr1, addr7, 6_000_000, notifier.clone())
            .await
            .expect("node7: Joining the network failed");

        let addr8 = SocketAddr::new(localhost_ip, 30008);
        let node8 = ChordHandle::join_with_fixed_id(addr1, addr8, 5_000_000, notifier.clone())
            .await
            .expect("node8: Joining the network failed");

        tokio::time::sleep(4 * STABILIZE_INTERVAL).await;

        let successor_of_0 = node5
            .find_node_by_id(0)
            .await
            .expect("Successor of 0 should exist.");
        assert_eq!(successor_of_0, addr8);

        let successor_of_5_500_000 = node5
            .find_node_by_id(5_500_000)
            .await
            .expect("Successor of 5_500_000 should exist.");
        assert_eq!(successor_of_5_500_000, addr7);

        let successor_of_6_500_000 = node8
            .find_node_by_id(6_500_000)
            .await
            .expect("Successor of 6_500_000 should exist.");
        assert_eq!(successor_of_6_500_000, addr4);

        let successor_of_7_500_000 = node1
            .find_node_by_id(7_500_000)
            .await
            .expect("Successor of 7_500_000 should exist.");
        assert_eq!(successor_of_7_500_000, addr5);

        let successor_of_8_500_000 = node1
            .find_node_by_id(8_500_000)
            .await
            .expect("Successor of 8_500_000 should exist.");
        assert_eq!(successor_of_8_500_000, addr6);

        let successor_of_9_500_000 = node2
            .find_node_by_id(9_500_000)
            .await
            .expect("Successor of 9_500_000 should exist.");
        assert_eq!(successor_of_9_500_000, addr1);

        let successor_of_10_500_000 = node7
            .find_node_by_id(10_500_000)
            .await
            .expect("Successor of 10_500_000 should exist.");
        assert_eq!(successor_of_10_500_000, addr2);

        let successor_of_11_500_000 = node3
            .find_node_by_id(11_500_000)
            .await
            .expect("Successor of 11_500_000 should exist.");
        assert_eq!(successor_of_11_500_000, addr3);

        let successor_of_100_000_000 = node4
            .find_node_by_id(100_000_000)
            .await
            .expect("Successor of 100_000_000 should exist.");
        assert_eq!(successor_of_100_000_000, addr8);

        node1.leave().await.ok();
        node2.leave().await.ok();
        node3.leave().await.ok();
        node4.leave().await.ok();
        node5.leave().await.ok();
        node6.leave().await.ok();
        node7.leave().await.ok();
        node8.leave().await.ok();
    }

    #[tokio::test]
    async fn test_leave() {
        let notifier = Arc::new(ChordNotifier::new());
        let localhost_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let addr1 = SocketAddr::new(localhost_ip, 40001);
        let node1 = ChordHandle::new_network_with_fixed_id(addr1, 10_000_000, notifier.clone())
            .await
            .expect("node1: New network creation failed.");

        let addr2 = SocketAddr::new(localhost_ip, 40002);
        let node2 = ChordHandle::join_with_fixed_id(addr1, addr2, 11_000_000, notifier.clone())
            .await
            .expect("node2: Joining the network failed");

        let addr3 = SocketAddr::new(localhost_ip, 40003);
        let node3 = ChordHandle::join_with_fixed_id(addr1, addr3, 9_000_000, notifier.clone())
            .await
            .expect("node3: Joining the network failed");

        let addr4 = SocketAddr::new(localhost_ip, 40004);
        let node4 = ChordHandle::join_with_fixed_id(addr1, addr4, 8_000_000, notifier.clone())
            .await
            .expect("node4: Joining the network failed");

        tokio::time::sleep(2 * STABILIZE_INTERVAL).await;

        node3.leave().await.expect("node3 should be able to leave.");

        tokio::time::sleep(2 * STABILIZE_INTERVAL).await;

        let successor_of_8_500_000 = node4
            .find_node_by_id(8_500_000)
            .await
            .expect("Successor of 8_500_000 should exist.");
        assert_eq!(successor_of_8_500_000, addr1);

        let successor_of_9_500_000 = node4
            .find_node_by_id(9_500_000)
            .await
            .expect("Successor of 9_500_000 should exist.");
        assert_eq!(successor_of_9_500_000, addr1);

        node1.leave().await.expect("node1 should be able to leave.");
        tokio::time::sleep(2 * STABILIZE_INTERVAL).await;

        let successor_of_8_500_000 = node4
            .find_node_by_id(8_500_000)
            .await
            .expect("Successor of 8_500_000 should exist.");
        assert_eq!(successor_of_8_500_000, addr2);

        let successor_of_10_500_000 = node4
            .find_node_by_id(10_500_000)
            .await
            .expect("Successor of 10_500_000 should exist.");
        assert_eq!(successor_of_10_500_000, addr2);

        node2.leave().await.expect("node2 should be able to leave.");
        tokio::time::sleep(2 * STABILIZE_INTERVAL).await;

        let successor_of_10_500_000 = node4
            .find_node_by_id(10_500_000)
            .await
            .expect("Successor of 10_500_000 should exist.");
        assert_eq!(successor_of_10_500_000, addr4);

        node4
            .leave()
            .await
            .expect("node4: should be able to leave.");
    }
}
