use crate::chord_rpc::node_server::Node;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use crate::finger::Finger;
use crate::finger_table::{
    in_ring_interval_exclusive, in_store_interval, FingerTable, CHORD_ID_BITSIZE,
};
use crate::notification::chord_notification::{
    ChordNotification, ChordNotifier, TransferNotification,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

const SUCCESSOR_NOT_FOUND: &str = "Successor not found.";
const PREDECESSOR_NOT_FOUND: &str = "Predecessor not found.";
const NO_REMOTE_ADDR: &str = "Could not extract the remote address out of the request.";

pub struct ChordNode {
    finger_table: Arc<FingerTable>,
    notifier: Arc<ChordNotifier>,
}

impl ChordNode {
    pub fn new(finger_table: Arc<FingerTable>, notifier: Arc<ChordNotifier>) -> Self {
        ChordNode {
            finger_table,
            notifier,
        }
    }
}

#[tonic::async_trait]
impl Node for ChordNode {
    /// # Explanation
    /// This function returns the id this chord node is using. It can be used to verify the correctness
    /// of the current network.
    async fn get_id(&self, _: Request<Empty>) -> Result<Response<Identifier>, Status> {
        Ok(Response::new(Identifier {
            id: self.finger_table.own_id(),
        }))
    }

    /// # Explanation
    /// This function searches for the closest finger (in this finger table) that precedes the given id.
    async fn closest_preceding_finger(
        &self,
        request: Request<Identifier>,
    ) -> Result<Response<NodeInfo>, Status> {
        let id = request.into_inner().id;

        let mut closest_node = self.finger_table.own_info();
        for i in 0..CHORD_ID_BITSIZE {
            if let Some(ith_finger) = self.finger_table.get_finger(i).await {
                if in_ring_interval_exclusive(ith_finger.id(), closest_node.id, id) {
                    // maybe check finger before updating
                    closest_node = ith_finger.info();
                }
            }
        }

        log::trace!(
            "closest_preceding_finger: The closest preceding finger of {} is {:?}.",
            id,
            closest_node
        );

        Ok(Response::new(closest_node))
    }

    /// # Explanation
    /// This function returns the successor node of the in the request given identifier. It works by
    /// calling closest_preceding_finger until it finds the node that is supposed to store the given id.
    async fn find_successor(
        &self,
        request: Request<Identifier>,
    ) -> Result<Response<NodeInfo>, Status> {
        let id = request.into_inner().id;

        let mut node = Finger::from_info(self.finger_table.own_info())?; // should not be returned
        let mut node_successor = self
            .finger_table
            .successor()
            .await
            .ok_or(Status::aborted(SUCCESSOR_NOT_FOUND))?;

        while !in_store_interval(id, node.id(), node_successor.id()) {
            node = Finger::from_info(node.closest_preceding_finger(id).await?)?;
            node_successor = Finger::from_info(node.successor().await?)?;
        }

        log::trace!(
            "find_successor: The successor of {} is {:?}.",
            id,
            node_successor.info()
        );

        Ok(Response::new(node_successor.info()))
    }

    /// # Explanation
    /// The function returns the successor of this chord node. It returns the successor's ip and its id.
    async fn successor(&self, _: Request<Empty>) -> Result<Response<NodeInfo>, Status> {
        let successor = self
            .finger_table
            .successor()
            .await
            .ok_or(Status::aborted(SUCCESSOR_NOT_FOUND))?;

        log::trace!("successor: My successor is {:?}.", successor.info());

        Ok(Response::new(successor.info()))
    }

    /// # Explanation
    /// The function returns the predecessor of this chord node. It returns the predecessor's ip and its id.
    async fn predecessor(&self, _: Request<Empty>) -> Result<Response<NodeInfo>, Status> {
        let predecessor = self
            .finger_table
            .predecessor()
            .await
            .ok_or(Status::aborted(PREDECESSOR_NOT_FOUND))?;

        log::trace!("predecessor: My predecessor is {:?}.", predecessor.info());

        Ok(Response::new(predecessor.info()))
    }

    /// # Explanation
    /// The notify function should be called by all the nodes that believe that they are the new
    /// predecessor of this node.
    ///
    /// Currently it only works on joins. This might change when each node stores a successor list
    /// (instead of just one successor).
    async fn notify(&self, request: Request<NodeInfo>) -> Result<Response<Empty>, Status> {
        let maybe_new_predecessor = Finger::from_info(request.into_inner())?;
        let optional_predecessor = self.finger_table.predecessor().await;

        match optional_predecessor {
            Some(current_predecssor)
                if in_ring_interval_exclusive(
                    maybe_new_predecessor.id(),
                    current_predecssor.id(),
                    self.finger_table.own_id(),
                ) =>
            {
                log::trace!(
                    "notify: Updating the predecessor to {:?}.",
                    maybe_new_predecessor.info()
                );
                self.update_predecessor_on_join(maybe_new_predecessor, current_predecssor)
                    .await;
            }
            None => {
                log::trace!(
                    "notify: Setting the predecessor to {:?}.",
                    maybe_new_predecessor.info()
                );
                self.update_predecessor_on_join(
                    maybe_new_predecessor,
                    Finger::from_info(self.finger_table.own_info())?,
                )
                .await;
            }
            _ => {}
        }

        Ok(Response::new(Empty {}))
    }

    /// # Explantion
    /// The notify_leave function should be called by the nodes that want to leave and
    /// either are the successor or predecessor of this node.
    ///
    /// This function might be removed when a successor list is used.
    async fn notify_leave(&self, request: Request<NodeInfo>) -> Result<Response<Empty>, Status> {
        let remote_ip = request
            .remote_addr()
            .ok_or(Status::aborted(NO_REMOTE_ADDR))?
            .ip();
        let new_finger = Finger::from_info(request.into_inner())?;

        if let Some(successor) = self.finger_table.successor().await {
            if successor.ip() == remote_ip {
                log::trace!(
                    "notify_leave: Updating the successor to {:?}.",
                    new_finger.info()
                );
                self.update_successor(new_finger.clone()).await;
            }
        }

        if let Some(predecessor) = self.finger_table.predecessor().await {
            if predecessor.ip() == remote_ip {
                log::trace!(
                    "notify_leave: Updating the predecessor to {:?}.",
                    new_finger.info()
                );
                self.update_predecessor_on_leave(new_finger, predecessor)
                    .await;
            }
        }

        Ok(Response::new(Empty {}))
    }
}

impl ChordNode {
    pub(crate) async fn update_predecessor_on_join(
        &self,
        new_predecessor: Finger,
        old_predecessor: Finger,
    ) {
        self.finger_table
            .update_predecessor(Some(new_predecessor.clone()))
            .await;

        self.notifier
            .notify(ChordNotification::DataTo(TransferNotification::from_range(
                new_predecessor.ip(),
                old_predecessor.id(),
                new_predecessor.id(),
            )))
            .await;

        self.notifier
            .notify(ChordNotification::StoreRangeUpdate(
                new_predecessor.id()..self.finger_table.own_id(),
            ))
            .await;
    }

    pub(crate) async fn update_predecessor_on_leave(
        &self,
        new_predecessor: Finger,
        old_predecessor: Finger,
    ) {
        self.finger_table
            .update_predecessor(Some(new_predecessor.clone()))
            .await;

        // The old predecessor might send data. Its store range should be (new_predecessor, old_predecessor]
        self.notifier
            .notify(ChordNotification::DataFrom(
                TransferNotification::from_range(
                    old_predecessor.ip(),
                    new_predecessor.id(),
                    old_predecessor.id(),
                ),
            ))
            .await;

        self.notifier
            .notify(ChordNotification::StoreRangeUpdate(
                new_predecessor.id()..self.finger_table.own_id(),
            ))
            .await;
    }

    pub(crate) async fn update_successor(&self, new_successor: Finger) {
        self.finger_table
            .update_successor(Some(new_successor))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use crate::chord_node::ChordNode;
    use crate::chord_rpc::node_server::Node;
    use crate::chord_rpc::{Identifier, NodeInfo};
    use crate::finger::Finger;
    use crate::finger_table::CHORD_ID_BITSIZE;
    use crate::finger_table::{ChordId, FingerTable};
    use crate::notification::chord_notification::{
        ChordNotification, ChordNotifier, TransferNotification,
    };
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tonic::Request;

    #[tokio::test]
    async fn test_closest_preceding_finger() {
        async fn check(node: &ChordNode, id: ChordId, correct_node: NodeInfo) {
            let response = node
                .closest_preceding_finger(Request::new(Identifier { id }))
                .await
                .expect("The closest preceding finger should exist.");
            assert_eq!(response.into_inner(), correct_node);
        }

        let localhost_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let notifier = Arc::new(ChordNotifier::new());

        let node_fingers: Vec<Finger> = (0..8)
            .map(|i| Finger::new(SocketAddr::new(localhost_ip, i), (i * 10) as ChordId))
            .collect();

        let mut table: Vec<Mutex<Option<Finger>>> = std::iter::repeat_with(|| Mutex::new(None))
            .take(CHORD_ID_BITSIZE)
            .collect();
        table[10] = Mutex::new(Some(node_fingers[0].clone()));
        table[3] = Mutex::new(Some(node_fingers[4].clone()));
        table[7] = Mutex::new(Some(node_fingers[2].clone()));
        table[42] = Mutex::new(Some(node_fingers[7].clone()));
        table[4] = Mutex::new(Some(node_fingers[1].clone()));
        table[16] = Mutex::new(Some(node_fingers[5].clone()));
        table[11] = Mutex::new(Some(node_fingers[3].clone()));
        table[60] = Mutex::new(Some(node_fingers[6].clone()));

        let finger_table = FingerTable::new(node_fingers[0].info(), Mutex::new(None), table);
        let node = ChordNode::new(Arc::new(finger_table), notifier);

        check(&node, 2, node_fingers[0].info()).await;
        check(&node, 29, node_fingers[2].info()).await;
        check(&node, 75, node_fingers[7].info()).await;
        check(&node, 60, node_fingers[5].info()).await;
        check(&node, 42, node_fingers[4].info()).await;
        check(&node, 53, node_fingers[5].info()).await;
    }

    #[tokio::test]
    async fn test_notify() {
        let localhost_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let notifier = Arc::new(ChordNotifier::new());

        let node50_finger = Finger::new(SocketAddr::new(localhost_ip, 2), 50);
        let node75_finger = Finger::new(SocketAddr::new(localhost_ip, 3), 75);

        let node_finger = Finger::new(SocketAddr::new(localhost_ip, 1), 100);
        let finger_table = Arc::new(FingerTable::from_successor_predecessor(
            None,
            None,
            node_finger.info(),
        ));
        let node = ChordNode::new(finger_table.clone(), notifier.clone());

        let _ = node
            .notify(Request::new(node50_finger.info()))
            .await
            .expect("");
        assert_eq!(
            finger_table.predecessor().await.map(|f| f.info()),
            Some(node50_finger.info())
        );

        let _ = node
            .notify(Request::new(node75_finger.info()))
            .await
            .expect("");
        assert_eq!(
            finger_table.predecessor().await.map(|f| f.info()),
            Some(node75_finger.info())
        );

        let _ = node
            .notify(Request::new(node50_finger.info()))
            .await
            .expect("");
        assert_eq!(
            finger_table.predecessor().await.map(|f| f.info()),
            Some(node75_finger.info())
        );
    }

    #[tokio::test]
    async fn test_update_predecessor_on_join() {
        let localhost_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let notifier = Arc::new(ChordNotifier::new());

        let node10_finger = Finger::new(SocketAddr::new(localhost_ip, 2), 10);
        let node5_finger = Finger::new(SocketAddr::new(localhost_ip, 3), 5);

        let node_finger = Finger::new(SocketAddr::new(localhost_ip, 1), 0);
        let finger_table = Arc::new(FingerTable::from_successor_predecessor(
            None,
            Some(node_finger.clone()),
            node_finger.info(),
        ));
        let node = ChordNode::new(finger_table.clone(), notifier.clone());

        node.update_predecessor_on_join(node5_finger.clone(), node_finger.clone())
            .await;
        assert_eq!(
            finger_table.predecessor().await.map(|f| f.info()),
            Some(node5_finger.info())
        );
        assert!(
            notifier
                .has_and_remove(&ChordNotification::StoreRangeUpdate(5..0))
                .await
        );
        assert!(
            notifier
                .has_and_remove(&ChordNotification::DataTo(
                    TransferNotification::from_range(localhost_ip, 0, 5)
                ))
                .await
        );

        node.update_predecessor_on_join(node10_finger.clone(), node5_finger.clone())
            .await;
        assert_eq!(
            finger_table.predecessor().await.map(|f| f.info()),
            Some(node10_finger.info())
        );
        assert!(
            notifier
                .has_and_remove(&ChordNotification::StoreRangeUpdate(10..0))
                .await
        );
        assert!(
            notifier
                .has_and_remove(&ChordNotification::DataTo(
                    TransferNotification::from_range(localhost_ip, 5, 10)
                ))
                .await
        );
    }

    #[tokio::test]
    async fn test_update_predecessor_on_leave() {
        let localhost_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let notifier = Arc::new(ChordNotifier::new());

        let node10_finger = Finger::new(SocketAddr::new(localhost_ip, 2), 10);
        let node5_finger = Finger::new(SocketAddr::new(localhost_ip, 3), 5);

        let node_finger = Finger::new(SocketAddr::new(localhost_ip, 1), 0);
        let finger_table = Arc::new(FingerTable::from_successor_predecessor(
            None,
            Some(node10_finger.clone()),
            node_finger.info(),
        ));
        let node = ChordNode::new(finger_table.clone(), notifier.clone());

        node.update_predecessor_on_leave(node5_finger.clone(), node10_finger.clone())
            .await;
        assert_eq!(
            finger_table.predecessor().await.map(|f| f.info()),
            Some(node5_finger.info())
        );
        assert!(
            notifier
                .has_and_remove(&ChordNotification::StoreRangeUpdate(5..0))
                .await
        );
        assert!(
            notifier
                .has_and_remove(&ChordNotification::DataFrom(
                    TransferNotification::from_range(localhost_ip, 5, 10)
                ))
                .await
        );

        node.update_predecessor_on_leave(node_finger.clone(), node5_finger.clone())
            .await;
        assert_eq!(
            finger_table.predecessor().await.map(|f| f.info()),
            Some(node_finger.info())
        );
        assert!(
            notifier
                .has_and_remove(&ChordNotification::StoreRangeUpdate(0..0))
                .await
        );
        assert!(
            notifier
                .has_and_remove(&ChordNotification::DataFrom(
                    TransferNotification::from_range(localhost_ip, 0, 5)
                ))
                .await
        );
    }
}
