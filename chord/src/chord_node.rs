use crate::chord_rpc::node_server::Node;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use crate::finger::Finger;
use crate::finger_table::{
    in_ring_interval_exclusive, in_store_interval, ChordId, FingerTable, CHORD_ID_BITSIZE,
};
use crate::notification::chord_notification::{
    ChordNotification, ChordNotifier, TransferNotification,
};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use tonic::{Request, Response, Status};

const COULD_NOT_CREATE_FINGER: &str = "Was not able to create a finger from the given ip.";
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
        let local_addr = request
            .local_addr()
            .map(|addr| addr.ip())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let mut closest_node = NodeInfo {
            ip: local_addr.to_string(),
            id: self.finger_table.own_id(),
        };

        let id = request.into_inner().id;

        for i in 0..CHORD_ID_BITSIZE {
            if let Some(ith_finger) = self.finger_table.get_finger(i).await {
                if in_ring_interval_exclusive(ith_finger.id(), closest_node.id, id) {
                    // maybe check finger before updating
                    if ith_finger.check().await {
                        closest_node = ith_finger.info();
                    }
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

        let mut node = Finger::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.finger_table.own_id());
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
    async fn notify(&self, request: Request<Identifier>) -> Result<Response<Empty>, Status> {
        let ip = request
            .remote_addr()
            .ok_or(Status::aborted(COULD_NOT_CREATE_FINGER))?
            .ip();
        let id = request.into_inner().id;

        let maybe_new_predecessor = Finger::new(ip, id);
        let optional_predecessor = self.finger_table.predecessor().await;

        match optional_predecessor {
            Some(predecssor)
                if in_ring_interval_exclusive(id, predecssor.id(), self.finger_table.own_id()) =>
            {
                log::trace!(
                    "notify: Updating the predecessor to {:?}.",
                    maybe_new_predecessor.info()
                );
                self.update_predecessor_on_join(maybe_new_predecessor, predecssor.id())
                    .await;
            }
            None => {
                log::trace!(
                    "notify: Setting the predecessor to {:?}.",
                    maybe_new_predecessor.info()
                );
                self.update_predecessor_on_join(maybe_new_predecessor, self.finger_table.own_id())
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
    async fn update_predecessor_on_join(
        &self,
        new_predecessor: Finger,
        old_predecessor_id: ChordId,
    ) {
        self.finger_table
            .update_predecessor(Some(new_predecessor.clone()))
            .await;

        self.notifier
            .notify(ChordNotification::DataTo(TransferNotification::from_range(
                new_predecessor.ip(),
                old_predecessor_id,
                new_predecessor.id(),
            )))
            .await;

        self.notifier
            .notify(ChordNotification::StoreRangeUpdate(
                new_predecessor.id()..self.finger_table.own_id(),
            ))
            .await;
    }

    async fn update_predecessor_on_leave(&self, new_predecessor: Finger, old_predecessor: Finger) {
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

    async fn update_successor(&self, new_successor: Finger) {
        self.finger_table
            .update_successor(Some(new_successor))
            .await;
    }
}
