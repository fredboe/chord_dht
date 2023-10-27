use crate::chord_rpc::node_server::Node;
use crate::chord_rpc::{Empty, Identifier, NodeInfo};
use crate::finger_table::{in_ring_interval_exclusive, in_store_interval, Finger, FingerTable};
use crate::notification::chord_notification::{
    ChordNotification, ChordNotifier, TransferNotification,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

const COULD_NOT_CREATE_FINGER: &str = "Was not able to create a finger from the given ip.";
const SUCCESSOR_NOT_FOUND: &str = "Successor was not found.";
const PREDECESSOR_NOT_FOUND: &str = "Predecessor was not found.";

pub struct ChordNode {
    own_id: u64,
    finger_table: Arc<FingerTable>,
    notifier: Arc<ChordNotifier>,
}

impl ChordNode {
    pub fn new(own_id: u64, finger_table: Arc<FingerTable>, notifier: Arc<ChordNotifier>) -> Self {
        ChordNode {
            own_id,
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
        Ok(Response::new(Identifier { id: self.own_id }))
    }

    /// # Explanation
    /// This function returns the successor node of the in the request given identifier. It works by checking if
    /// the successor of this node is the successor of the identifier and if not it forwards the request to the successor.
    /// By that we move clockwise on the chord ring until we find the successor.
    ///
    /// This function will be improved with an improved finger table to be in O(log n).
    async fn find_successor(
        &self,
        request: Request<Identifier>,
    ) -> Result<Response<NodeInfo>, Status> {
        let successor = self
            .finger_table
            .successor()
            .await
            .ok_or(Status::aborted(SUCCESSOR_NOT_FOUND))?;
        let requested_id = request.into_inner().id;
        if in_store_interval(requested_id, self.own_id, successor.id()) {
            Ok(Response::new(NodeInfo {
                ip: successor.ip().to_string(),
                id: successor.id(),
            }))
        } else {
            successor
                .find_successor(requested_id)
                .await
                .map(|info| Response::new(info))
        }
    }

    /// # Explanation
    /// This function returns the predecessor node of the in the request given identifier. It works by checking if
    /// the predecessor of this node is the predecessor of the identifier and if not it forwards the request to the predecessor.
    /// By that we move counterclockwise on the chord ring until we find the predecessor.
    ///
    /// This function will be improved with an improved finger table to be in O(log n).
    async fn find_predecessor(
        &self,
        request: Request<Identifier>,
    ) -> Result<Response<NodeInfo>, Status> {
        let predecessor = self
            .finger_table
            .predecessor()
            .await
            .ok_or(Status::aborted(PREDECESSOR_NOT_FOUND))?;
        let requested_id = request.into_inner().id;
        if in_store_interval(requested_id, predecessor.id(), self.own_id) {
            Ok(Response::new(NodeInfo {
                ip: predecessor.ip().to_string(),
                id: predecessor.id(),
            }))
        } else {
            predecessor
                .find_predecessor(requested_id)
                .await
                .map(|info| Response::new(info))
        }
    }

    /// # Explanation
    /// The function returns the successor of this chord node. It returns the successor's ip and its id.
    async fn successor(&self, _: Request<Empty>) -> Result<Response<NodeInfo>, Status> {
        let successor = self
            .finger_table
            .successor()
            .await
            .ok_or(Status::aborted(SUCCESSOR_NOT_FOUND))?;
        Ok(Response::new(NodeInfo {
            ip: successor.ip().to_string(),
            id: successor.id(),
        }))
    }

    /// # Explanation
    /// The function returns the predecessor of this chord node. It returns the predecessor's ip and its id.
    async fn predecessor(&self, _: Request<Empty>) -> Result<Response<NodeInfo>, Status> {
        let predecessor = self
            .finger_table
            .predecessor()
            .await
            .ok_or(Status::aborted(PREDECESSOR_NOT_FOUND))?;
        Ok(Response::new(NodeInfo {
            ip: predecessor.ip().to_string(),
            id: predecessor.id(),
        }))
    }

    async fn notify(&self, request: Request<Identifier>) -> Result<Response<Empty>, Status> {
        let ip = request
            .remote_addr()
            .ok_or(Status::aborted(COULD_NOT_CREATE_FINGER))?
            .ip();
        let id = request.into_inner().id;

        let maybe_new_predecessor = Finger::new(ip, id);
        let optional_predecessor = self.finger_table.predecessor().await;

        match optional_predecessor {
            Some(predecssor) if in_ring_interval_exclusive(id, predecssor.id(), self.own_id) => {
                self.update_predecessor_and_notify(maybe_new_predecessor, predecssor.id(), id)
                    .await;
            }
            None => {
                self.update_predecessor_and_notify(maybe_new_predecessor, self.own_id, id)
                    .await;
            }
            _ => {}
        }

        Ok(Response::new(Empty {}))
    }
}

impl ChordNode {
    async fn update_predecessor_and_notify(
        &self,
        new_predecessor: Finger,
        left_id: u64,
        right_id: u64,
    ) {
        self.finger_table
            .update_predecessor(Some(new_predecessor.clone()))
            .await;

        // notify the data layer
        self.notifier
            .notify(ChordNotification::DataTo(TransferNotification::from_range(
                new_predecessor.ip(),
                left_id,
                right_id,
            )))
            .await;
    }
}
