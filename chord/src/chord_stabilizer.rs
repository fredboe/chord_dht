use crate::finger::Finger;
use crate::finger_table::{in_ring_interval_exclusive, FingerTable, CHORD_ID_BITSIZE};
use anyhow::{anyhow, Result};
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::interval;

const STABILIZE_INTERVAL: Duration = Duration::from_millis(100);

pub struct ChordStabilizer {
    stabilize_process_shutdown: oneshot::Sender<()>,
    stabilize_process_join: JoinHandle<()>,
}

impl ChordStabilizer {
    /// # Explanation
    /// This function starts the stabilize process that periodically notifies
    /// the successor of this node's existence and also updates the fingers in the finger table.
    /// This function returns a channel that can be used to shutdown the process.
    pub fn start(finger_table: Arc<FingerTable>) -> Self {
        let (stabilize_shutdown, mut stabilize_shutdown_receiver) = oneshot::channel();
        let stabilize_process_join = tokio::task::spawn(async move {
            let mut interval = interval(STABILIZE_INTERVAL);

            while stabilize_shutdown_receiver.try_recv().is_err() {
                interval.tick().await;
                Self::stabilize(finger_table.clone()).await.ok();
                Self::fix_fingers(finger_table.clone()).await.ok();
            }

            log::trace!("The stabilize process shutdown.");
        });

        ChordStabilizer {
            stabilize_process_shutdown: stabilize_shutdown,
            stabilize_process_join,
        }
    }

    pub async fn stop(self) {
        self.stabilize_process_shutdown.send(()).ok();
        self.stabilize_process_join.await.ok();
    }

    /// # Explanation
    /// The stabilize function updates the current successor if there is a new one.
    /// And it notifies the successor of this node's existence.
    async fn stabilize(finger_table: Arc<FingerTable>) -> Result<()> {
        finger_table.check_fingers().await;

        let mut successor = finger_table
            .successor()
            .await
            .ok_or(anyhow!("There is no successor in the finger table."))?;

        if let Ok(x) = successor.predecessor().await {
            if in_ring_interval_exclusive(x.id, finger_table.own_id(), successor.id()) {
                let maybe_new_successor = Finger::from_info(x)?;
                // verify that it is an actual node
                if maybe_new_successor.check().await {
                    successor = maybe_new_successor;

                    finger_table.update_successor(Some(successor.clone())).await;

                    log::trace!("Updated the successor to {:?}.", successor.info());
                }
            }
        }

        successor.notify(finger_table.own_id()).await?;

        Ok(())
    }

    /// # Explanation
    /// fix_fingers selects a random finger and updates it by looking for the current successor of own_id + 2^i
    /// (i being the index of the selected finger).
    async fn fix_fingers(finger_table: Arc<FingerTable>) -> Result<()> {
        let i: usize = thread_rng().gen_range(1..CHORD_ID_BITSIZE);
        let successor = finger_table
            .successor()
            .await
            .ok_or(anyhow!("There is no successor."))?;

        let id_to_look_for = finger_table.own_id().wrapping_add(1 << i);
        log::trace!(
            "Fixing the {} ith finger. Looking for id {}.",
            i,
            id_to_look_for
        );

        let ith_finger = successor.find_successor(id_to_look_for).await?;

        let updated_finger = Finger::from_info(ith_finger)?;
        log::trace!(
            "Updating the {} th finger to {:?}.",
            i,
            updated_finger.info()
        );

        finger_table.update_finger(i, Some(updated_finger)).await;

        Ok(())
    }
}
