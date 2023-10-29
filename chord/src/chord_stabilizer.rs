use crate::finger::Finger;
use crate::finger_table::{in_ring_interval_exclusive, FingerTable};
use anyhow::{anyhow, Result};
use rand::{thread_rng, Rng};
use std::sync::Arc;

pub struct ChordStabilizer {
    finger_table: Arc<FingerTable>,
}

impl ChordStabilizer {
    pub fn new(finger_table: Arc<FingerTable>) -> Self {
        ChordStabilizer { finger_table }
    }

    /// # Explanation
    /// The stabilize function updates the current successor if there is a new one.
    /// And it notifies the successor of this node's existence.
    pub async fn stabilize(&self) -> Result<()> {
        let mut successor = self
            .finger_table
            .successor()
            .await
            .ok_or(anyhow!("There is no successor in the finger table."))?;

        if let Ok(x) = successor.predecessor().await {
            if in_ring_interval_exclusive(x.id, self.finger_table.own_id(), successor.id()) {
                let maybe_new_successor = Finger::from_info(x)?;
                // verify that it is an actual node
                if maybe_new_successor.check().await {
                    successor = maybe_new_successor;

                    self.finger_table
                        .update_successor(Some(successor.clone()))
                        .await;

                    log::trace!("Updated the successor to {:?}.", successor.info());
                }
            }
        }

        successor.notify(self.finger_table.own_id()).await?;

        Ok(())
    }

    /// # Explanation
    /// fix_fingers selects a random finger and updates it by looking for the current successor of own_id + 2^i
    /// (i being the index of the selected finger).
    pub async fn fix_fingers(&self) -> Result<()> {
        let i: usize = thread_rng().gen_range(1..64);
        let successor = self
            .finger_table
            .successor()
            .await
            .ok_or(anyhow!("There is no successor."))?;

        let id_to_look_for = self.finger_table.own_id().wrapping_add(1 << i);
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

        self.finger_table
            .update_finger(i, Some(updated_finger))
            .await;

        Ok(())
    }
}
