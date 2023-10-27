use crate::finger_table::{in_ring_interval_exclusive, Finger, FingerTable};
use anyhow::{anyhow, Result};
use rand::{thread_rng, Rng};
use std::sync::Arc;

pub struct ChordStabilizer {
    own_id: u64,
    finger_table: Arc<FingerTable>,
}

impl ChordStabilizer {
    pub fn new(own_id: u64, finger_table: Arc<FingerTable>) -> Self {
        ChordStabilizer {
            own_id,
            finger_table,
        }
    }

    pub async fn stabilize(&self) -> Result<()> {
        self.finger_table.check_fingers().await;

        let mut successor = self
            .finger_table
            .successor()
            .await
            .ok_or(anyhow!("There is no successor."))?;

        if let Ok(x) = successor.predecessor().await {
            if in_ring_interval_exclusive(x.id, self.own_id, successor.id()) {
                let maybe_new_successor = Finger::from_info(x)?;
                // verify that it is an actual node
                if maybe_new_successor.check().await {
                    successor = maybe_new_successor;
                }
            }
        }

        self.finger_table
            .update_successor(Some(successor.clone()))
            .await;

        successor.notify(self.own_id).await?;
        // notify data layer

        Ok(())
    }

    pub async fn fix_fingers(&self) -> Result<()> {
        let i: usize = thread_rng().gen_range(1..64);
        let successor = self
            .finger_table
            .successor()
            .await
            .ok_or(anyhow!("There is no successor."))?;

        let ith_finger = successor
            .find_successor(self.own_id.wrapping_add(1 << i))
            .await?;

        self.finger_table
            .update_finger(i, Some(Finger::from_info(ith_finger)?))
            .await;

        Ok(())
    }
}
