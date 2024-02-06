use crate::storage_rpc::{KeyValue, KeyValues};
use crate::utils::{create_data_client, NodeDataState};
use anyhow::{anyhow, Result};
use chord::notification::chord_notification::{
    ChordCharacteristic, ChordNotification, ChordNotifier, TransferNotification,
};
use chord::notification::notifier::Subscription;
use futures::StreamExt;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::Request;

pub struct SimpleNotificationHandler {
    subscription: Subscription<ChordNotification>,
    node_state: Arc<Mutex<NodeDataState>>,
}

impl SimpleNotificationHandler {
    pub async fn from_notifier(
        notifier: Arc<ChordNotifier>,
        node_state: Arc<Mutex<NodeDataState>>,
    ) -> Result<Self> {
        let subscription = notifier
            .subscribe(ChordCharacteristic::AnyRangeOrDataTo)
            .await;

        if let Some(subscription) = subscription {
            Ok(Self {
                subscription,
                node_state,
            })
        } else {
            Err(anyhow!("Was not able to create a subscription."))
        }
    }

    pub async fn handle_notification(&mut self) {
        if let Some(notification) = self.subscription.next().await {
            match notification {
                ChordNotification::DataTo(transfer_notification) => {
                    let data = self.extract_data_to_transfer(transfer_notification).await;
                    Self::transfer_data(transfer_notification.ip, data).await;
                }
                ChordNotification::StoreRangeUpdate(updated_range) => {
                    let mut node_state = self.node_state.lock().await;
                    let _ = std::mem::replace(&mut node_state.range, updated_range);
                }
                _ => unreachable!("Should only be subscribed to DataTo and StoreRangeUpdate."),
            }
        }
    }

    pub async fn extract_data_to_transfer(
        &self,
        transfer_notification: TransferNotification,
    ) -> Vec<KeyValue> {
        let mut data = Vec::new();
        let mut node_state = self.node_state.lock().await;
        node_state.data.retain(|key, value| {
            if transfer_notification.should_transfer(&key) {
                data.push(KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                });
                false
            } else {
                true
            }
        });
        data
    }

    pub async fn transfer_data(addr: IpAddr, data: Vec<KeyValue>) {
        let data_client = create_data_client(addr).await.ok();
        if let Some(mut data_client) = data_client {
            data_client
                .transfer_data(Request::new(KeyValues { key_values: data }))
                .await
                .ok();
        }
    }
}
