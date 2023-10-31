use crate::storage::data_demo::DATA_PORT;
use crate::storage_rpc::data_storage_client::DataStorageClient;
use crate::storage_rpc::{KeyValue, KeyValues};
use anyhow::Result;
use chord::finger_table::{compute_chord_id, in_store_interval, ChordId};
use chord::notification::chord_notification::{
    ChordCharacteristic, ChordNotification, ChordNotifier, TransferNotification,
};
use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout};
use tonic::transport::{Channel, Uri};
use tonic::Request;

pub struct SimpleDataDistributor {
    distribution_process_shutdown: oneshot::Sender<()>,
    distribution_process_join: JoinHandle<()>,
    listen_process_join: JoinHandle<()>,
}

impl SimpleDataDistributor {
    pub fn new(notifier: Arc<ChordNotifier>, storage: Arc<Mutex<HashMap<String, String>>>) -> Self {
        let current_range = Arc::new(Mutex::new(0..0));
        let data_out_log = Arc::new(Mutex::new(VecDeque::new()));

        let listen_process_join =
            Self::start_listen_process(notifier, current_range.clone(), data_out_log.clone());

        let (distribution_process_shutdown, distribution_process_join) =
            Self::start_distribution_process(storage, current_range, data_out_log);

        SimpleDataDistributor {
            distribution_process_shutdown,
            distribution_process_join,
            listen_process_join,
        }
    }

    fn start_listen_process(
        notifier: Arc<ChordNotifier>,
        current_range: Arc<Mutex<Range<ChordId>>>,
        data_out_log: Arc<Mutex<VecDeque<TransferNotification>>>,
    ) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            let subscription = notifier
                .subscribe(ChordCharacteristic::AnyRangeOrDataTo)
                .await;

            if let Some(mut subscription) = subscription {
                while let Some(notification) = subscription.next().await {
                    match notification {
                        ChordNotification::DataTo(transfer_notification) => {
                            log::trace!("New transfer notification: {:?}.", transfer_notification);
                            let mut data_out_log = data_out_log.lock().await;
                            if data_out_log.len() > 10 {
                                data_out_log.pop_back();
                            }
                            data_out_log.push_front(transfer_notification);
                        }
                        ChordNotification::StoreRangeUpdate(updated_range) => {
                            log::trace!("Updated range: {:?}.", updated_range);
                            let _ =
                                std::mem::replace(&mut *current_range.lock().await, updated_range);
                        }
                        _ => {}
                    }
                }

                log::trace!("Subscription process stopped.");
            }
        })
    }

    fn start_distribution_process(
        storage: Arc<Mutex<HashMap<String, String>>>,
        current_range: Arc<Mutex<Range<ChordId>>>,
        data_out_log: Arc<Mutex<VecDeque<TransferNotification>>>,
    ) -> (oneshot::Sender<()>, JoinHandle<()>) {
        let (distribution_shutdown, mut distribution_shutdown_receiver) = oneshot::channel();
        let distribution_join = tokio::task::spawn(async move {
            let mut interval = interval(Duration::from_millis(100));

            while distribution_shutdown_receiver.try_recv().is_err() {
                interval.tick().await;
                let transfer_data = {
                    let current_range = current_range.lock().await.clone();
                    let data_out_log = data_out_log.lock().await;
                    let mut storage = storage.lock().await;
                    Self::extract_keys_to_transfer(current_range, &data_out_log, &mut *storage)
                };

                if transfer_data.len() > 0 {
                    log::trace!(
                        "Transfer the data {:?} with current range {:?}.",
                        transfer_data,
                        current_range.lock().await
                    );
                }
                Self::transfer_data(transfer_data).await;
            }

            log::trace!("Distribution process stopped.");
        });

        (distribution_shutdown, distribution_join)
    }

    fn extract_keys_to_transfer(
        current_range: Range<ChordId>,
        data_out_log: &VecDeque<TransferNotification>,
        storage: &mut HashMap<String, String>,
    ) -> HashMap<IpAddr, Vec<KeyValue>> {
        let mut transfers: HashMap<IpAddr, Vec<KeyValue>> = HashMap::new();
        storage.retain(|key, value| {
            let key_id = compute_chord_id(&key);
            if !in_store_interval(key_id, current_range.start, current_range.end) {
                if let Some(notification) = data_out_log
                    .iter()
                    .filter(|n| n.should_transfer(&key))
                    .next()
                {
                    transfers
                        .entry(notification.ip)
                        .or_insert_with(Vec::new)
                        .push(KeyValue {
                            key: key.clone(),
                            value: value.clone(),
                        });
                    false
                } else {
                    true
                }
            } else {
                true
            }
        });

        transfers
    }

    async fn transfer_data(transfers: HashMap<IpAddr, Vec<KeyValue>>) {
        for (ip, key_values) in transfers.into_iter() {
            let data_client = Self::create_data_client(ip).await.ok();
            if let Some(mut data_client) = data_client {
                data_client
                    .transfer_data(Request::new(KeyValues { key_values }))
                    .await
                    .ok();
            }
        }
    }

    /// # Explanation
    /// This function creates a client to the data server of the given ip.
    pub async fn create_data_client(addr: IpAddr) -> Result<DataStorageClient<Channel>> {
        let uri = Uri::builder()
            .scheme("http")
            .authority(format!("{}:{}", addr, DATA_PORT))
            .path_and_query("/")
            .build()?;
        let data_client =
            timeout(Duration::from_secs(1), DataStorageClient::connect(uri)).await??;

        Ok(data_client)
    }

    pub async fn stop(self) {
        self.distribution_process_shutdown.send(()).ok();
        self.distribution_process_join.await.ok();
        self.listen_process_join.await.ok();
    }
}
