use crate::storage_rpc::data_storage_client::DataStorageClient;
use crate::storage_rpc::data_storage_server::DataStorage;
use crate::storage_rpc::data_storage_server::DataStorageServer;
use crate::storage_rpc::{Empty, Key, KeyValue, KeyValues, OptionalValue, Value};
use anyhow::Result;
use chord::notification::chord_notification::{
    ChordCharacteristic, ChordNotification, ChordNotifier, TransferNotification,
};
use chord::notification::notifier::Subscription;
use futures::StreamExt;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Status};

pub const DATA_PORT: u16 = 43355;

/// # Explanation
/// This struct is used for starting a simple data server that uses a simple HashMap as storage and
/// to start the transfer_out process.
///
/// The data server provides basic functions for storing and retrieving data (in this case strings).
///
/// The transfer_out process subscribes to the DataTo characteristic and thus is notified by the chord
/// layer of any key transfers out of this node. The transfer_out process then performs this key transfers.
pub struct SimpleDataHandle {
    shutdown_server: oneshot::Sender<()>,
    transfer_out_handle: JoinHandle<()>,
}

impl SimpleDataHandle {
    /// # Explanation
    /// This function starts the data server and the transfer_out process.
    pub async fn start(notifier: Arc<ChordNotifier>) -> Result<Self> {
        // the storage needs to be shared between the server and the transfer_out process
        let storage = Arc::new(Mutex::new(HashMap::new()));
        let shutdown_server = Self::start_server(storage.clone()).await?;
        let transfer_out_handle = Self::start_transfer_out_task(notifier, storage).await;
        Ok(SimpleDataHandle {
            shutdown_server,
            transfer_out_handle,
        })
    }

    /// # Explanation
    /// Starts the actual data server.
    async fn start_server(
        storage: Arc<Mutex<HashMap<String, String>>>,
    ) -> Result<oneshot::Sender<()>> {
        let storage_service = SimpleStorage::new(storage.clone());
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        tokio::task::spawn(async move {
            Server::builder()
                .add_service(DataStorageServer::new(storage_service))
                .serve_with_shutdown(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DATA_PORT),
                    async move {
                        shutdown_receiver.await.ok();
                        log::trace!("Data server shutdown.");
                    },
                )
                .await
                .ok();
        });
        Ok(shutdown_sender)
    }

    /// # Explanation
    /// Starts the transfer_out process.
    async fn start_transfer_out_task(
        notifier: Arc<ChordNotifier>,
        storage: Arc<Mutex<HashMap<String, String>>>,
    ) -> JoinHandle<()> {
        let task_handle = tokio::task::spawn(async move {
            let subscription = notifier.subscribe(ChordCharacteristic::AnyDataTo).await;
            if let Some(subscription) = subscription {
                Self::handle_transfer_out_subscription(subscription, storage).await;
            }
            log::trace!("Transfer out task stopped.");
        });
        task_handle
    }

    /// # Explanation
    /// This function waits for new DataTo notifications and then processes these.
    async fn handle_transfer_out_subscription(
        mut subscription: Subscription<ChordNotification>,
        storage: Arc<Mutex<HashMap<String, String>>>,
    ) {
        while let Some(notification) = subscription.next().await {
            match notification {
                ChordNotification::DataTo(transfer_notification) => {
                    Self::transport_data_out(transfer_notification, storage.clone())
                        .await
                        .ok();
                }
                _ => {}
            }
        }
    }

    /// # Explanation
    /// This function processes a single notification. It creates a client to the ip specified in the notification.
    /// Then it searches the storage (HashMap) for all the keys that should be transfered.
    /// And then all these keys are transfered to the ip.
    async fn transport_data_out(
        transfer_notification: TransferNotification,
        storage: Arc<Mutex<HashMap<String, String>>>,
    ) -> Result<()> {
        let transfer_ip = transfer_notification.ip;
        let mut data_client = Self::create_data_client(transfer_ip).await?;

        let out = {
            let mut storage = storage.lock().await;
            let mut out = vec![];
            storage.retain(|key, value| {
                if transfer_notification.should_transfer(&key) {
                    out.push(KeyValue {
                        key: key.clone(),
                        value: value.clone(),
                    });
                    false
                } else {
                    true
                }
            });
            out
        };

        log::trace!("Transfer the data {:?} to {}.", out, transfer_ip);

        data_client
            .transfer_data(Request::new(KeyValues { key_values: out }))
            .await?;
        Ok(())
    }

    /// # Explantion
    /// This function stops the transfer_out process and the data server.
    pub async fn stop(self) {
        self.transfer_out_handle.await.ok();
        self.shutdown_server.send(()).ok();
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
}

/// # Explanation
/// This is a simple data server that uses a hash map as the storage.
pub struct SimpleStorage {
    storage: Arc<Mutex<HashMap<String, String>>>,
}

impl SimpleStorage {
    pub fn new(storage: Arc<Mutex<HashMap<String, String>>>) -> Self {
        SimpleStorage { storage }
    }
}

#[tonic::async_trait]
impl DataStorage for SimpleStorage {
    async fn lookup(&self, request: Request<Key>) -> Result<Response<OptionalValue>, Status> {
        let key = request.into_inner().key;
        log::trace!("Lookup for {}.", key);
        let storage = self.storage.lock().await;
        let optional_value = storage.get(&key).cloned().map(|value| Value { value });

        Ok(Response::new(OptionalValue { optional_value }))
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        let KeyValue { key, value } = request.into_inner();
        log::trace!("Put of {} and {}.", key, value);
        let mut storage = self.storage.lock().await;
        storage.insert(key, value);

        Ok(Response::new(Empty {}))
    }

    /// # Explanation
    /// This function checks with the notifier if the transfer of keys into this node is allowed.
    /// And then it extends the current storage with all the keys in the request.
    async fn transfer_data(&self, request: Request<KeyValues>) -> Result<Response<Empty>, Status> {
        let key_values = request.into_inner().key_values;
        log::trace!("Incoming data is {:?}.", key_values);
        // maybe filter all the allowed keys here
        let mut storage = self.storage.lock().await;
        storage.extend(
            key_values
                .into_iter()
                .map(|KeyValue { key, value }| (key, value)),
        );

        Ok(Response::new(Empty {}))
    }
}
