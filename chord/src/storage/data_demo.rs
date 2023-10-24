use crate::notification::chord_notification::{
    ChordCharacteristic, ChordNotification, ChordNotifier, TransferNotification,
};
use crate::notification::notifier::Subscription;
use crate::storage_rpc::data_storage_client::DataStorageClient;
use crate::storage_rpc::data_storage_server::DataStorage;
use crate::storage_rpc::data_storage_server::DataStorageServer;
use crate::storage_rpc::{Empty, Key, KeyValue, KeyValues, OptionalValue, Value};
use anyhow::Result;
use futures::StreamExt;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Status};

pub const DATA_PORT: u16 = 43355;

pub struct SimpleDataHandle {
    shutdown_server: oneshot::Sender<()>,
    transfer_out_handle: JoinHandle<()>,
}

impl SimpleDataHandle {
    pub async fn start(notifier: Arc<ChordNotifier>) -> Result<Self> {
        let storage = Arc::new(Mutex::new(HashMap::new()));
        let shutdown_server = Self::start_server(notifier.clone(), storage.clone()).await?;
        let transfer_out_handle = Self::start_transfer_out_task(notifier, storage).await;
        Ok(SimpleDataHandle {
            shutdown_server,
            transfer_out_handle,
        })
    }

    async fn start_server(
        notifier: Arc<ChordNotifier>,
        storage: Arc<Mutex<HashMap<String, String>>>,
    ) -> Result<oneshot::Sender<()>> {
        let storage_service = SimpleStorage::new(notifier.clone(), storage.clone());
        let (shudown_sender, shutdown_receiver) = oneshot::channel();
        tokio::task::spawn(async move {
            Server::builder()
                .add_service(DataStorageServer::new(storage_service))
                .serve_with_shutdown(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DATA_PORT),
                    async move {
                        shutdown_receiver.await.ok();
                    },
                )
                .await
                .ok();
        });
        Ok(shudown_sender)
    }

    async fn start_transfer_out_task(
        notifier: Arc<ChordNotifier>,
        storage: Arc<Mutex<HashMap<String, String>>>,
    ) -> JoinHandle<()> {
        let task_handle = tokio::task::spawn(async move {
            let subscription = notifier.subscribe(ChordCharacteristic::AnyDataTo).await;
            if let Some(subscription) = subscription {
                Self::handle_transfer_out_subscription(subscription, storage).await;
            }
        });
        task_handle
    }

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

    async fn transport_data_out(
        transfer_notification: TransferNotification,
        storage: Arc<Mutex<HashMap<String, String>>>,
    ) -> Result<()> {
        let transfer_ip = transfer_notification.ip;
        let mut data_client = Self::create_data_client(transfer_ip).await?;

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

        data_client
            .transfer_data(Request::new(KeyValues { key_values: out }))
            .await?;
        Ok(())
    }

    pub async fn stop(self) {
        self.transfer_out_handle.await.ok();
        self.shutdown_server.send(()).ok();
    }

    pub async fn create_data_client(addr: IpAddr) -> Result<DataStorageClient<Channel>> {
        let uri = Uri::builder()
            .scheme("http")
            .authority(format!("{}:{}", addr, DATA_PORT))
            .path_and_query("/")
            .build()?;
        let data_client = DataStorageClient::connect(uri).await?;

        Ok(data_client)
    }
}

pub struct SimpleStorage {
    storage: Arc<Mutex<HashMap<String, String>>>,
    notifier: Arc<ChordNotifier>,
}

impl SimpleStorage {
    pub fn new(notifier: Arc<ChordNotifier>, storage: Arc<Mutex<HashMap<String, String>>>) -> Self {
        SimpleStorage { storage, notifier }
    }
}

#[tonic::async_trait]
impl DataStorage for SimpleStorage {
    async fn lookup(&self, request: Request<Key>) -> Result<Response<OptionalValue>, Status> {
        let storage = self.storage.lock().await;
        let optional_value = storage
            .get(&request.into_inner().key)
            .cloned()
            .map(|value| Value { value });

        Ok(Response::new(OptionalValue { optional_value }))
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        let mut storage = self.storage.lock().await;
        let KeyValue { key, value } = request.into_inner();
        storage.insert(key, value);

        Ok(Response::new(Empty {}))
    }

    async fn transfer_data(&self, request: Request<KeyValues>) -> Result<Response<Empty>, Status> {
        const NO_REMOTE_IP: &str = "Was not able to extract the remote ip out of the request.";
        const NO_ACCESS_GRANTED: &str = "The access to transfer data was not granted.";

        let remote_ip = request
            .remote_addr()
            .ok_or(Status::unavailable(NO_REMOTE_IP))?
            .ip();
        if self
            .notifier
            .has_and_remove(&ChordNotification::DataFrom(
                TransferNotification::allow_all(remote_ip),
            ))
            .await
        {
            let mut storage = self.storage.lock().await;
            storage.extend(
                request
                    .into_inner()
                    .key_values
                    .into_iter()
                    .map(|KeyValue { key, value }| (key, value)),
            );

            Ok(Response::new(Empty {}))
        } else {
            Err(Status::unavailable(NO_ACCESS_GRANTED))
        }
    }
}
