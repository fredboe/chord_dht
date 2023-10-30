use crate::storage::data_distributor::SimpleDataDistributor;
use crate::storage_rpc::data_storage_server::DataStorage;
use crate::storage_rpc::data_storage_server::DataStorageServer;
use crate::storage_rpc::{Empty, Key, KeyValue, KeyValues, OptionalValue, Value};
use anyhow::Result;
use chord::notification::chord_notification::ChordNotifier;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tonic::transport::Server;
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
    distributor: SimpleDataDistributor,
}

impl SimpleDataHandle {
    /// # Explanation
    /// This function starts the data server and the transfer_out process.
    pub async fn start(notifier: Arc<ChordNotifier>) -> Result<Self> {
        // the storage needs to be shared between the server and the distributor
        let storage = Arc::new(Mutex::new(HashMap::new()));
        let shutdown_server = Self::start_server(storage.clone()).await?;
        let distributor = SimpleDataDistributor::new(notifier, storage.clone());
        Ok(SimpleDataHandle {
            shutdown_server,
            distributor,
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

    /// # Explantion
    /// This function stops the transfer_out process and the data server.
    pub async fn stop(self) {
        self.shutdown_server.send(()).ok();
        self.distributor.stop().await;
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
        let storage = self.storage.lock().await;
        log::trace!("Lookup for {}.", key);
        let optional_value = storage.get(&key).cloned().map(|value| Value { value });

        Ok(Response::new(OptionalValue { optional_value }))
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        let KeyValue { key, value } = request.into_inner();
        let mut storage = self.storage.lock().await;
        log::trace!("Put of {} and {}.", key, value,);
        storage.insert(key, value);

        Ok(Response::new(Empty {}))
    }

    /// # Explanation
    /// This function checks with the notifier if the transfer of keys into this node is allowed.
    /// And then it extends the current storage with all the keys in the request.
    async fn transfer_data(&self, request: Request<KeyValues>) -> Result<Response<Empty>, Status> {
        let key_values = request.into_inner().key_values;
        let mut storage = self.storage.lock().await;
        log::trace!(
            "Incoming data is {:?}. Currently we store {:?}.",
            key_values,
            storage
        );
        // maybe filter all the allowed keys here
        storage.extend(
            key_values
                .into_iter()
                .map(|KeyValue { key, value }| (key, value)),
        );

        Ok(Response::new(Empty {}))
    }
}
