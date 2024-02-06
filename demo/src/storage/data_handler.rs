use crate::storage::notification_handler::SimpleNotificationHandler;
use crate::storage_rpc::data_storage_server::{DataStorage, DataStorageServer};
use crate::storage_rpc::{Empty, Key, KeyValue, KeyValues, OptionalValue, Value};
use crate::utils::{NodeDataState, DATA_PORT};
use anyhow::Result;
use chord::notification::chord_notification::ChordNotifier;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio::time::interval;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// # Explanation
/// The SimpleDataHandle is mainly used for starting (and stopping) all the data processes.
/// Including starting the data server and the notification process which is used for communicating with the chord process
/// (e.g. for handling data transfers).
pub struct SimpleDataHandle {
    data_server_shutdown: Option<oneshot::Sender<()>>,
    notification_shutdown: Option<oneshot::Sender<()>>,
}

impl SimpleDataHandle {
    pub fn start(notifier: Arc<ChordNotifier>) -> Self {
        let node_data_state = Arc::new(Mutex::new(NodeDataState::new()));
        let data_server_shutdown = Self::start_data_server(node_data_state.clone());
        let notification_shutdown = Self::start_notification_handler(notifier, node_data_state);

        Self {
            data_server_shutdown: Some(data_server_shutdown),
            notification_shutdown: Some(notification_shutdown),
        }
    }

    pub fn start_data_server(node_data_state: Arc<Mutex<NodeDataState>>) -> oneshot::Sender<()> {
        let (data_server_shutdown, data_server_shutdown_receiver) = oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .add_service(DataStorageServer::new(SimpleDataServer::new(
                    node_data_state,
                )))
                .serve_with_shutdown(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DATA_PORT),
                    async move {
                        data_server_shutdown_receiver.await.ok();
                        log::trace!("Data server shutdown.");
                    },
                )
                .await
                .ok();
        });
        data_server_shutdown
    }

    pub fn start_notification_handler(
        notifier: Arc<ChordNotifier>,
        node_data_state: Arc<Mutex<NodeDataState>>,
    ) -> oneshot::Sender<()> {
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(10));
            let notification_handler =
                SimpleNotificationHandler::from_notifier(notifier, node_data_state).await;

            if let Ok(mut notification_handler) = notification_handler {
                while shutdown_receiver.try_recv().is_err() {
                    interval.tick().await;
                    notification_handler.handle_notification().await;
                }
            } else {
                log::error!(
                    "Was not able to create a notification handler. \
                    Therefore there will not be any data transfers."
                );
            }
        });
        shutdown_sender
    }

    pub fn stop(&mut self) {
        if let Some(notification_shutdown) = self.notification_shutdown.take() {
            notification_shutdown.send(()).ok();
        }
        if let Some(data_server_shutdown) = self.data_server_shutdown.take() {
            data_server_shutdown.send(()).ok();
        }
    }
}

impl Drop for SimpleDataHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// # Explanation
/// Implements a data server for handling basic data storage and retrieval operations within a distributed system.
///
/// This server offers straightforward functions for manipulating data, focusing on key-value storage paradigms. It is designed
/// to facilitate easy and efficient data management on individual nodes within a distributed network, such as a chord network.
///
/// ## Functions Overview:
/// - `lookup(key) -> Option<Value>`: Searches for the specified key within this node's storage.
///                                   Returns the associated value if found, otherwise returns `None` to indicate the absence of the key.
/// - `put(key, value)`: Inserts or updates the given key-value pair in this node's local storage.
///                      Ensures that data is readily accessible on this node for future queries.
/// - `transfer_data(key_values)`: Receives and stores a batch of key-value pairs from another node.
///                                This function is typically used during node join or leave operations to maintain data consistency across the network.
#[derive(Clone)]
pub struct SimpleDataServer {
    node_state: Arc<Mutex<NodeDataState>>,
}

impl SimpleDataServer {
    pub fn new(node_state: Arc<Mutex<NodeDataState>>) -> Self {
        Self { node_state }
    }
}

#[tonic::async_trait]
impl DataStorage for SimpleDataServer {
    async fn lookup(&self, request: Request<Key>) -> Result<Response<OptionalValue>, Status> {
        let key = request.into_inner().key;
        log::trace!("Lookup for {}.", key);
        let node_state = self.node_state.lock().await;
        let optional_value = node_state
            .data
            .get(&key)
            .cloned()
            .map(|value| Value { value });

        Ok(Response::new(OptionalValue { optional_value }))
    }

    async fn put(&self, request: Request<KeyValue>) -> Result<Response<Empty>, Status> {
        let KeyValue { key, value } = request.into_inner();
        log::trace!("Put of {} and {}.", key, value,);
        let mut node_state = self.node_state.lock().await;
        node_state.data.insert(key, value);

        Ok(Response::new(Empty {}))
    }

    async fn transfer_data(&self, request: Request<KeyValues>) -> Result<Response<Empty>, Status> {
        let key_values = request.into_inner().key_values;
        let mut node_state = self.node_state.lock().await;
        log::trace!(
            "Incoming data is {:?}. Currently we store {:?}.",
            key_values,
            node_state.data
        );

        node_state.data.extend(
            key_values
                .into_iter()
                .map(|KeyValue { key, value }| (key, value)),
        );

        Ok(Response::new(Empty {}))
    }
}
