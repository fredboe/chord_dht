use crate::storage::data_handler::SimpleDataHandle;
use crate::storage_rpc::{Key, KeyValue};
use crate::utils::create_data_client;
use anyhow::{anyhow, Result};
use chord::chord_handle::ChordHandle;
use chord::notification::chord_notification::ChordNotifier;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::Request;

/// # Explanation
/// A basic implementation of a chord network using a `HashMap` for data storage. This struct facilitates
/// the creation of a simplified chord network without built-in protections such as data validation or
/// authentication. It is not recommended for use in production environments due to these limitations.
///
/// The implementation operates through two primary processes:
/// 1. Chord Protocol Process: Handles the operations related to the chord protocol, including node
///    communication and network maintenance.
/// 2. Data Management Process: Manages the network's data storage, utilizing a basic `HashMap<String, String>`
///    to store and retrieve data.
///
/// These processes operate independently for the most part, but there are instances where communication
/// between them is necessary, particularly for coordinating data transfers across nodes. This inter-process
/// communication is achieved through a notification system, where the chord protocol process can send
/// notifications (e.g., `DataTo(...)`) to the data management process for handling.
pub struct SimpleChordDHT {
    chord_handle: ChordHandle,
    data_handle: SimpleDataHandle,
}

impl SimpleChordDHT {
    /// # Explanation
    /// This function creates a new chord network. For the finger table initialization process the own ip is needed.
    pub async fn new_network(own_addr: SocketAddr) -> Result<Self> {
        let notifier = Arc::new(ChordNotifier::new());
        let data_handle = SimpleDataHandle::start(notifier.clone());
        let chord_handle = ChordHandle::new_network(own_addr, notifier.clone()).await?;
        Ok(SimpleChordDHT {
            chord_handle,
            data_handle,
        })
    }

    /// # Explanation
    /// This function joins the chord network the introducer node is located in.
    pub async fn join(introducer: SocketAddr, own_addr: SocketAddr) -> Result<Self> {
        let notifier = Arc::new(ChordNotifier::new());
        let data_server_handle = SimpleDataHandle::start(notifier.clone());
        let chord_handle = ChordHandle::join(introducer, own_addr, notifier.clone()).await?;
        Ok(SimpleChordDHT {
            chord_handle,
            data_handle: data_server_handle,
        })
    }

    /// # Explanation
    /// This function finds the node that is responsible for storing the given key. Then a lookup-request
    /// is performed on it.
    pub async fn lookup(&self, key: &str) -> Result<String> {
        let store_ip = self.chord_handle.find_node(key).await?.ip();
        log::trace!("Store node for {} is {}.", key, store_ip);
        let mut data_client = create_data_client(store_ip).await?;
        let value_response = data_client
            .lookup(Request::new(Key {
                key: key.to_string(),
            }))
            .await?;

        value_response
            .into_inner()
            .optional_value
            .map(|value| value.value)
            .ok_or(anyhow!("The value is not present."))
    }

    pub async fn retry_lookup(&self, key: &str, retries: usize) -> Option<String> {
        for _ in 0..retries {
            let value = self.lookup(key).await.ok();
            if value.is_some() {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }

    /// # Explanation
    /// This function finds the node that is responsible for storing the given key. Then a put-request
    /// is performed on it.
    pub async fn put(&self, key: String, value: String) -> Result<()> {
        let store_ip = self.chord_handle.find_node(&key).await?.ip();
        let mut data_client = create_data_client(store_ip).await?;
        data_client
            .put(Request::new(KeyValue { key, value }))
            .await?;
        Ok(())
    }

    /// # Explantion
    /// With this function one can leave the chord network.
    ///
    /// Be sure to call this function at the end so that no keys are lost in the network.
    ///
    /// (Note: This function should probably be called in the drop-function.)
    pub async fn leave(mut self) -> Result<()> {
        self.chord_handle.leave().await?;
        tokio::time::sleep(Duration::from_millis(100)).await; // give the notification process some time
        self.data_handle.stop();
        Ok(())
    }
}
