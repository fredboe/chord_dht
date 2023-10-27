use crate::storage::data_demo::SimpleDataHandle;
use crate::storage_rpc::{Key, KeyValue};
use anyhow::{anyhow, Result};
use chord::chord_handle::ChordHandle;
use chord::notification::chord_notification::ChordNotifier;
use std::net::IpAddr;
use std::sync::Arc;
use tonic::Request;

/// # Explanation
/// This is a built-in struct for creating your own simple chord network. As a data store it uses a normal HashMap.
/// There is no protection (e.g. data validation or authentication) built in the data server. Therfore, do not use
/// this sturct in a professional environment.
///
/// The SimpleChordDHT struct provides a lookup and a put function. The key and value type is a normal string.
pub struct SimpleChordDHT {
    chord_handle: ChordHandle,
    data_server_handle: SimpleDataHandle,
}

impl SimpleChordDHT {
    /// # Explanation
    /// This function creates a new chord network. For the finger table initialization process the own ip is needed.
    pub async fn new_network(own_ip: IpAddr) -> Result<Self> {
        let notifier = Arc::new(ChordNotifier::new());
        let data_server_handle = SimpleDataHandle::start(notifier.clone()).await?;
        let chord_handle = ChordHandle::new_network(own_ip, notifier.clone()).await?;
        Ok(SimpleChordDHT {
            chord_handle,
            data_server_handle,
        })
    }

    /// # Explanation
    /// This function join the chord network the introducer node is located in.
    pub async fn join(introducer: IpAddr) -> Result<Self> {
        let notifier = Arc::new(ChordNotifier::new());
        let data_server_handle = SimpleDataHandle::start(notifier.clone()).await?;
        let chord_handle = ChordHandle::join(introducer, notifier.clone()).await?;
        Ok(SimpleChordDHT {
            chord_handle,
            data_server_handle,
        })
    }

    /// # Explanation
    /// This function finds the node that is responsible for storing the given key. Then a lookup-request
    /// is performed on it.
    pub async fn lookup(&self, key: &str) -> Result<String> {
        let store_ip = self.chord_handle.find_node(key).await?;
        let mut data_client = SimpleDataHandle::create_data_client(store_ip).await?;
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

    /// # Explanation
    /// This function finds the node that is responsible for storing the given key. Then a put-request
    /// is performed on it.
    pub async fn put(&self, key: String, value: String) -> Result<()> {
        let store_ip = self.chord_handle.find_node(&key).await?;
        let mut data_client = SimpleDataHandle::create_data_client(store_ip).await?;
        data_client
            .put(Request::new(KeyValue { key, value }))
            .await?;
        Ok(())
    }

    /// # Explantion
    /// With this function one can leave the chord network.
    ///
    /// Be sure to call this function at the end so that no keys are lost in the network.
    pub async fn leave(self) -> Result<()> {
        self.chord_handle.leave().await?;
        self.data_server_handle.stop().await;
        Ok(())
    }
}
