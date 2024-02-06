use crate::storage_rpc::data_storage_client::DataStorageClient;
use chord::finger_table::ChordId;
use std::collections::HashMap;
use std::net::IpAddr;
use std::ops::Range;
use std::time::Duration;
use tokio::time::timeout;
use tonic::transport::{Channel, Uri};

pub const DATA_PORT: u16 = 43355;

pub struct NodeDataState {
    pub(crate) range: Range<ChordId>,
    pub(crate) data: HashMap<String, String>,
}

impl NodeDataState {
    pub fn new() -> Self {
        Self {
            range: 0..0,
            data: HashMap::new(),
        }
    }
}

/// # Explanation
/// This function creates a client to the data server of the given ip.
pub async fn create_data_client(addr: IpAddr) -> anyhow::Result<DataStorageClient<Channel>> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(format!("{}:{}", addr, DATA_PORT))
        .path_and_query("/")
        .build()?;
    let data_client = timeout(Duration::from_secs(1), DataStorageClient::connect(uri)).await??;

    Ok(data_client)
}
