mod chord_rpc {
    tonic::include_proto!("chord");
}

mod storage_rpc {
    tonic::include_proto!("storage");
}

pub mod chord_handle;
pub mod chord_node;
mod chord_stabilizer;
pub mod dht_demo;
pub mod finger_table;
pub mod notification;
pub mod storage;
