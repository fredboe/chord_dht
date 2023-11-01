mod chord_rpc {
    tonic::include_proto!("chord");
}

pub mod chord_handle;
pub mod chord_node;
mod chord_stabilizer;
pub mod finger;
pub mod finger_table;
pub mod notification;
