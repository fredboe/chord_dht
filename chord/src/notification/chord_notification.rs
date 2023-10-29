use crate::finger_table::{compute_chord_id, in_store_interval};
use crate::notification::notifier::Notifier;
use std::net::IpAddr;
use std::ops::Range;

pub type ChordNotifier = Notifier<ChordCharacteristic, ChordNotification>;

/// # Explantion
/// The ChordNotification type consists of two elements.
/// - The DataTo element which specifies that data should be send to another node.
/// - The DataFrom element which specifies that data can come from another node.
/// - And the StoreRangeUpdate element which specifies the current range of ids this node should store.
///   (so most of the time just the last of these events is interesting)
#[derive(Clone, Eq, PartialEq, Hash)]
pub enum ChordNotification {
    DataTo(TransferNotification),
    DataFrom(TransferNotification),
    StoreRangeUpdate(Range<u64>),
}

/// # Explanation
/// The ChordCharacteristic type consists of four elements.
/// - The AnyDataTo element which accepts any notification of the DataTo format.
/// - The AnyDataFrom element which acceps any notification of the DataFrom format.
/// - The AnyStoreRange element which accepts any notification of the StoreRange format.
/// - The DataTo element which accepts a specific DataTo notification.
/// - The DataFrom element which accepts a specific DataFrom notification.
/// - And the StoreRange element which accepts a specific range update.
#[derive(Clone, Eq, PartialEq, Hash)]
pub enum ChordCharacteristic {
    AnyDataTo,
    AnyDataFrom,
    AnyStoreRange,
    DataTo(TransferNotification),
    DataFrom(TransferNotification),
    StoreRange(Range<u64>),
}

impl Into<Vec<ChordCharacteristic>> for ChordNotification {
    fn into(self) -> Vec<ChordCharacteristic> {
        match self {
            ChordNotification::DataTo(ip) => {
                vec![
                    ChordCharacteristic::AnyDataTo,
                    ChordCharacteristic::DataTo(ip),
                ]
            }
            ChordNotification::DataFrom(ip) => vec![
                ChordCharacteristic::AnyDataFrom,
                ChordCharacteristic::DataFrom(ip),
            ],
            ChordNotification::StoreRangeUpdate(range) => vec![
                ChordCharacteristic::AnyStoreRange,
                ChordCharacteristic::StoreRange(range),
            ],
        }
    }
}

/// # Explanation
/// A TransferNotification consists of a chord node's ip and an interval on the chord ring.
/// In order to ease the specification of the interval we have two modes.
/// - The normal mode in which one needs to specify the interval borders directly.
/// - And the allow_all mode in which every element on the chord ring is accepted.
#[derive(Copy, Clone, Hash)]
pub struct TransferNotification {
    pub ip: IpAddr,
    left_id: u64,
    right_id: u64,
    allow_all: bool,
}

impl TransferNotification {
    pub fn new(ip: IpAddr, left_id: u64, right_id: u64, allow_all: bool) -> Self {
        TransferNotification {
            ip,
            left_id,
            right_id,
            allow_all,
        }
    }

    /// # Explanation
    /// This function creates a TransferNotification that should transfer all
    /// the ids in the range from left_id to right_id to the specified ip address.
    pub fn from_range(ip: IpAddr, left_id: u64, right_id: u64) -> Self {
        Self::new(ip, left_id, right_id, false)
    }

    /// # Explanation
    /// This function creates a TransferNotification that should transfer all ids (that are
    /// currently stored in this node).
    pub fn allow_all(ip: IpAddr) -> Self {
        Self::new(ip, 0, 0, true)
    }

    /// # Explanation
    /// With this function one can check if a specific key is allowed in this transfer operation.
    pub fn should_transfer(&self, key: &str) -> bool {
        let key_id = compute_chord_id(key);
        self.allow_all || in_store_interval(key_id, self.left_id, self.right_id)
    }
}

impl PartialEq for TransferNotification {
    fn eq(&self, other: &Self) -> bool {
        let same_range = self.allow_all == other.allow_all
            || (self.left_id == other.left_id && self.right_id == other.right_id);
        self.ip == other.ip && same_range
    }
}

impl Eq for TransferNotification {}
