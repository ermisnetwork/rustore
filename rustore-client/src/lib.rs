pub mod hybrid_client;
pub mod piped_client;
pub mod pooled_client;
pub(crate) mod reconnect;
use std::{error::Error, fmt};

#[derive(Debug)]
pub enum DataStoreError {
    ConnectFailed,
    GetFailed,
    InsertFailed,
    UpsertFailed,
    InsertBatchFailed,
    InsertBatchFailedAll,
    InsertBatchFailedSome,
    RemoveFailed,
    RemoveNotExisted,
    IterNextFailed,
    IterFailed,
    IterFromFailed,
    IncrementFailed,
    IncrementBatchFailed,
    IncrementBatchFailedAll,
    IncrementBatchFailedSome,
    ScanPrefixFailed,
    WatchPrefixFailed,
    ClearFailed,
    GetCountFailed,
    GetCountFromFailed,
    GetCountPrefixFailed,
    RemoveBatchFailed,
}

impl fmt::Display for DataStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataStoreError::ConnectFailed => write!(f, "Connect failed"),
            DataStoreError::GetFailed => write!(f, "Get failed"),
            DataStoreError::InsertFailed => write!(f, "Insert failed"),
            DataStoreError::UpsertFailed => write!(f, "Upsert failed"),
            DataStoreError::InsertBatchFailed => write!(f, "Inser batch failed"),
            DataStoreError::InsertBatchFailedAll => write!(f, "Inser batch failed all"),
            DataStoreError::InsertBatchFailedSome => write!(f, "Inser batch failed some"),
            DataStoreError::RemoveFailed => write!(f, "Remove failed"),
            DataStoreError::RemoveNotExisted => write!(f, "Remove not existed"),
            DataStoreError::IterFailed => write!(f, "Iter failed"),
            DataStoreError::IterNextFailed => write!(f, "Iter next failed"),
            DataStoreError::IterFromFailed => write!(f, "Iter from failed"),
            DataStoreError::IncrementFailed => write!(f, "Increment failed"),
            DataStoreError::IncrementBatchFailed => write!(f, "Increment batch failed"),
            DataStoreError::IncrementBatchFailedAll => write!(f, "Increment batch failed all"),
            DataStoreError::IncrementBatchFailedSome => write!(f, "Increment batch failed some"),
            DataStoreError::ScanPrefixFailed => write!(f, "Scan prefix failed"),
            DataStoreError::WatchPrefixFailed => write!(f, "Watch prefix failed"),
            DataStoreError::ClearFailed => write!(f, "Clear failed"),
            DataStoreError::GetCountFailed => write!(f, "Get count failed"),
            DataStoreError::GetCountFromFailed => write!(f, "Iter count from failed"),
            DataStoreError::GetCountPrefixFailed => write!(f, "Iter count prefix failed"),
            DataStoreError::RemoveBatchFailed => write!(f, "Remove batch failed"),
        }
    }
}

impl Error for DataStoreError {}
