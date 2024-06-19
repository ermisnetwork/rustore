use crate::{Operation, Response};
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};

impl OwnedResponse {
    #[inline(always)]
    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        let response = Response::from_bytes(&bytes)?;
        let owned_response = match response {
            Response::Ok => OwnedResponse::Ok,
            Response::PutError => OwnedResponse::PutError,
            Response::NoMatchingKey => OwnedResponse::NoMatchingKey,
            Response::KeyExists => OwnedResponse::KeyExists,
            Response::Value(value) => OwnedResponse::Value(bytes.slice_ref(value)),
            Response::Entry(key, value) => {
                OwnedResponse::Entry(bytes.slice_ref(key), bytes.slice_ref(value))
            }
            Response::Entries(entries) => OwnedResponse::Entries(
                entries
                    .into_iter()
                    .map(|(k, v)| (bytes.slice_ref(k), bytes.slice_ref(v)))
                    .collect(),
            ),
            Response::IterEnd => OwnedResponse::IterEnd,
            Response::InvalidCommand => OwnedResponse::InvalidCommand,
            Response::BatchFailed => OwnedResponse::BatchFailed,
            Response::WakePrefix(table, prefix) => {
                OwnedResponse::WakePrefix(bytes.slice_ref(table), bytes.slice_ref(prefix))
            }
            Response::WatchPrefixUnsupported => OwnedResponse::WatchPrefixUnsupported,
        };
        Ok(owned_response)
    }
}

#[derive(Debug)]
#[repr(u8)]
pub enum OwnedResponse {
    Ok = 0,
    PutError = 1,
    NoMatchingKey = 2,
    KeyExists = 3,
    Value(Bytes) = 4,
    Entry(Bytes, Bytes) = 5,
    Entries(Vec<(Bytes, Bytes)>) = 6,
    IterEnd = 7,
    InvalidCommand = 8,
    BatchFailed = 9,
    WakePrefix(Bytes, Bytes) = 10,
    WatchPrefixUnsupported = 11,
}

#[derive(PartialEq, Clone, Debug)]
pub struct ClientSidePipedResponse {
    pub id: u32,
    pub response: Bytes,
}

impl ClientSidePipedResponse {
    #[inline(always)]
    pub fn from_bytes(mut bytes: BytesMut) -> Result<Self> {
        let id = bytes.get_u32();
        let response = bytes.freeze();
        Ok(ClientSidePipedResponse { id, response })
    }
}
#[derive(Debug)]
#[repr(u8)]
pub enum OwnedOperation {
    Get(Bytes) = 0,
    Put(Bytes, Bytes) = 1,
    PutIfAbsent(Bytes, Bytes) = 2,
    Remove(Bytes) = 3,
    Update(Bytes, Bytes) = 4,
    Iter(u32) = 5,
    IterPrefix(Bytes, u32) = 6,
    IterPrefixFrom(Bytes, Bytes, u32) = 7,
    IterPrefixFromRev(Bytes, Bytes, u32) = 8,
    IterFrom(Bytes, u32) = 9,
    Increment(Bytes) = 10,
    IterCount = 11,
    IterPrefixCount(Bytes) = 12,
    IterFromCount(Bytes) = 13,
    IterFromRev(Bytes, u32) = 14,
    IterFromRevCount(Bytes) = 15,
    BatchedPut(Vec<(Bytes, Bytes)>) = 16,
    BatchedGet(Vec<Bytes>) = 17,
    BatchedIncrement(Vec<Bytes>) = 18,
    BatchedRemove(Vec<Bytes>) = 19,
    WatchPrefix(Bytes) = 20,
    PutAndWake(Bytes, Bytes, Bytes) = 21,
    BatchedPutAndWake(Vec<(Bytes, Bytes)>, Vec<Bytes>) = 22,
    GetDelayed(u32, Bytes) = 23,
    DropTable = 24,
    Decrement(Bytes) = 25,
    Range(Bytes, Bytes, u32) = 26,
    RangeCount(Bytes, Bytes) = 27,
}

impl OwnedOperation {
    #[inline(always)]
    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        let operation = Operation::from_bytes(&bytes)?;
        let owned_operation = match operation {
            Operation::Get(key) => OwnedOperation::Get(bytes.slice_ref(key)),
            Operation::Put(key, value) => {
                OwnedOperation::Put(bytes.slice_ref(key), bytes.slice_ref(value))
            }
            Operation::PutIfAbsent(key, value) => {
                OwnedOperation::PutIfAbsent(bytes.slice_ref(key), bytes.slice_ref(value))
            }
            Operation::Remove(key) => OwnedOperation::Remove(bytes.slice_ref(key)),
            Operation::Update(key, value) => {
                OwnedOperation::Update(bytes.slice_ref(key), bytes.slice_ref(value))
            }
            Operation::Iter(count) => OwnedOperation::Iter(count),
            Operation::IterPrefix(prefix, count) => {
                OwnedOperation::IterPrefix(bytes.slice_ref(prefix), count)
            }
            Operation::IterPrefixFrom(prefix, from, count) => OwnedOperation::IterPrefixFrom(
                bytes.slice_ref(prefix),
                bytes.slice_ref(from),
                count,
            ),
            Operation::IterPrefixFromRev(prefix, from, count) => OwnedOperation::IterPrefixFromRev(
                bytes.slice_ref(prefix),
                bytes.slice_ref(from),
                count,
            ),
            Operation::IterFrom(from, count) => {
                OwnedOperation::IterFrom(bytes.slice_ref(from), count)
            }
            Operation::Increment(key) => OwnedOperation::Increment(bytes.slice_ref(key)),
            Operation::IterCount => OwnedOperation::IterCount,
            Operation::IterPrefixCount(prefix) => {
                OwnedOperation::IterPrefixCount(bytes.slice_ref(prefix))
            }
            Operation::IterFromCount(from) => OwnedOperation::IterFromCount(bytes.slice_ref(from)),
            Operation::IterFromRev(from, count) => {
                OwnedOperation::IterFromRev(bytes.slice_ref(from), count)
            }
            Operation::IterFromRevCount(from) => {
                OwnedOperation::IterFromRevCount(bytes.slice_ref(from))
            }
            Operation::BatchedPut(entries) => OwnedOperation::BatchedPut(
                entries
                    .into_iter()
                    .map(|(k, v)| (bytes.slice_ref(k), bytes.slice_ref(v)))
                    .collect(),
            ),
            Operation::BatchedGet(keys) => {
                OwnedOperation::BatchedGet(keys.into_iter().map(|k| bytes.slice_ref(k)).collect())
            }
            Operation::BatchedIncrement(keys) => OwnedOperation::BatchedIncrement(
                keys.into_iter().map(|k| bytes.slice_ref(k)).collect(),
            ),
            Operation::BatchedRemove(keys) => OwnedOperation::BatchedRemove(
                keys.into_iter().map(|k| bytes.slice_ref(k)).collect(),
            ),
            Operation::WatchPrefix(prefix) => OwnedOperation::WatchPrefix(bytes.slice_ref(prefix)),
            Operation::PutAndWake(key, value, wake) => OwnedOperation::PutAndWake(
                bytes.slice_ref(key),
                bytes.slice_ref(value),
                bytes.slice_ref(wake),
            ),
            Operation::BatchedPutAndWake(entries, prefixes) => OwnedOperation::BatchedPutAndWake(
                entries
                    .into_iter()
                    .map(|(k, v)| (bytes.slice_ref(k), bytes.slice_ref(v)))
                    .collect(),
                prefixes.into_iter().map(|p| bytes.slice_ref(p)).collect(),
            ),
            Operation::GetDelayed(delay, key) => {
                OwnedOperation::GetDelayed(delay, bytes.slice_ref(key))
            },
            Operation::DropTable => OwnedOperation::DropTable,
            Operation::Decrement(key) => OwnedOperation::Decrement(bytes.slice_ref(key)),
            Operation::Range(start, end, limit) => {
                OwnedOperation::Range(bytes.slice_ref(start), bytes.slice_ref(end), limit)
            }
            Operation::RangeCount(start, end) => {
                OwnedOperation::RangeCount(bytes.slice_ref(start), bytes.slice_ref(end))
            }
        };
        Ok(owned_operation)
    }
}
