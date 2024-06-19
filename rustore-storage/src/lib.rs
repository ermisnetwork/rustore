pub mod sled_database;
pub mod sled_table;
pub mod fjall_database;
pub mod fjall_table;
use std::ops::RangeBounds;

use anyhow::Result;
use bytes::Bytes;

pub trait Database {
    type Item: Table;
    fn open_or_create(path: impl AsRef<std::path::Path>) -> Result<Self>
    where
        Self: Sized;
    fn open_or_create_table<'a>(&'a mut self, id: Bytes) -> Result<&'a Self::Item>;
    fn drop_table(&mut self, id: Bytes) -> Result<()>;
    fn flush(&self) -> Result<()>;
}

pub trait Table {
    type Value: AsRef<[u8]>;
    type Key: AsRef<[u8]>;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn put_if_absent(&self, key: &[u8], value: &[u8]) -> Result<bool>;
    fn update(&self, key: &[u8], value: &[u8]) -> Result<bool>;
    fn get(&self, key: &[u8]) -> Result<Option<Self::Value>>;
    fn get_batch<'a>(&'a self, batch: &[&'a [u8]]) -> Result<Vec<(&'a [u8], Self::Value)>>;
    fn remove(&self, key: &[u8]) -> Result<()>;
    fn increment(&self, key: &[u8]) -> Result<Self::Value>;
    fn decrement(&self, key: &[u8]) -> Result<Self::Value>;
    fn insert_batch(&self, batch: &[(&[u8], &[u8])]) -> Result<()>;
    fn remove_batch(&self, batch: Vec<&[u8]>) -> Result<()>;
    fn batched_increment<'a>(&self, batch: Vec<&'a [u8]>) -> Result<Vec<(&'a [u8], Self::Value)>>;
    fn iter(&self, count: usize) -> Result<Vec<(Self::Key, Self::Value)>>;
    fn iter_count(&self) -> Result<usize>;
    fn range<'a>(&self, range: impl RangeBounds<&'a [u8]>, limit: usize, reverse: bool) -> Result<Vec<(Self::Key, Self::Value)>>;
    fn range_count<'a>(&self, range: impl RangeBounds<&'a [u8]>, reverse: bool) -> Result<usize>;
    fn iter_prefix(&self, prefix: &[u8], count: usize) -> Result<Vec<(Self::Key, Self::Value)>>;
    fn iter_prefix_from(&self, prefix: &[u8], from: &[u8], count: usize, reverse: bool) -> Result<Vec<(Self::Key, Self::Value)>>;
    fn iter_prefix_count(&self, prefix: &[u8]) -> Result<usize>;
    fn flush(&self) -> Result<()>;
}