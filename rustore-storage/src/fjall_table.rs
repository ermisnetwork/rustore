use std::ops::RangeBounds;

use fjall::{PersistMode, UserKey, UserValue};
use anyhow::Result;
use crate::Table;
#[derive(Clone)]
pub struct FjallTable {
    pub(crate) tree: fjall::Partition,
    pub(crate) db_handle: fjall::Keyspace,
}

impl Table for FjallTable {
    type Value = UserValue;
    type Key = UserKey;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.tree.insert(key, value)?;
        Ok(())
    }

    fn put_if_absent(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        if self.tree.contains_key(key)? {
            Ok(false)
        } else {
            self.tree.insert(key, value)?;
            Ok(true)
        }
    }

    fn update(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        if self.tree.contains_key(key)? {
            self.tree.insert(key, value)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<UserValue>> {
        if let Ok(Some(val)) = self.tree.get(key) {
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    fn get_batch<'a>(&'a self, batch: &[&'a [u8]]) -> Result<Vec<(&'a [u8], UserValue)>> {
        let mut res = Vec::with_capacity(batch.len());
        for &key in batch {
            if let Ok(Some(val)) = self.tree.get(key) {
                res.push((key, val));
            }
        }
        Ok(res)
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        self.tree.remove(key)?;
        Ok(())
    }

    fn increment(&self, key: &[u8]) -> Result<UserValue> {
        !unimplemented!()
    }

    fn decrement(&self, key: &[u8]) -> Result<UserValue> {
        !unimplemented!()
    }
    fn insert_batch(&self, batch: &[(&[u8], &[u8])]) -> Result<()> {
        let mut new_batch = self.db_handle.batch();
        for (key, value) in batch {
            new_batch.insert(&self.tree, *key, *value );
        }
        new_batch.commit()
            .map_err(|e| anyhow::anyhow!(e))
    }
    fn remove_batch(&self, batch: Vec<&[u8]>) -> Result<()> {
        let mut new_batch = self.db_handle.batch();
        for key in batch {
            new_batch.remove(&self.tree, key);
        }
        new_batch.commit()
            .map_err(|e| anyhow::anyhow!(e))
    }
    fn batched_increment<'a>(&self, batch: Vec<&'a [u8]>) -> Result<Vec<(&'a [u8], Self::Value)>> {
        let mut res = Vec::with_capacity(batch.len());
        for key in batch {
            let a = self.increment(key)?;
            res.push((key, a));
        }
        Ok(res)
    }
    fn iter(&self, count: usize) -> Result<Vec<(Self::Key, Self::Value)>> {
        let mut res = Vec::with_capacity(count);
        for entry in self.tree.iter().take(count) {
            if let Ok((key, value)) = entry {
                res.push((key, value));
            }
        }
        Ok(res)
    }
    fn iter_count(&self) -> Result<usize> {
        let res = self.tree.iter().count();
        Ok(res)
    }
    fn range<'a>(&self, range: impl RangeBounds<&'a [u8]>, limit: usize, reverse: bool) -> Result<Vec<(Self::Key, Self::Value)>> {
        let mut res = Vec::with_capacity(limit);
        let iter = self.tree.range(range);
        if reverse {
            for item in iter.rev().take(limit) {
                let (k, v) = item?;
                res.push((k, v));
            }
        } else {
            for item in iter.take(limit) {
                let (k, v) = item?;
                res.push((k, v));
            }
        }
        Ok(res)
    }
    fn range_count<'a>(&self, range: impl RangeBounds<&'a [u8]>, reverse: bool) -> Result<usize> {
        let res = if reverse {
            self.tree.range(range).rev().count()
        } else {
            self.tree.range(range).count()
        };
        Ok(res)
    }
    fn iter_prefix(&self, prefix: &[u8], count: usize) -> Result<Vec<(Self::Key, Self::Value)>> {
        let mut res = Vec::with_capacity(count);
        for entry in self.tree.prefix(prefix).take(count) {
            if let Ok((key, value)) = entry {
                res.push((key, value));
            }
        }
        Ok(res)
    }
    fn iter_prefix_from(&self, prefix: &[u8], from: &[u8], count: usize, reverse: bool) -> Result<Vec<(Self::Key, Self::Value)>> {
        let mut res = Vec::with_capacity(count);
        let iter = self.tree.prefix(prefix);
        if reverse {
            for item in iter.rev().take(count) {
                let (k, v) = item?;
                if k.as_ref() <= from {
                    res.push((k, v));
                }
            }
        } else {
            for item in iter.take(count) {
                let (k, v) = item?;
                if k.as_ref() >= from {
                    res.push((k, v));
                }
            }
        }
        Ok(res)
    }
    fn iter_prefix_count(&self, prefix: &[u8]) -> Result<usize> {
        let res = self.tree.prefix(prefix).count();
        Ok(res)
    }
    fn flush(&self) -> Result<()> {
        self.db_handle.persist(PersistMode::SyncData)?;
        Ok(())
    }
}
