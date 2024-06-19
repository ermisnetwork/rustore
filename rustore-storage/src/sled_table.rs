use anyhow::Result;
use sled::{Batch, CompareAndSwapError, CompareAndSwapSuccess, InlineArray, Tree};
use std::{cmp::min, ops::RangeBounds};

use crate::Table;

#[derive(Clone)]
pub struct SledTable {
    pub(crate) tree: Tree<128>,
}

impl Table for SledTable {
    type Value = InlineArray;
    type Key = InlineArray;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.tree.insert(key, value)?;
        Ok(())
    }

    fn put_if_absent(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        let res = self
            .tree
            .compare_and_swap(key, None::<&[u8]>, Some(value))??;
        if res.new_value.is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn update(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        let updated = self.tree.fetch_and_update(key, |old| old.map(|_| value))?;
        Ok(updated.is_some())
    }

    fn get(&self, key: &[u8]) -> Result<Option<InlineArray>> {
        if let Ok(Some(val)) = self.tree.get(key) {
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    fn get_batch<'a>(&'a self, batch: &[&'a [u8]]) -> Result<Vec<(&'a [u8], InlineArray)>> {
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

    fn increment(&self, key: &[u8]) -> Result<InlineArray> {
        let mut old = self.tree.get(key)?;
        loop {
            let new = match &old {
                Some(bytes) => {
                    let number = u64::from_be_bytes(bytes.as_ref().try_into()?);
                    number + 1
                }
                None => 1,
            };
            let new = Some(new.to_be_bytes());
            let res = self.tree.compare_and_swap(key, old.as_ref(), new.as_ref());
            match res {
                Ok(Ok(CompareAndSwapSuccess { new_value, previous_value: _})) => return new_value.ok_or(anyhow::anyhow!("increment failed")),
                Ok(Err(CompareAndSwapError{ current, proposed: _ })) => {
                    old = current;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn decrement(&self, key: &[u8]) -> Result<InlineArray> {
        let mut old = self.tree.get(key)?;
        loop {
            let new = match &old {
                Some(bytes) => {
                    let number = u64::from_be_bytes(bytes.as_ref().try_into()?);
                    if number == 0 {
                        return Err(anyhow::anyhow!("decrement failed: value is already 0"));
                    }
                    number - 1
                }
                None => return Err(anyhow::anyhow!("decrement failed: value not found")),
            };
            let new = Some(new.to_be_bytes());
            let res = self.tree.compare_and_swap(key, old.as_ref(), new.as_ref());
            match res {
                Ok(Ok(CompareAndSwapSuccess {new_value, previous_value: _})) => return new_value.ok_or(anyhow::anyhow!("decrement failed")),
                Ok(Err(CompareAndSwapError{ current, proposed: _ })) => {
                    old = current;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn insert_batch(&self, batch: &[(&[u8], &[u8])]) -> Result<()> {
        let mut new_batch = Batch::default();
        for (key, value) in batch {
            new_batch.insert(*key, *value);
        }
        self.tree
            .apply_batch(new_batch)
            .map_err(|e| anyhow::anyhow!(e))
    }

    fn remove_batch(&self, batch: Vec<&[u8]>) -> Result<()> {
        let mut new_batch = Batch::default();
        for key in batch {
            new_batch.remove(key);
        }
        self.tree
            .apply_batch(new_batch)
            .map_err(|e| anyhow::anyhow!(e))
    }

    fn batched_increment<'a>(
        &self,
        batch: Vec<&'a [u8]>,
    ) -> Result<Vec<(&'a [u8], InlineArray)>> {
        let mut res = Vec::with_capacity(batch.len());
        for key in batch {
            let a = self.increment(key)?;
            res.push((key, a));
        }
        Ok(res)
    }

    fn iter(&self, limit: usize) -> Result<Vec<(InlineArray, InlineArray)>> {
        let mut res = Vec::with_capacity(min(limit, 1024));
        let mut iter = self.tree.iter().take(limit);
        while let Some(Ok((k, v))) = iter.next() {
            res.push((k, v));
        }
        Ok(res)
    }

    fn iter_count(&self) -> Result<usize> {
        let res = self.tree.iter().count();
        Ok(res)
    }

    fn range<'a>(
        &self,
        range: impl RangeBounds<&'a [u8]>,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(InlineArray, InlineArray)>> {
        let mut res = Vec::with_capacity(min(limit, 1024));
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

    fn range_count<'a>(
        &self,
        range: impl RangeBounds<&'a [u8]>,
        reverse: bool,
    ) -> Result<usize> {
        let iter = self.tree.range(range);
        let res = if reverse {
            iter.rev().count()
        } else {
            iter.count()
        };
        Ok(res)
    }

    fn iter_prefix(
        &self,
        prefix: &[u8],
        count: usize,
    ) -> Result<Vec<(InlineArray, InlineArray)>> {
        let mut res = Vec::with_capacity(min(count, 1024));
        let iter = self.tree.scan_prefix(prefix);
        for item in iter.take(count) {
            let (k, v) = item?;
            res.push((k, v));
        }
        Ok(res)
    }

    fn iter_prefix_from(
        &self,
        prefix: &[u8],
        from: &[u8],
        count: usize,
        reverse: bool,
    ) -> Result<Vec<(InlineArray, InlineArray)>> {
        let mut res = Vec::with_capacity(min(count, 1024));

        if reverse {
            let iter = self.tree.range(..=from).rev();
            for item in iter
                .take_while(|item| {
                    item.as_ref()
                        .map(|(k, _)| k.starts_with(prefix))
                        .unwrap_or(false)
                })
                .take(count)
            {
                let (k, v) = item?;
                res.push((k, v));
            }
        } else {
            let iter = self.tree.range(from..);
            for item in iter
                .take_while(|item| {
                    item.as_ref()
                        .map(|(k, _)| k.starts_with(prefix))
                        .unwrap_or(false)
                })
                .take(count)
            {
                let (k, v) = item?;
                res.push((k, v));
            }
        }
        Ok(res)
    }

    fn iter_prefix_count(&self, prefix: &[u8]) -> Result<usize> {
        let iter = self.tree.scan_prefix(prefix);
        let res = iter.count();
        Ok(res)
    }

    fn flush(&self) -> Result<()> {
        self.tree.flush()?;
        Ok(())
    }
}
