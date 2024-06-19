use std::ops::Deref;

use crate::piped_client::Client as PipedClient;
use crate::pooled_client::Client as PooledClient;
use anyhow::Result;
use bytes::Bytes;
use tokio::sync::watch::Receiver;
#[derive(Clone)]
pub struct HybridClient {
    pub piped_client: PipedClient,
    pub standard_client: PooledClient,
}

impl HybridClient {
    pub async fn new(
        addr: &str,
        secret: &str,
        max_size: u32,
        subscribe: bool,
        watch_prefix: bool,
    ) -> Result<Self> {
        let piped_client = PipedClient::new(addr, secret, subscribe, watch_prefix).await?;
        let standard_client = PooledClient::new(addr, max_size, secret, false).await?;
        Ok(Self {
            piped_client,
            standard_client,
        })
    }
    pub async fn new_with_pool(
        addr: &str,
        secret: &str,
        pool: PooledClient,
        subscribe: bool,
        watch_prefix: bool,
    ) -> Result<Self> {
        let piped_client = PipedClient::new(addr, secret, subscribe, watch_prefix).await?;
        Ok(Self {
            piped_client,
            standard_client: pool,
        })
    }
    pub async fn drop_table(&self, table: &[u8]) -> Result<()> {
        self.piped_client.drop_table(table).await
    }
    pub async fn get(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Option<Bytes>> {
        self.piped_client.get(table, key).await
    }
    pub async fn get_delayed(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        delay_in_micros: u32,
    ) -> Result<Option<Bytes>> {
        self.piped_client
            .get_delayed(table, key, delay_in_micros)
            .await
    }
    pub async fn get_batch(&self, table: &[u8], batch: Vec<&[u8]>) -> Result<Vec<(Bytes, Bytes)>> {
        if batch.len() <= 20 {
            return self.piped_client.get_batch(table, batch).await;
        }
        self.standard_client.get_batch(table, batch).await
    }
    pub async fn put(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        self.piped_client.put(table, key, value).await
    }
    pub async fn put_and_wake(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        self.piped_client
            .put_and_wake(table, key, value, prefix)
            .await
    }
    pub async fn put_batch(&self, table: &[u8], batch: Vec<(&[u8], &[u8])>) -> Result<()> {
        if batch.len() <= 20 {
            return self.piped_client.put_batch(table, batch).await;
        }
        self.standard_client.put_batch(table, batch).await
    }
    pub async fn put_batch_and_wake(
        &self,
        table: &[u8],
        batch: Vec<(&[u8], &[u8])>,
        prefixes: Vec<&[u8]>,
    ) -> Result<()> {
        self.piped_client
            .put_batch_and_wake(table, batch, prefixes)
            .await
    }
    pub async fn put_if_absent(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        self.piped_client.put_if_absent(table, key, value).await
    }
    pub async fn remove(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<()> {
        self.piped_client.remove(table, key).await
    }
    pub async fn remove_batch(&self, table: &[u8], batch: Vec<&[u8]>) -> Result<()> {
        if batch.len() <= 50 {
            return self.piped_client.remove_batch(table, batch).await;
        }
        self.standard_client.remove_batch(table, batch).await
    }
    pub async fn update(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        self.piped_client.update(table, key, value).await
    }
    pub async fn increment(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Bytes> {
        self.piped_client.increment(table, key).await
    }

    pub async fn decrement(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Bytes> {
        self.piped_client.decrement(table, key).await
    }

    pub async fn increment_batch(
        &self,
        table: &[u8],
        batch: Vec<&[u8]>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if batch.len() <= 50 {
            return self.piped_client.increment_batch(table, batch).await;
        }
        self.standard_client.increment_batch(table, batch).await
    }
    pub async fn iter(&self, table: &[u8], count: u32) -> Result<Vec<(Bytes, Bytes)>> {
        self.standard_client.iter(table, count).await
    }
    pub async fn iter_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        count: u32,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.standard_client.iter_prefix(table, prefix, count).await
    }
    pub async fn iter_prefix_from(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        from: impl Deref<Target = [u8]>,
        count: u32,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.standard_client
            .iter_prefix_from(table, prefix, from, count, reverse)
            .await
    }
    pub async fn iter_from(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        count: u32,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.standard_client
            .iter_from(table, key, count, reverse)
            .await
    }

    pub async fn count(&self, table: &[u8]) -> Result<usize> {
        self.piped_client.count(table).await
    }

    pub async fn count_range(
        &self,
        table: &[u8],
        from: &[u8], 
        to: &[u8]
    ) -> Result<usize> {
        self.piped_client.count_range(table, from, to).await
    }
    pub async fn count_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<usize> {
        self.piped_client.count_prefix(table, prefix).await
    }

    pub async fn count_from(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        reverse: bool,
    ) -> Result<usize> {
        self.piped_client.count_from(table, prefix, reverse).await
    }

    pub fn watch_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<Receiver<()>> {
        self.piped_client.watch_prefix(table, prefix)
    }
}
