use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bb8::{ManageConnection, Pool};
use bytes::Bytes;
use rustore_protocol::{owned::OwnedResponse, Command, HandShake, Operation};
use futures_util::{SinkExt, Stream, StreamExt};
use quick_cache::sync::Cache;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{io::BufStream, net::TcpStream, task::JoinHandle};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use super::DataStoreError;
/// A client for the key-value store.
pub struct Connection {
    address: SocketAddr,
    secret: String,
    subscribe: bool,
}

#[async_trait]
impl ManageConnection for Connection {
    type Connection = Framed<BufStream<TcpStream>, LengthDelimitedCodec>;
    type Error = std::io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(1024 * 1024 * 1024);
        let stream = TcpStream::connect(self.address).await?;
        stream.set_nodelay(true)?;
        let stream = BufStream::new(stream);
        let mut framed = Framed::new(stream, codec);
        let handshake = HandShake::new_standard(&self.secret, self.subscribe)
            .to_bytes()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        framed.send(handshake).await?;
        Ok(framed)
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        let context = &mut Context::from_waker(futures_util::task::noop_waker_ref());
        !conn.poll_ready_unpin(context).is_ready()
        // true
    }
}
#[derive(Clone)]
pub struct Client {
    connection_pool: Pool<Connection>,
    pub cache: Option<Arc<Cache<(Bytes, Bytes), Bytes>>>,
    _subscriber: Option<Arc<JoinHandle<Result<(), anyhow::Error>>>>,
}

impl Client {
    /// Create a new client connecting to the given address.
    pub async fn new(address: &str, size: u32, secret: &str, subscribe: bool) -> Result<Self> {
        let addr = address
            .to_socket_addrs()?
            .next()
            .ok_or(anyhow!("new(): invalid address"))?;

        let client = Connection {
            address: addr,
            secret: secret.to_string(),
            subscribe,
        };
        let connection_pool = Pool::builder()
            .max_size(size)
            .min_idle(Some(size))
            .build(client)
            .await?;

        let mut cache: Option<Arc<Cache<(Bytes, Bytes), Bytes>>> = None;
        let mut subscriber: Option<Arc<JoinHandle<Result<()>>>> = None;
        let secret_clone = secret.to_string();
        if subscribe {
            let push_stream = TcpStream::connect(addr).await?;
            cache = Some(Arc::new(Cache::new(100_000)));
            let cache_clone = cache.clone().unwrap();
            let sub = tokio::spawn(async move {
                let mut stream = Framed::new(push_stream, LengthDelimitedCodec::new());
                let handshake = HandShake::new_subscriber(&secret_clone).to_bytes()?;
                stream.send(handshake).await?;
                while let Some(bytes) = stream.next().await {
                    let bytes = bytes?.freeze();
                    let Command { table, operation } = Command::from_bytes(&bytes)?;
                    let table = bytes.slice_ref(table.as_ref());
                    match operation {
                        Operation::Put(key, val) => {
                            cache_clone.insert(
                                (table, Bytes::copy_from_slice(key)),
                                Bytes::copy_from_slice(val),
                            );
                        }
                        Operation::BatchedPut(batch) => {
                            for (key, val) in batch {
                                cache_clone.insert(
                                    (table.clone(), Bytes::copy_from_slice(key)),
                                    Bytes::copy_from_slice(val),
                                );
                            }
                        }
                        Operation::PutIfAbsent(key, val) => {
                            cache_clone.insert(
                                (table, Bytes::copy_from_slice(key)),
                                Bytes::copy_from_slice(val),
                            );
                        }
                        Operation::Update(key, val) => {
                            cache_clone.insert(
                                (table, Bytes::copy_from_slice(key)),
                                Bytes::copy_from_slice(val),
                            );
                        }
                        Operation::Remove(key) => {
                            cache_clone.remove(&(table, Bytes::copy_from_slice(key)));
                        }
                        Operation::BatchedRemove(batch) => {
                            for key in batch {
                                cache_clone.remove(&(table.clone(), Bytes::copy_from_slice(key)));
                            }
                        }
                        _ => {}
                    }
                }
                Ok(())
            });
            subscriber = Some(Arc::new(sub));
        }
        Ok(Self {
            connection_pool,
            cache,
            _subscriber: subscriber,
        })
    }

    pub async fn drop_table(&self, table: &[u8]) -> Result<()> {
        let bytes = Command {
            table,
            operation: Operation::DropTable,
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!("drop_table(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("drop_table(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("drop_table(): None")),
        }
    }
    /// Get the given key-value pair from a table
    pub async fn get(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.get(&(
                Bytes::copy_from_slice(table.as_ref()),
                Bytes::copy_from_slice(key.as_ref()),
            )) {
                return Ok(Some(value.into()));
            }
        }
        let bytes = Command {
            table,
            operation: Operation::Get(key.deref()),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Value(value) => Ok(Some(value.into())),
                    OwnedResponse::NoMatchingKey => Ok(None),
                    _ => Err(anyhow!("get(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("get(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("get(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("get(): None")),
        }
    }

    /// Get the given key-value pairs from a table, return the entries if successful
    pub async fn get_batch(&self, table: &[u8], batch: Vec<&[u8]>) -> Result<Vec<(Bytes, Bytes)>> {
        let mut result = Vec::with_capacity(batch.len());
        let mut not_in_cache = Vec::with_capacity(batch.len());
        if let Some(cache) = &self.cache {
            for key in &batch {
                if let Some(value) = cache.get(&(
                    Bytes::copy_from_slice(table.as_ref()),
                    Bytes::copy_from_slice(key),
                )) {
                    result.push((Bytes::copy_from_slice(key), value.clone()));
                } else {
                    not_in_cache.push(*key);
                }
            }
        } else {
            not_in_cache = batch;
        }

        if not_in_cache.is_empty() {
            return Ok(result);
        }
        let bytes = Command {
            table,
            operation: Operation::BatchedGet(not_in_cache),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Entries(entries) => {
                        result.extend(entries);
                        Ok(result)
                    }
                    OwnedResponse::BatchFailed => Err(anyhow!(
                        "get_batch(): Some(Ok(response_bytes)) OwnedResponse::BatchFailed"
                    )),
                    _ => Err(anyhow!("get(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("get(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("get(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("get(): None")),
        }
    }

    /// Put the given key-value pair from a table
    pub async fn put(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let bytes = Command {
            table,
            operation: Operation::Put(key.deref(), value.deref()),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Ok => Ok(()),
                    _ => Err(anyhow!("put(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("put(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("put(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("put(): None")),
        }
    }
    /// Put the given key-value pairs into a table, returning error if batch failed
    pub async fn put_batch(&self, table: &[u8], batch: Vec<(&[u8], &[u8])>) -> Result<()> {
        let bytes = Command {
            table,
            operation: Operation::BatchedPut(batch),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Ok => Ok(()),
                    _ => Err(anyhow!("put(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("put(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("put(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("put(): None")),
        }
    }

    /// Put the given key-value pair from a table if the key does not exist
    pub async fn put_if_absent(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let bytes = Command {
            table,
            operation: Operation::PutIfAbsent(key.deref(), value.deref()),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Ok => Ok(()),
                    OwnedResponse::KeyExists => Err(anyhow!(
                        "put_if_absent(): Some(Ok(response_bytes)) OwnedResponse::KeyExists"
                    )),
                    _ => Err(anyhow!(
                        "put_if_absent(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!(
                    "put_if_absent(): Some(Ok(response_bytes)) Err(e): {}",
                    e
                )),
            },
            Some(Err(e)) => Err(anyhow!("put_if_absent(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("put_if_absent(): None")),
        }
    }
    /// Remove the given key-value pair from a table
    pub async fn remove(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<()> {
        let bytes = Command {
            table,
            operation: Operation::Remove(key.deref()),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Ok => Ok(()),
                    OwnedResponse::NoMatchingKey => Err(anyhow!(
                        "remove(): Some(Ok(response_bytes)) OwnedResponse::NoMatchingKey"
                    )),
                    _ => Err(anyhow!(
                        "remove(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!("remove(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("remove(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("remove(): None")),
        }
    }

    pub async fn remove_batch(&self, table: &[u8], batch: Vec<&[u8]>) -> Result<()> {
        let bytes = Command {
            table,
            operation: Operation::BatchedRemove(batch),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!(
                    "batched_remove(): Some(Ok(response_bytes)) Err(e): {}",
                    e
                )),
            },
            Some(Err(e)) => Err(anyhow!("batched_remove(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("batched_remove(): None")),
        }
    }

    /// Update the given key-value pair from a table
    pub async fn update(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let bytes = Command {
            table,
            operation: Operation::Update(key.deref(), value.deref()),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Ok => Ok(()),
                    OwnedResponse::NoMatchingKey => Err(anyhow!(
                        "update(): Some(Ok(response_bytes)) OwnedResponse::NoMatchingKey"
                    )),
                    _ => Err(anyhow!(
                        "update(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!("update(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("update(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("update(): None")),
        }
    }
    /// Increment the value associated to a given key from a table
    pub async fn increment(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Bytes> {
        let bytes = Command {
            table,
            operation: Operation::Increment(key.deref()),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Value(value) => Ok(value),
                    OwnedResponse::NoMatchingKey => Err(anyhow!(
                        "increment(): Some(Ok(response_bytes)) OwnedResponse::NoMatchingKey"
                    )),
                    _ => Err(anyhow!(
                        "increment(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!(
                    "increment(): Some(Ok(response_bytes)) Err(e): {}",
                    e
                )),
            },
            Some(Err(e)) => Err(anyhow!("increment(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("increment(): None")),
        }
    }
    pub async fn decrement(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Bytes> {
        let bytes = Command {
            table,
            operation: Operation::Decrement(key.deref()),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Value(value) => Ok(value),
                    OwnedResponse::NoMatchingKey => Err(anyhow!(
                        "decrement(): Some(Ok(response_bytes)) OwnedResponse::NoMatchingKey"
                    )),
                    _ => Err(anyhow!(
                        "decrement(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!(
                    "decrement(): Some(Ok(response_bytes)) Err(e): {}",
                    e
                )),
            },
            Some(Err(e)) => Err(anyhow!("decrement(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("decrement(): None")),
        }
    }
    /// Increment the values associated to given keys from a table, return the keys whose values failed to be incremented
    pub async fn increment_batch(
        &self,
        table: &[u8],
        batch: Vec<&[u8]>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let bytes = Command {
            table,
            operation: Operation::BatchedIncrement(batch),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Entries(entries) => Ok(entries),
                    OwnedResponse::BatchFailed => Err(anyhow!(
                        "increment(): Some(Ok(response_bytes)) OwnedResponse::BatchFailed"
                    )),
                    _ => Err(anyhow!(
                        "increment(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!(
                    "increment(): Some(Ok(response_bytes)) Err(e): {}",
                    e
                )),
            },
            Some(Err(e)) => Err(anyhow!("increment(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("increment(): None")),
        }
    }
    /// Iterate over all key-value pairs of a table in sorted order.
    pub async fn iter(&self, table: &[u8], count: u32) -> Result<Vec<(Bytes, Bytes)>> {
        let bytes = Command {
            table,
            operation: Operation::Iter(count),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Entries(entries) => Ok(entries),
                    OwnedResponse::BatchFailed => Err(anyhow!(
                        "get_batch(): Some(Ok(response_bytes)) OwnedResponse::BatchFailed"
                    )),
                    _ => Err(anyhow!("get(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("get(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("get(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("get(): None")),
        }
    }

    /// Iterate over all key-value pairs, whose keys are within a given range, in sorted order.
    pub async fn range(&self, table: &[u8], from: &[u8], to: &[u8], count: u32) -> Result<Vec<(Bytes, Bytes)>> {
        let bytes = Command {
            table,
            operation: Operation::Range(from, to, count),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Entries(entries) => Ok(entries),
                    OwnedResponse::BatchFailed => Err(anyhow!(
                        "get_batch(): Some(Ok(response_bytes)) OwnedResponse::BatchFailed"
                    )),
                    _ => Err(anyhow!("get(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("get(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("get(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("get(): None")),
        }
    }

    /// Iterate over all key-value pairs, whose keys match a prefix, in sorted order.
    pub async fn iter_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        count: u32,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let bytes = Command {
            table,
            operation: Operation::IterPrefix(prefix.deref(), count),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Entries(entries) => Ok(entries),
                    OwnedResponse::BatchFailed => Err(anyhow!(
                        "get_batch(): Some(Ok(response_bytes)) OwnedResponse::BatchFailed"
                    )),
                    _ => Err(anyhow!("get(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("get(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("get(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("get(): None")),
        }
    }

    /// Iterate over all key-value pairs whose keys are greater than or equal to a given key with prefix, in sorted order.
    pub async fn iter_prefix_from(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        key: impl Deref<Target = [u8]>,
        count: u32,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let bytes = match reverse {
            true => Command {
                table,
                operation: Operation::IterPrefixFromRev(prefix.deref(), key.deref(), count),
            }
            .to_bytes()?,
            false => Command {
                table,
                operation: Operation::IterPrefixFrom(prefix.deref(), key.deref(), count),
            }
            .to_bytes()?,
        };
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Entries(entries) => Ok(entries),
                    OwnedResponse::BatchFailed => Err(anyhow!(
                        "get_batch(): Some(Ok(response_bytes)) OwnedResponse::BatchFailed"
                    )),
                    _ => Err(anyhow!("get(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("get(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("get(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("get(): None")),
        }
    }

    /// Iterate over all key-value pairs whose keys are greater than or equal to a given key, in sorted order.
    pub async fn iter_from(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        count: u32,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let bytes = match reverse {
            true => Command {
                table,
                operation: Operation::IterFromRev(key.deref(), count),
            }
            .to_bytes()?,
            false => Command {
                table,
                operation: Operation::IterFrom(key.deref(), count),
            }
            .to_bytes()?,
        };
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Entries(entries) => Ok(entries),
                    OwnedResponse::BatchFailed => Err(anyhow!(
                        "get_batch(): Some(Ok(response_bytes)) OwnedResponse::BatchFailed"
                    )),
                    _ => Err(anyhow!("get(): Some(Ok(response_bytes)) OwnedResponse::_")),
                },
                Err(e) => Err(anyhow!("get(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("get(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("get(): None")),
        }
    }

    /// get count of iterator
    pub async fn count(&self, table: &[u8]) -> Result<usize> {
        let bytes = Command {
            table,
            operation: Operation::IterCount,
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Value(value) => Ok(usize::from_be_bytes(value[..].try_into()?)),
                    _ => Err(anyhow!(
                        "count(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!("count(): Some(Ok(response_bytes)) Err(e): {}", e)),
            },
            Some(Err(e)) => Err(anyhow!("count(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("count(): None")),
        }
    }

    pub async fn count_range(&self, table: &[u8], from: &[u8], to: &[u8]) -> Result<usize> {
        let bytes = Command {
            table,
            operation: Operation::RangeCount(from, to),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Value(value) => Ok(usize::from_be_bytes(value[..].try_into()?)),
                    _ => Err(anyhow!(
                        "count_range(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!(
                    "count_range(): Some(Ok(response_bytes)) Err(e): {}",
                    e
                )),
            },
            Some(Err(e)) => Err(anyhow!("count_range(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("count_range(): None")),
        }
    }
    /// get count of prefix iterator
    pub async fn count_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<usize> {
        let bytes = Command {
            table,
            operation: Operation::IterPrefixCount(prefix.deref()),
        }
        .to_bytes()?;
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Value(value) => Ok(usize::from_be_bytes(value[..].try_into()?)),
                    _ => Err(anyhow!(
                        "count_prefix(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!(
                    "count_prefix(): Some(Ok(response_bytes)) Err(e): {}",
                    e
                )),
            },
            Some(Err(e)) => Err(anyhow!("count_prefix(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("count_prefix(): None")),
        }
    }
    /// get count of from_iterator
    pub async fn count_from(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        reverse: bool,
    ) -> Result<usize> {
        let bytes = match reverse {
            true => Command {
                table,
                operation: Operation::IterFromRevCount(key.deref()),
            }
            .to_bytes()?,
            false => Command {
                table,
                operation: Operation::IterFromCount(key.deref()),
            }
            .to_bytes()?,
        };
        let mut connection = self.connection_pool.get().await?;
        connection.send(bytes).await?;
        match connection.next().await {
            Some(Ok(response_bytes)) => match OwnedResponse::from_bytes(response_bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::Value(value) => Ok(usize::from_be_bytes(value[..].try_into()?)),
                    _ => Err(anyhow!(
                        "count_from(): Some(Ok(response_bytes)) OwnedResponse::_"
                    )),
                },
                Err(e) => Err(anyhow!(
                    "count_from(): Some(Ok(response_bytes)) Err(e): {}",
                    e
                )),
            },
            Some(Err(e)) => Err(anyhow!("count_from(): Some(Err(e)): {}", e)),
            None => Err(anyhow!("count_from(): None")),
        }
    }
}

pub struct IterStream {
    pub recv: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Stream for IterStream {
    type Item = Result<(Bytes, Bytes)>;
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.recv).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => match OwnedResponse::from_bytes(bytes.freeze()) {
                Ok(response) => match response {
                    OwnedResponse::IterEnd => Poll::Ready(None),
                    OwnedResponse::Entry(k, v) => Poll::Ready(Some(Ok((k, v)))),
                    _ => Poll::Ready(Some(Err(DataStoreError::IterNextFailed.into()))),
                },
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
