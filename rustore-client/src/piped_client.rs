use crate::reconnect::ReconnectingFramed;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use dashmap::{mapref::entry::Entry, DashMap};
use rustore_protocol::{
    owned::{ClientSidePipedResponse, OwnedResponse},
    Command, HandShake, Operation, PipedCommand,
};
use flume::Sender;
use futures_util::{SinkExt, StreamExt};
use nohash::NoHashHasher;
use quick_cache::sync::Cache;
use std::{hash::BuildHasherDefault, ops::Deref, sync::Arc, time::Duration};
use tokio::{
    sync::{
        oneshot::{self, Sender as OneshotSender},
        watch::{self, Receiver, Sender as WatchSender},
    },
    task::JoinHandle,
    time::timeout,
};
/// piped client to improve throughput on a single connection. Clone to share across threads. Put it in an RwLock and use writeguard to make batch operations. Other operations that are not batched only need a readguard. If you do not need batch operations, simply clone.
/// example:
/// ```rust
/// use tokio::sync::RwLock;
/// use data_store::client::piped_client::Client;
/// use std::sync::Arc;
/// use anyhow::Result;
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let client = Client::new("127.0.0.1:5555", "secret", false, false).await.unwrap();
///   let client_clone = client.clone();
///   client_clone.put(0, "key", "value").await.unwrap();
///   client.put_batch(0, vec![("key", "value"), ("key2", "value2")]).await });
///   Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Client {
    sender: Sender<Bytes>,
    _worker: Arc<JoinHandle<Result<(), anyhow::Error>>>,
    response_senders: Arc<DashMap<u32, OneshotSender<Bytes>, BuildHasherDefault<NoHashHasher<u8>>>>,
    pub cache: Option<Arc<Cache<(Bytes, Bytes), Bytes>>>,
    _subscriber: Option<Arc<JoinHandle<Result<(), anyhow::Error>>>>,
    watchers: Arc<DashMap<(Bytes, Bytes), (WatchSender<()>, Receiver<()>)>>,
}
/// Create a new client with the given address. The subscribe option is used to keep a local cache of the database. The watch_prefix option is used to watch for changes to a prefix. These 2 features have not been thoroughly tested, so best not use them in production.
impl Client {
    pub async fn new(
        addr: &str,
        secret: &str,
        subscribe: bool,
        watch_prefix: bool,
    ) -> anyhow::Result<Self> {
        let handshake = HandShake::new_piped(secret, subscribe, watch_prefix).to_bytes()?;
        let (sender, receiver) = flume::bounded::<Bytes>(1000000);
        let response_senders: Arc<
            DashMap<u32, OneshotSender<Bytes>, BuildHasherDefault<NoHashHasher<u8>>>,
        > = Arc::new(DashMap::with_hasher(BuildHasherDefault::default()));
        let response_senders_clone = response_senders.clone();
        let mut framed = ReconnectingFramed::connect(addr.to_string(), handshake.clone()).await?;
        let watchers: Arc<DashMap<(Bytes, Bytes), (WatchSender<()>, Receiver<()>)>> =
            Arc::new(DashMap::new());
        let watchers_clone = watchers.clone();
        let worker: JoinHandle<Result<()>> = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Ok(frame) = receiver.recv_async() => {
                        if let Err(e) = framed.send(frame.clone()).await {
                            println!("Error sending frame: {:?}, reconnecting", e);
                            framed.reconnect().await?;
                            framed.send(frame.clone()).await?;
                        }
                    }
                    frame = framed.next() => {
                        match frame {
                            Some(Ok(frame)) =>{
                                let ClientSidePipedResponse { id, response } = ClientSidePipedResponse::from_bytes(frame)?;
                                if id == 0 {
                                    if let Ok(OwnedResponse::WakePrefix(table, prefix)) =
                                        OwnedResponse::from_bytes(response.clone())
                                    {
                                        if let Some(sender) = watchers_clone.remove(&(table, prefix)) {
                                            sender.1 .0.send(())?;
                                        }
                                    }
                                }
                                if let Some(sender) = response_senders_clone.remove(&id) {
                                    sender.1.send(response).map(|_| ()).map_err(|e| anyhow::anyhow!("recv_worker error: {:?}", e))?;
                                }
                            }
                            Some(Err(e)) => {
                                println!("Error receiving frame: {:?}, reconnecting", e);
                                framed.reconnect().await?;
                            }
                            None => {
                                println!("Connection closed, reconnecting");
                                framed.reconnect().await?;
                            }
                        }
                    }
                }
            }
        });
        let mut cache: Option<Arc<Cache<(Bytes, Bytes), Bytes>>> = None;
        let mut subscriber: Option<Arc<JoinHandle<Result<()>>>> = None;
        let secret_clone = secret.to_string();
        if subscribe {
            let addr = addr.to_string();
            cache = Some(Arc::new(Cache::new(100_000)));
            let cache_clone = cache.clone().unwrap();
            let sub = tokio::spawn(async move {
                let handshake = HandShake::new_subscriber(&secret_clone).to_bytes()?;
                let mut stream = ReconnectingFramed::connect(addr, handshake.clone()).await?;
                loop {
                    match stream.next().await {
                        Some(Ok(bytes)) => {
                            let bytes = bytes.freeze();
                            let Command { table, operation } = Command::from_bytes(&bytes)?;
                            let table = bytes.slice_ref(table.as_ref());
                            match operation {
                                Operation::Put(key, val) => {
                                    // println!("put {:?}", key);
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
                            }}
                        Some(Err(e)) => {
                            println!("Error receiving frame: {:?}, reconnecting", e);
                            stream.reconnect().await?;
                        }
                        None => {
                            println!("Connection closed, reconnecting");
                            stream.reconnect().await?;
                        }
                    }
                }
            });
            subscriber = Some(Arc::new(sub));
        }

        Ok(Self {
            sender,
            _worker: Arc::new(worker),
            response_senders,
            cache,
            _subscriber: subscriber,
            watchers,
        })
    }
    #[inline(always)]
    fn new_command_id(&self) -> u32 {
        loop {
            let id = fastrand::u32(..);
            if !self.response_senders.contains_key(&id) && id != 0 {
                //0 is reserved for prefix watchers
                return id;
            }
        }
    }
    /// Drop/Remove a table
    pub async fn drop_table(&self, table: &[u8]) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::DropTable,
            },
        }
        .to_bytes()?;
        self.sender.send(command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            _ => Err(anyhow!("drop_table(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Get a value from the database, returns I/O error if disconnected, returns None if key does not exist
    pub async fn get(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Option<Bytes>> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.get(&(
                Bytes::copy_from_slice(table),
                Bytes::copy_from_slice(key.as_ref()),
            )) {
                return Ok(Some(value.clone()));
            }
        }
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::Get(&key),
            },
        }
        .to_bytes()?;
        self.sender.send(command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Value(response) => Ok(Some(response)),
            _ => Ok(None),
        }
    }
    /// Get a value from the database after a delay, returns I/O error if disconnected, returns None if key does not exist
    pub async fn get_delayed(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        delay_in_micros: u32,
    ) -> Result<Option<Bytes>> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.get(&(
                Bytes::copy_from_slice(table.as_ref()),
                Bytes::copy_from_slice(key.as_ref()),
            )) {
                return Ok(Some(value.clone()));
            }
        }
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::GetDelayed(delay_in_micros, &key),
            },
        }
        .to_bytes()?;
        self.sender.send(command)?;
        let response = timeout(
            Duration::from_micros(delay_in_micros as u64 + 1000000),
            receiver,
        )
        .await??;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Value(response) => Ok(Some(response)),
            _ => Ok(None),
        }
    }
    /// Get a batch of key/value tuples from the database, returns I/O error if disconnected
    pub async fn get_batch(&self, table: &[u8], batch: Vec<&[u8]>) -> Result<Vec<(Bytes, Bytes)>> {
        let mut result = Vec::with_capacity(batch.len());
        let mut not_in_cache: Vec<&[u8]> = Vec::with_capacity(batch.len());
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
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::BatchedGet(not_in_cache),
            },
        }
        .to_bytes()?;
        self.sender.send(command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Entries(entries) => {
                result.extend(entries);
                Ok(result)
            }
            _ => Err(anyhow!("get_batch(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Put a key/value tuple into the database, returns I/O error if disconnected
    pub async fn put(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::Put(key.as_ref(), value.as_ref()),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            OwnedResponse::PutError => Err(anyhow::anyhow!("Serverside put error")),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }
    /// Put a key/value tuple into the database and wake up any watchers of the prefix, returns I/O error if disconnected
    pub async fn put_and_wake(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::PutAndWake(key.as_ref(), value.as_ref(), prefix.as_ref()),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            OwnedResponse::PutError => Err(anyhow::anyhow!("Serverside put error")),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }
    /// Put a batch of key/value tuples into the database, returns I/O error if disconnected
    pub async fn put_batch(&self, table: &[u8], batch: Vec<(&[u8], &[u8])>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::BatchedPut(batch),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            OwnedResponse::BatchFailed => Err(anyhow!(
                "put_batch(): Some(Ok(response_bytes)) Response::BatchFailed"
            )),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }
    /// Put a batch of key/value tuples into the database and wake up any watchers of the prefixes, returns I/O error if disconnected
    pub async fn put_batch_and_wake(
        &self,
        table: &[u8],
        batch: Vec<(&[u8], &[u8])>,
        prefixes: Vec<&[u8]>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::BatchedPutAndWake(batch, prefixes),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            OwnedResponse::BatchFailed => Err(anyhow!(
                "put_batch_and_wake_prefix(): Some(Ok(response_bytes)) Response::BatchFailed"
            )),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }
    /// Put a key/value tuple into the database if the key does not exist, returns I/O error if disconnected
    pub async fn put_if_absent(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::PutIfAbsent(key.as_ref(), value.as_ref()),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            OwnedResponse::KeyExists => Err(anyhow!(
                "put_if_absent(): Some(Ok(response_bytes)) Response::KeyExists"
            )),
            _ => Err(anyhow!(
                "put_if_absent(): Some(Ok(response_bytes)) Response::_"
            )),
        }
    }
    /// Remove a key and its corresponding value from the database, returns I/O error if disconnected
    pub async fn remove(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::Remove(key.as_ref()),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            OwnedResponse::NoMatchingKey => Err(anyhow!(
                "remove(): Some(Ok(response_bytes)) Response::NoMatchingKey"
            )),
            _ => Err(anyhow!("remove(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Remove a batch of keys and their corresponding values from the database, returns I/O error if disconnected
    pub async fn remove_batch(&self, table: &[u8], batch: Vec<&[u8]>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::BatchedRemove(batch),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            OwnedResponse::BatchFailed => Err(anyhow!(
                "batched_remove(): Some(Ok(response_bytes)) Response::BatchFailed"
            )),
            _ => Err(anyhow!(
                "batched_remove(): Some(Ok(response_bytes)) Response::_"
            )),
        }
    }

    /// Update a key and its corresponding value in the database, returns I/O error if disconnected or if the key does not exist
    pub async fn update(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::Update(key.as_ref(), value.as_ref()),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Ok => Ok(()),
            OwnedResponse::NoMatchingKey => Err(anyhow!(
                "update(): Some(Ok(response_bytes)) Response::NoMatchingKey"
            )),
            _ => Err(anyhow!("update(): Some(Ok(response_bytes)) Response::_")),
        }
    }

    /// Increment a key by 1, value will be 1 if key does not exist, returns I/O error if disconnected, values have to be u64s in big endian order
    pub async fn increment(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Bytes> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::Increment(key.as_ref()),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Value(value) => Ok(value),
            OwnedResponse::NoMatchingKey => Err(anyhow!(
                "increment(): Some(Ok(response_bytes)) Response::NoMatchingKey"
            )),
            _ => Err(anyhow!("increment(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Decrement a key by 1, value will be -1 if key does not exist, returns I/O error if disconnected, values have to be u64s in big endian order
    pub async fn decrement(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Bytes> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::Decrement(key.as_ref()),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Value(value) => Ok(value),
            OwnedResponse::NoMatchingKey => Err(anyhow!(
                "decrement(): Some(Ok(response_bytes)) Response::NoMatchingKey"
            )),
            _ => Err(anyhow!("decrement(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Increment a batch of keys by 1, value will be 1 if key does not exist, returns I/O error if disconnected, values have to be u64s in big endian order
    pub async fn increment_batch(
        &self,
        table: &[u8],
        batch: Vec<&[u8]>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::BatchedIncrement(batch),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Entries(entries) => Ok(entries),
            OwnedResponse::BatchFailed => Err(anyhow!(
                "increment(): Some(Ok(response_bytes)) Response::BatchFailed"
            )),
            _ => Err(anyhow!("increment(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Iterate over the keys and values in a table, returns I/O error if disconnected
    pub async fn iter(&self, table: &[u8], limit: u32) -> Result<Vec<(Bytes, Bytes)>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::Iter(limit),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Entries(entries) => Ok(entries),
            _ => Err(anyhow!("get_batch(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Iterate over the keys and values with a range in a table, returns I/O error if disconnected
    pub async fn range(&self, table: &[u8], from: &[u8], to: &[u8], limit: u32) -> Result<Vec<(Bytes, Bytes)>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::Range(from, to, limit),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Entries(entries) => Ok(entries),
            _ => Err(anyhow!("get_batch(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Iterate over the keys and values with a prefix in a table, returns I/O error if disconnected
    pub async fn iter_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        count: u32,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::IterPrefix(prefix.as_ref(), count),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Entries(entries) => Ok(entries),
            _ => Err(anyhow!("get_batch(): Some(Ok(response_bytes)) Response::_")),
        }
    }

    pub async fn iter_prefix_from(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        from: impl Deref<Target = [u8]>,
        count: u32,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = if reverse {
            PipedCommand {
                id,
                command: Command {
                    table,
                    operation: Operation::IterPrefixFromRev(prefix.as_ref(), from.as_ref(), count),
                },
            }
            .to_bytes()?
        } else {
            PipedCommand {
                id,
                command: Command {
                    table,
                    operation: Operation::IterPrefixFrom(prefix.as_ref(), from.as_ref(), count),
                },
            }
            .to_bytes()?
        };
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Entries(entries) => Ok(entries),
            _ => Err(anyhow!("get_batch(): Some(Ok(response_bytes)) Response::_")),
        }
    }

    /// Iterate over the keys and values where keys are larger/smaller (depending on reverse) than the specified key in a table, returns I/O error if disconnected
    pub async fn iter_from(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        count: u32,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = if reverse {
            PipedCommand {
                id,
                command: Command {
                    table,
                    operation: Operation::IterFromRev(key.as_ref(), count),
                },
            }
            .to_bytes()?
        } else {
            PipedCommand {
                id,
                command: Command {
                    table,
                    operation: Operation::IterFrom(key.as_ref(), count),
                },
            }
            .to_bytes()?
        };
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        self.response_senders.remove(&id);
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Entries(entries) => Ok(entries),
            _ => Err(anyhow!("get_batch(): Some(Ok(response_bytes)) Response::_")),
        }
    }
    /// Count the number of entries in a table, returns I/O error if disconnected
    pub async fn count(&self, table: &[u8]) -> Result<usize> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::IterCount,
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Value(count) => Ok(usize::from_be_bytes(count[..].try_into()?)),
            _ => Err(anyhow!("count(): Some(Ok(response_bytes)) Response::_")),
        }
    }

    /// Count the number of entries within a key range in a table, returns I/O error if disconnected
    pub async fn count_range(&self, table: &[u8], from: &[u8], to: &[u8]) -> Result<usize> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::RangeCount(from, to),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        self.response_senders.remove(&id);
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Value(count) => Ok(usize::from_be_bytes(count[..].try_into()?)),
            _ => Err(anyhow!(
                "count_range(): Some(Ok(response_bytes)) Response::_"
            )),
        }
    }
    /// Count the number of entries with a prefix in a table, returns I/O error if disconnected
    pub async fn count_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<usize> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = PipedCommand {
            id,
            command: Command {
                table,
                operation: Operation::IterPrefixCount(&prefix),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        self.response_senders.remove(&id);
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Value(count) => Ok(usize::from_be_bytes(count[..].try_into()?)),
            _ => Err(anyhow!(
                "count_prefix(): Some(Ok(response_bytes)) Response::_"
            )),
        }
    }
    /// Count the entries where keys are larger/smaller (depending on reverse) than the specified key in a table, returns I/O error if disconnected
    pub async fn count_from(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        reverse: bool,
    ) -> Result<usize> {
        let (sender, receiver) = oneshot::channel();
        let id = self.new_command_id();
        self.response_senders.insert(id, sender);
        let piped_command = match reverse {
            true => PipedCommand {
                id,
                command: Command {
                    table,
                    operation: Operation::IterFromRevCount(&prefix),
                },
            }
            .to_bytes()?,
            false => PipedCommand {
                id,
                command: Command {
                    table,
                    operation: Operation::IterFromCount(&prefix),
                },
            }
            .to_bytes()?,
        };
        self.sender.send(piped_command)?;
        let response = receiver.await?;
        self.response_senders.remove(&id);
        let response = OwnedResponse::from_bytes(response)?;
        match response {
            OwnedResponse::Value(count) => Ok(usize::from_be_bytes(count[..].try_into()?)),
            _ => Err(anyhow!(
                "count_from(): Some(Ok(response_bytes)) Response::_"
            )),
        }
    }

    /// Watch a prefix for changes, returns a receiver that will receive a message when the prefix is changed
    pub fn watch_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<Receiver<()>> {
        let rx = match self.watchers.entry((
            Bytes::copy_from_slice(table.as_ref()),
            Bytes::copy_from_slice(prefix.as_ref()),
        )) {
            Entry::Occupied(entry) => entry.get().1.clone(),
            Entry::Vacant(entry) => {
                let (sender, receiver) = watch::channel(());
                entry.insert((sender, receiver.clone()));
                receiver
            }
        };
        let piped_command = PipedCommand {
            id: 0,
            command: Command {
                table,
                operation: Operation::WatchPrefix(prefix.as_ref()),
            },
        }
        .to_bytes()?;
        self.sender.send(piped_command)?;
        Ok(rx)
    }
}

use async_trait::async_trait;
use bb8::{ManageConnection, Pool};

pub struct Manager {
    addr: String,
    secret: String,
    subscribe: bool,
    watch_prefix: bool,
}

#[async_trait]
impl ManageConnection for Manager {
    type Connection = Client;
    type Error = anyhow::Error;
    async fn connect(&self) -> Result<Self::Connection> {
        Client::new(&self.addr, &self.secret, self.subscribe, self.watch_prefix).await
    }
    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<()> {
        Ok(())
    }
    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

pub struct ClientPool {
    pool: Pool<Manager>,
}

impl ClientPool {
    pub async fn new(
        addr: &str,
        secret: &str,
        max_size: u32,
        subscribe: bool,
        watch_prefix: bool,
    ) -> Result<Self> {
        let manager = Manager {
            addr: addr.to_string(),
            secret: secret.to_string(),
            subscribe,
            watch_prefix,
        };
        let pool = Pool::builder()
            .min_idle(Some(max_size))
            .max_size(max_size)
            .build(manager)
            .await?;
        Ok(Self { pool })
    }
    pub async fn get_conn(&self) -> Result<bb8::PooledConnection<'_, Manager>> {
        self.pool
            .get()
            .await
            .map_err(|_e| anyhow::anyhow!("ClientPool::get() error"))
    }
    pub async fn get(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Option<Bytes>> {
        let conn = self.get_conn().await?;
        conn.get(table, key).await
    }
    pub async fn get_batch(&self, table: &[u8], batch: Vec<&[u8]>) -> Result<Vec<(Bytes, Bytes)>> {
        let conn = self.get_conn().await?;
        conn.get_batch(table, batch).await
    }
    pub async fn put(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let conn = self.get_conn().await?;
        conn.put(table, key, value).await
    }
    pub async fn put_and_wake(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let conn = self.get_conn().await?;
        conn.put_and_wake(table, key, value, prefix).await
    }
    pub async fn put_batch(&self, table: &[u8], batch: Vec<(&[u8], &[u8])>) -> Result<()> {
        let conn = self.get_conn().await?;
        conn.put_batch(table, batch).await
    }
    pub async fn put_batch_and_wake(
        &self,
        table: &[u8],
        batch: Vec<(&[u8], &[u8])>,
        prefixes: Vec<&[u8]>,
    ) -> Result<()> {
        let conn = self.get_conn().await?;
        conn.put_batch_and_wake(table, batch, prefixes).await
    }
    pub async fn put_if_absent(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let conn = self.get_conn().await?;
        conn.put_if_absent(table, key, value).await
    }
    pub async fn remove(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<()> {
        let conn = self.get_conn().await?;
        conn.remove(table, key).await
    }
    pub async fn remove_batch(&self, table: &[u8], batch: Vec<&[u8]>) -> Result<()> {
        let conn = self.get_conn().await?;
        conn.remove_batch(table, batch).await
    }
    pub async fn update(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        value: impl Deref<Target = [u8]>,
    ) -> Result<()> {
        let conn = self.get_conn().await?;
        conn.update(table, key, value).await
    }
    pub async fn increment(&self, table: &[u8], key: impl Deref<Target = [u8]>) -> Result<Bytes> {
        let conn = self.get_conn().await?;
        conn.increment(table, key).await
    }
    pub async fn increment_batch(
        &self,
        table: &[u8],
        batch: Vec<&[u8]>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let conn = self.get_conn().await?;
        conn.increment_batch(table, batch).await
    }
    pub async fn iter(&self, table: &[u8], count: u32) -> Result<Vec<(Bytes, Bytes)>> {
        let conn = self.get_conn().await?;
        conn.iter(table, count).await
    }
    pub async fn iter_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        count: u32,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let conn = self.get_conn().await?;
        conn.iter_prefix(table, prefix, count).await
    }
    pub async fn iter_prefix_from(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        from: impl Deref<Target = [u8]>,
        count: u32,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let conn = self.get_conn().await?;
        conn.iter_prefix_from(table, prefix, from, count, reverse)
            .await
    }
    pub async fn iter_from(
        &self,
        table: &[u8],
        key: impl Deref<Target = [u8]>,
        count: u32,
        reverse: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let conn = self.get_conn().await?;
        conn.iter_from(table, key, count, reverse).await
    }
    pub async fn count(&self, table: &[u8]) -> Result<usize> {
        let conn = self.get_conn().await?;
        conn.count(table).await
    }
    pub async fn count_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<usize> {
        let conn = self.get_conn().await?;
        conn.count_prefix(table, prefix).await
    }
    pub async fn count_from(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
        reverse: bool,
    ) -> Result<usize> {
        let conn = self.get_conn().await?;
        conn.count_from(table, prefix, reverse).await
    }
    pub async fn watch_prefix(
        &self,
        table: &[u8],
        prefix: impl Deref<Target = [u8]>,
    ) -> Result<Receiver<()>> {
        let conn = self.get_conn().await?;
        conn.watch_prefix(table, prefix)
    }
}
