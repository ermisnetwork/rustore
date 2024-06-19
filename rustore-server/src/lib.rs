use std::sync::Arc;

use ahash::RandomState;
use anyhow::Result;
use dashmap::{mapref::entry::Entry, DashMap};
use rustore_protocol::{Command, Operation, PipedCommand, PipedResponse, Response};
use rustore_storage::{Database, Table};
use flume::Sender;
use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_util::{
    bytes::Bytes,
    codec::{Framed, LengthDelimitedCodec},
};
pub mod server;
#[inline(always)]
pub async fn handle_piped_command<T> (
    tables: &mut T,
    bytes: Bytes,
    sender: &mut Sender<Bytes>,
    push_sender: &mut async_broadcast::Sender<Bytes>,
    subscribe: bool,
    watch_prefix: bool,
    wakers: &Arc<DashMap<(Bytes, Bytes), Vec<Sender<Bytes>>, RandomState>>,
    watched_keys: &Arc<DashMap<(Bytes, Bytes), Vec<(u32, Sender<Bytes>)>, RandomState>>,
) -> Result<()> where T: Database {
    let piped_command = PipedCommand::from_bytes(&bytes)?;
    let PipedCommand { command, id } = piped_command;
    let operation = command.operation;
    let table_id = bytes.slice_ref(command.table);
    let table = tables.open_or_create_table(table_id.clone())?;
    match operation {
        Operation::Put(key, value) => {
            if table.insert(key, value).is_err() {
                let response = PipedResponse {
                    id,
                    response: Response::PutError,
                }
                .to_bytes();
                return sender.send(response).map_err(|e| e.into());
            }
            if let Some(entry) = watched_keys.remove(&(table_id.clone(), bytes.slice_ref(key))) {
                for (id, sender) in entry.1 {
                    let response_bytes = PipedResponse {
                        id,
                        response: Response::Value(value),
                    }
                    .to_bytes();
                    sender.send(response_bytes)?;
                }
            }
            let response = PipedResponse {
                id,
                response: Response::Ok,
            }
            .to_bytes();
            if watch_prefix {
                let key = bytes.slice_ref(&key);
                for i in 0..=key.len() {
                    let Some(((table, prefix), senders)) =
                        wakers.remove(&(table_id.clone(), key.slice(0..i)))
                    else {
                        continue;
                    };
                    let wake_response = PipedResponse {
                        id: 0,
                        response: Response::WakePrefix(table.as_ref(), &prefix),
                    }
                    .to_bytes();
                    for sender in senders {
                        sender.send(wake_response.clone())?;
                    }
                }
            }
            if subscribe {
                push_sender.broadcast_direct(bytes.slice(4..)).await?;
            }
            sender.send(response).map_err(|e| e.into())
        }
        Operation::PutAndWake(key, value, prefix) => {
            if table.insert(key, value).is_err() {
                let response = PipedResponse {
                    id,
                    response: Response::PutError,
                }
                .to_bytes();
                return sender.send(response).map_err(|e| e.into());
            }
            if let Some(entry) = watched_keys.remove(&(table_id.clone(), bytes.slice_ref(key))) {
                for (id, sender) in entry.1 {
                    let response_bytes = PipedResponse {
                        id,
                        response: Response::Value(value),
                    }
                    .to_bytes();
                    sender.send(response_bytes)?;
                }
            }
            let response = PipedResponse {
                id,
                response: Response::Ok,
            }
            .to_bytes();
            if watch_prefix {
                let prefix = bytes.slice_ref(&prefix);
                if let Some(((table, prefix), senders)) = wakers.remove(&(table_id, prefix)) {
                    let wake_response = PipedResponse {
                        id: 0,
                        response: Response::WakePrefix(table.as_ref(), &prefix),
                    }
                    .to_bytes();
                    for sender in senders {
                        sender.send(wake_response.clone())?;
                    }
                };
            }
            if subscribe {
                push_sender.broadcast_direct(bytes.slice(4..)).await?;
            }
            sender.send(response).map_err(|e| e.into())
        }
        Operation::BatchedPut(batch) => {
            if table.insert_batch(&batch).is_err() {
                let response = PipedResponse {
                    id,
                    response: Response::BatchFailed,
                }
                .to_bytes();
                return sender.send(response).map_err(|e| e.into());
            }
            for &(key, value) in batch.iter() {
                if let Some(entry) = watched_keys.remove(&(table_id.clone(), bytes.slice_ref(key)))
                {
                    for (id, sender) in entry.1 {
                        let response_bytes = PipedResponse {
                            id,
                            response: Response::Value(value),
                        }
                        .to_bytes();
                        sender.send(response_bytes)?;
                    }
                }
            }
            let response = PipedResponse {
                id,
                response: Response::Ok,
            }
            .to_bytes();
            if watch_prefix {
                for (key, _) in batch {
                    let key = bytes.slice_ref(&key);
                    for i in 0..=key.len() {
                        let Some(((table, prefix), senders)) =
                            wakers.remove(&(table_id.clone(), key.slice(0..i)))
                        else {
                            continue;
                        };
                        let wake_response = PipedResponse {
                            id: 0,
                            response: Response::WakePrefix(table.as_ref(), &prefix),
                        }
                        .to_bytes();
                        for sender in senders {
                            sender.send(wake_response.clone())?;
                        }
                    }
                }
            }
            if subscribe {
                push_sender.broadcast_direct(bytes.slice(4..)).await?;
            }
            sender.send(response).map_err(|e| e.into())
        }
        Operation::BatchedPutAndWake(batch, prefixes) => {
            if table.insert_batch(&batch).is_err() {
                let response = PipedResponse {
                    id,
                    response: Response::BatchFailed,
                }
                .to_bytes();
                return sender.send(response).map_err(|e| e.into());
            }
            for &(key, value) in batch.iter() {
                if let Some(entry) = watched_keys.remove(&(table_id.clone(), bytes.slice_ref(key)))
                {
                    for (id, sender) in entry.1 {
                        let response_bytes = PipedResponse {
                            id,
                            response: Response::Value(value),
                        }
                        .to_bytes();
                        sender.send(response_bytes)?;
                    }
                }
            }
            let response = PipedResponse {
                id,
                response: Response::Ok,
            }
            .to_bytes();
            if watch_prefix {
                for key in prefixes {
                    let key = bytes.slice_ref(&key);
                    if let Some(((table, prefix), senders)) =
                        wakers.remove(&(table_id.clone(), key))
                    {
                        let wake_response = PipedResponse {
                            id: 0,
                            response: Response::WakePrefix(table.as_ref(), &prefix),
                        }
                        .to_bytes();
                        for sender in senders {
                            sender.send(wake_response.clone())?;
                        }
                    };
                }
            }
            if subscribe {
                push_sender.broadcast_direct(bytes.slice(4..)).await?;
            }
            sender.send(response).map_err(|e| e.into())
        }
        Operation::Get(key) => {
            if let Ok(Some(entry)) = table.get(key) {
                let response = PipedResponse {
                    id,
                    response: Response::Value(entry.as_ref()),
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            } else {
                let response = PipedResponse {
                    id,
                    response: Response::NoMatchingKey,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
        }
        Operation::GetDelayed(_duration, key) => {
            if let Ok(Some(entry)) = table.get(key) {
                let response = PipedResponse {
                    id,
                    response: Response::Value(entry.as_ref()),
                }
                .to_bytes();
                return sender.send(response).map_err(|e| e.into());
            }
            let mut entry = watched_keys
                .entry((table_id, bytes.slice_ref(key)))
                .or_insert(vec![]);
            entry.push((id, sender.clone()));
            anyhow::Ok(())
        }
        Operation::BatchedGet(keys) => {
            if let Ok(entries) = table.get_batch(&keys) {
                let response = PipedResponse {
                    id,
                    response: Response::Entries(
                        entries.iter().map(|(k, v)| (*k, v.as_ref())).collect(),
                    ),
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            } else {
                let response = PipedResponse {
                    id,
                    response: Response::BatchFailed,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
        }
        Operation::PutIfAbsent(key, value) => match table.put_if_absent(key, value) {
            Ok(true) => {
                let response = PipedResponse {
                    id,
                    response: Response::Ok,
                }
                .to_bytes();
                if subscribe {
                    push_sender.broadcast_direct(bytes.slice(4..)).await?;
                }
                sender.send(response).map_err(|e| e.into())
            }
            Ok(false) => {
                let response = PipedResponse {
                    id,
                    response: Response::KeyExists,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
            Err(_) => {
                let response = PipedResponse {
                    id,
                    response: Response::PutError,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
        },
        Operation::Remove(key) => {
            if table.remove(key).is_ok() {
                let response = PipedResponse {
                    id,
                    response: Response::Ok,
                }
                .to_bytes();
                if subscribe {
                    push_sender.broadcast_direct(bytes.slice(4..)).await?;
                }
                sender.send(response).map_err(|e| e.into())
            } else {
                let response = PipedResponse {
                    id,
                    response: Response::NoMatchingKey,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
        }
        Operation::BatchedRemove(keys) => {
            if table.remove_batch(keys).is_ok() {
                let response = PipedResponse {
                    id,
                    response: Response::Ok,
                }
                .to_bytes();
                if subscribe {
                    push_sender.broadcast_direct(bytes.slice(4..)).await?;
                }
                sender.send(response).map_err(|e| e.into())
            } else {
                let response = PipedResponse {
                    id,
                    response: Response::BatchFailed,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
        }
        Operation::Update(key, value) => match table.update(key, value) {
            Ok(true) => {
                let response = PipedResponse {
                    id,
                    response: Response::Ok,
                }
                .to_bytes();
                if subscribe {
                    push_sender.broadcast_direct(bytes.slice(4..)).await?;
                }
                sender.send(response).map_err(|e| e.into())
            }
            Ok(false) => {
                let response = PipedResponse {
                    id,
                    response: Response::NoMatchingKey,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
            Err(_) => {
                let response = PipedResponse {
                    id,
                    response: Response::PutError,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
        },
        Operation::Iter(count) => {
            let iter = table.iter(count as usize)?;
            let response = PipedResponse {
                id,
                response: Response::Entries(
                    iter.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect(),
                ),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::Range(from, to, limit) => {
            let iter = table.range(from..=to, limit as usize, false)?;
            let response = PipedResponse {
                id,
                response: Response::Entries(
                    iter.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect(),
                ),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::IterPrefix(prefix, count) => {
            let iter = table.iter_prefix(prefix, count as usize)?;
            let response = PipedResponse {
                id,
                response: Response::Entries(
                    iter.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect(),
                ),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::IterPrefixFrom(prefix, from, count) => {
            let iter = table.iter_prefix_from(prefix, from, count as usize, false)?;
            let response = PipedResponse {
                id,
                response: Response::Entries(
                    iter.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect(),
                ),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::IterPrefixFromRev(prefix, from, count) => {
            let iter = table.iter_prefix_from(prefix, from, count as usize, true)?;
            let response = PipedResponse {
                id,
                response: Response::Entries(
                    iter.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect(),
                ),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::IterFrom(input_key, count) => {
            let iter = table.range(input_key.., count as usize, false)?;
            let response = PipedResponse {
                id,
                response: Response::Entries(
                    iter.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect(),
                ),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::IterFromRev(input_key, count) => {
            let iter = table.range(..=input_key, count as usize, true)?;
            let response = PipedResponse {
                id,
                response: Response::Entries(
                    iter.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect(),
                ),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::Increment(key) => {
            let new_value = table
                .increment(key)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Invalid command"))?;
            let response = PipedResponse {
                id,
                response: Response::Value(new_value.as_ref()),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::Decrement(key) => {
            let new_value = table
                .decrement(key)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Invalid command"))?;
            let response = PipedResponse {
                id,
                response: Response::Value(new_value.as_ref()),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::BatchedIncrement(keys) => {
            if let Ok(new_entries) = table.batched_increment(keys) {
                let response = PipedResponse {
                    id,
                    response: Response::Entries(
                        new_entries.iter().map(|(k, v)| (*k, v.as_ref())).collect(),
                    ),
                }
                .to_bytes();
                sender.send(response).map(|_| ()).map_err(|e| e.into())
            } else {
                let response = PipedResponse {
                    id,
                    response: Response::BatchFailed,
                }
                .to_bytes();
                sender.send(response).map(|_| ()).map_err(|e| e.into())
            }
        }
        Operation::IterCount => {
            let count = table.iter_count()?;
            let response = PipedResponse {
                id,
                response: Response::Value(count.to_be_bytes().as_ref()),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::IterPrefixCount(prefix) => {
            let count = table.iter_prefix_count(prefix)?;
            let response = PipedResponse {
                id,
                response: Response::Value(count.to_be_bytes().as_ref()),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::IterFromCount(input_key) => {
            let count = table.range_count(input_key.., false)?;
            let response = PipedResponse {
                id,
                response: Response::Value(count.to_be_bytes().as_ref()),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::IterFromRevCount(input_key) => {
            let count = table.range_count(..=input_key, true)?;
            let response = PipedResponse {
                id,
                response: Response::Value(count.to_be_bytes().as_ref()),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::RangeCount(from, to) => {
            let count = table.range_count(from..=to, false)?;
            let response = PipedResponse {
                id,
                response: Response::Value(count.to_be_bytes().as_ref()),
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
        Operation::WatchPrefix(prefix) => {
            let prefix = Bytes::copy_from_slice(prefix);
            if watch_prefix {
                match wakers.entry((table_id, prefix)) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().push(sender.clone());
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(vec![sender.clone()]);
                    }
                }
                Ok(())
            } else {
                let response = PipedResponse {
                    id,
                    response: Response::WatchPrefixUnsupported,
                }
                .to_bytes();
                sender.send(response).map_err(|e| e.into())
            }
        }
        Operation::DropTable => {
            tables.drop_table(table_id)?;
            let response = PipedResponse {
                id,
                response: Response::Ok,
            }
            .to_bytes();
            sender.send(response).map_err(|e| e.into())
        }
    }
}

#[inline(always)]
async fn handle_command<T> (
    tables: &mut T,
    bytes: Bytes,
    framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    push_sender: &mut async_broadcast::Sender<Bytes>,
    subscribe: bool,
    watched_keys: &Arc<DashMap<(Bytes, Bytes), Vec<(u32, Sender<Bytes>)>, RandomState>>,
) -> Result<()> where T: Database {
    let command = Command::from_bytes(&bytes)?;
    let operation = command.operation;
    let table_id = bytes.slice_ref(command.table);
    let table = tables.open_or_create_table(table_id.clone())?;
    match operation {
        Operation::Put(key, value) => {
            if table.insert(key, value).is_err() {
                return framed
                    .send(Response::PutError.to_bytes())
                    .await
                    .map_err(|e| e.into());
            }
            if let Some(entry) = watched_keys.remove(&(table_id.clone(), bytes.slice_ref(key))) {
                for (id, sender) in entry.1 {
                    let response_bytes = PipedResponse {
                        id,
                        response: Response::Value(value),
                    }
                    .to_bytes();
                    sender.send(response_bytes)?;
                }
            }
            if subscribe {
                push_sender.broadcast_direct(bytes).await?;
            }
            framed
                .send(Response::Ok.to_bytes())
                .await
                .map_err(|e| e.into())
        }
        Operation::BatchedPut(batch) => {
            if table.insert_batch(&batch).is_err() {
                let response = Response::BatchFailed.to_bytes();
                return framed.send(response).await.map_err(|e| e.into());
            }
            for &(key, value) in batch.iter() {
                if let Some(entry) = watched_keys.remove(&(table_id.clone(), bytes.slice_ref(key)))
                {
                    for (id, sender) in entry.1 {
                        let response_bytes = PipedResponse {
                            id,
                            response: Response::Value(value),
                        }
                        .to_bytes();
                        sender.send(response_bytes)?;
                    }
                }
            }
            if subscribe {
                push_sender.broadcast_direct(bytes).await?;
            }
            framed
                .send(Response::Ok.to_bytes())
                .await
                .map_err(|e| e.into())
        }
        Operation::Get(key) => {
            if let Ok(Some(entry)) = table.get(key) {
                framed
                    .send(Response::Value(entry.as_ref()).to_bytes())
                    .await
                    .map_err(|e| e.into())
            } else {
                framed
                    .send(Response::NoMatchingKey.to_bytes())
                    .await
                    .map_err(|e| e.into())
            }
        }
        Operation::BatchedGet(keys) => {
            if let Ok(entries) = table.get_batch(&keys) {
                let response =
                    Response::Entries(entries.iter().map(|(k, v)| (*k, v.as_ref())).collect())
                        .to_bytes();
                framed.send(response).await.map_err(|e| e.into())
            } else {
                let response = Response::BatchFailed.to_bytes();
                framed.send(response).await.map_err(|e| e.into())
            }
        }
        Operation::PutIfAbsent(key, value) => match table.put_if_absent(key, value) {
            Ok(true) => {
                if subscribe {
                    push_sender.broadcast_direct(bytes).await?;
                }
                framed
                    .send(Response::Ok.to_bytes())
                    .await
                    .map_err(|e| e.into())
            }
            Ok(false) => framed
                .send(Response::KeyExists.to_bytes())
                .await
                .map_err(|e| e.into()),
            Err(_) => framed
                .send(Response::PutError.to_bytes())
                .await
                .map_err(|e| e.into()),
        },
        Operation::Remove(key) => {
            if table.remove(key).is_ok() {
                if subscribe {
                    push_sender.broadcast_direct(bytes).await?;
                }
                framed
                    .send(Response::Ok.to_bytes())
                    .await
                    .map_err(|e| e.into())
            } else {
                framed
                    .send(Response::NoMatchingKey.to_bytes())
                    .await
                    .map_err(|e| e.into())
            }
        }
        Operation::BatchedRemove(keys) => {
            if table.remove_batch(keys).is_ok() {
                if subscribe {
                    push_sender.broadcast_direct(bytes).await?;
                }
                framed
                    .send(Response::Ok.to_bytes())
                    .await
                    .map_err(|e| e.into())
            } else {
                let response = Response::BatchFailed.to_bytes();
                framed.send(response).await.map_err(|e| e.into())
            }
        }
        Operation::Update(key, value) => match table.update(key, value) {
            Ok(true) => {
                if subscribe {
                    push_sender.broadcast_direct(bytes).await?;
                }
                framed
                    .send(Response::Ok.to_bytes())
                    .await
                    .map_err(|e| e.into())
            }
            Ok(false) => framed
                .send(Response::NoMatchingKey.to_bytes())
                .await
                .map_err(|e| e.into()),
            Err(_) => framed
                .send(Response::PutError.to_bytes())
                .await
                .map_err(|e| e.into()),
        },
        Operation::Iter(count) => {
            let iter = table.iter(count as usize)?;
            let response = Response::Entries(
                iter.iter()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .to_bytes();
            framed.send(response).await.map_err(|e| e.into())
        }
        Operation::Range(from, to, limit) => {
            let iter = table.range(from..=to, limit as usize, false)?;
            let response = Response::Entries(
                iter.iter()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .to_bytes();
            framed.send(response).await.map_err(|e| e.into())
        }
        Operation::IterPrefix(prefix, count) => {
            let iter = table.iter_prefix(prefix, count as usize)?;
            let response = Response::Entries(
                iter.iter()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .to_bytes();
            framed.send(response).await.map_err(|e| e.into())
        }
        Operation::IterPrefixFrom(prefix, from, count) => {
            let iter = table.iter_prefix_from(prefix, from, count as usize, false)?;
            let response = Response::Entries(
                iter.iter()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .to_bytes();
            framed.send(response).await.map_err(|e| e.into())
        }
        Operation::IterPrefixFromRev(prefix, from, count) => {
            let iter = table.iter_prefix_from(prefix, from, count as usize, true)?;
            let response = Response::Entries(
                iter.iter()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .to_bytes();
            framed.send(response).await.map_err(|e| e.into())
        }
        Operation::IterFrom(input_key, count) => {
            let iter = table.range(input_key.., count as usize, false)?;
            let response = Response::Entries(
                iter.iter()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .to_bytes();
            framed.send(response).await.map_err(|e| e.into())
        }
        Operation::IterFromRev(input_key, count) => {
            let iter = table.range(..=input_key, count as usize, true)?;
            let response = Response::Entries(
                iter.iter()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .to_bytes();
            framed.send(response).await.map_err(|e| e.into())
        }
        Operation::Increment(key) => {
            let new_value = table
                .increment(key)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Invalid command"))?;
            framed
                .send(Response::Value(new_value.as_ref()).to_bytes())
                .await
                .map_err(|e| e.into())
        }
        Operation::BatchedIncrement(keys) => {
            if let Ok(new_entries) = table.batched_increment(keys) {
                let response =
                    Response::Entries(new_entries.iter().map(|(k, v)| (*k, v.as_ref())).collect())
                        .to_bytes();
                framed
                    .send(response)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.into())
            } else {
                let response = Response::BatchFailed.to_bytes();
                framed
                    .send(response)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.into())
            }
        }
        Operation::RangeCount(from, to) => {
            let count = table.range_count(from..=to, false)?;
            framed
                .send(Response::Value(count.to_be_bytes().as_ref()).to_bytes())
                .await
                .map_err(|e| e.into())
        }
        Operation::IterCount => {
            let count = table.iter_count()?;
            framed
                .send(Response::Value(count.to_be_bytes().as_slice()).to_bytes())
                .await
                .map_err(|e| e.into())
        }
        Operation::IterPrefixCount(prefix) => {
            let count = table.iter_prefix_count(prefix)?;
            framed
                .send(Response::Value(count.to_be_bytes().as_slice()).to_bytes())
                .await
                .map_err(|e| e.into())
        }
        Operation::IterFromCount(input_key) => {
            let count = table.range_count(input_key.., false)?;
            framed
                .send(Response::Value(count.to_be_bytes().as_slice()).to_bytes())
                .await
                .map_err(|e| e.into())
        }
        Operation::IterFromRevCount(input_key) => {
            let count = table.range_count(..=input_key, true)?;
            framed
                .send(Response::Value(count.to_be_bytes().as_slice()).to_bytes())
                .await
                .map_err(|e| e.into())
        }
        Operation::DropTable => {
            tables.drop_table(table_id)?;
            framed
                .send(Response::Ok.to_bytes())
                .await
                .map_err(|e| e.into())
        }
        _ => unreachable!()
    }
}
