use crate::{handle_command, handle_piped_command};
use ahash::RandomState;
use anyhow::Result;
use async_broadcast::broadcast as async_broadcast;
use dashmap::DashMap;
use rustore_protocol::HandShake;
use rustore_storage::Database;
use flume::Sender;
use futures_util::StreamExt;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::AsyncWriteExt,
    net::TcpSocket,
    time::timeout,
};
use tokio_util::{
    bytes::Bytes,
    codec::{Framed, LengthDelimitedCodec},
};
#[derive(Clone)]
pub struct Server<T> where T: Database + Clone + Send {
    pub db: T,
    pub addr: SocketAddr,
    secret: String,
    codec: LengthDelimitedCodec,
}

impl<T> Server<T> where T: Database + Clone + Send + 'static {
    pub fn new(addr: SocketAddr, secret: &str, db: T) -> Result<Self> {
        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(1024 * 1024 * 1024);
        Ok(Self {
            db,
            addr,
            secret: secret.to_string(),
            codec,
        })
    }
    pub fn serve(&self, workers: Option<usize>) -> Result<()> {
        let wakers: Arc<DashMap<(Bytes, Bytes), Vec<Sender<Bytes>>, RandomState>> =
            Arc::new(DashMap::default());
        let mut threads: Vec<std::thread::JoinHandle<anyhow::Result<()>>> = Vec::new();
        let (ctrlc_sender, ctrlc_receiver) =
            flume::bounded::<bool>(workers.unwrap_or(num_cpus::get()));
        let (push_sender, _push_receiver) = async_broadcast(1024);
        let secret = Box::new(self.secret.clone());
        let secret: &'static str = Box::leak(secret);
        let watched_keys: Arc<DashMap<(Bytes, Bytes), Vec<(u32, Sender<Bytes>)>, RandomState>> =
            Arc::new(DashMap::default());
        for _i in 0..workers.unwrap_or(num_cpus::get()) {
            let server = self.clone();
            let addr = self.addr;
            let ctrlc_receiver = ctrlc_receiver.clone();
            let push_sender_clone = push_sender.clone();
            let codec = self.codec.clone();
            let wakers = wakers.clone();
            let watched_keys = watched_keys.clone();
            let t = std::thread::spawn(move || {
                let socket = TcpSocket::new_v4()?;
                socket.set_reuseaddr(true)?;
                socket.set_reuseport(true)?;
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let _: anyhow::Result<()> = rt.block_on(async move {
                    socket.bind(addr)?;
                    let listener = socket.listen(1024).unwrap();
                    let codec = codec.clone();
                    let localset = tokio::task::LocalSet::new();
                    let _: anyhow::Result<()> = localset
                        .run_until(async move {
                            loop {
                                let mut tables = server.db.clone();
                                let codec = codec.clone();
                                let mut push_sender = push_sender_clone.clone();
                                let wakers = wakers.clone();
                                let watched_keys = watched_keys.clone();
                                tokio::select! {
                                    Ok((tcp_stream, _client_addr)) = listener.accept() => {
                                        tokio::task::spawn_local(async move {
                                            let mut framed = Framed::new(tcp_stream, codec.clone());
                                            if let Ok(Some(Ok(first_frame))) = timeout(Duration::from_millis(5000), framed.next()).await {
                                                match HandShake::from_bytes(&first_frame) {
                                                    Ok(handshake) if handshake.secret == secret => {
                                                        match handshake.kind {
                                                            rustore_protocol::HandShakeKind::Piped(subscribe, watch_prefix) => {
                                                                let (mut sender, receiver) = flume::bounded::<Bytes>(2000000);
                                                                let receiver = receiver.into_stream();
                                                                let (sink, mut stream) = framed.split();
                                                                tokio::task::spawn_local((receiver.map(|bytes| Ok(bytes))).forward(sink));
                                                                let mut push_sender = push_sender.clone();
                                                                while let Some(Ok(bytes)) = stream.next().await {
                                                                    let _ = handle_piped_command(&mut tables, bytes.freeze(), &mut sender, &mut push_sender, subscribe, watch_prefix, &wakers, &watched_keys).await;
                                                                }
                                                            }
                                                            rustore_protocol::HandShakeKind::Standard(subscribe) => {
                                                                tokio::task::spawn_local(async move {
                                                                    while let Some(Ok(bytes)) = framed.next().await {
                                                                        let _ = handle_command(&mut tables, bytes.freeze(), &mut framed, &mut push_sender, subscribe, &watched_keys).await;
                                                                    }
                                                                });
                                                            }
                                                            rustore_protocol::HandShakeKind::Subscriber => {
                                                                tokio::task::spawn_local(async move {
                                                                    let (sink, _) = framed.split();
                                                                    let push_receiver = push_sender.new_receiver();
                                                                    push_receiver
                                                                        .map(|bytes| Ok(bytes))
                                                                        .forward(sink)
                                                                        .await
                                                                        .ok();
                                                                });
                                                            }
                                                        }
                                                    }
                                                    _ => {
                                                        let mut inner = framed.into_inner();
                                                        inner.write(b"cao ni ma").await.ok();
                                                        inner.shutdown().await.ok();
                                                    }
                                                }
                                            }
                                        });
                                    }
                                    _ = ctrlc_receiver.recv_async() => {
                                        println!("ctrl-c received, shutting down");
                                        tables.flush()?;
                                        break;
                                    }
                                }
                            }
                            Ok(())
                        })
                        .await;
                    Ok(())
                });
                Ok(())
            });
            threads.push(t);
        }

        ctrlc::set_handler(move || {
            println!("main thread closing");
            for _i in 0..workers.unwrap_or(num_cpus::get()) {
                let _ = ctrlc_sender.send(true);
            }
        })?;
        for t in threads {
            t.join().unwrap()?;
        }
        self.db.flush()?;
        Ok(())
    }
}
